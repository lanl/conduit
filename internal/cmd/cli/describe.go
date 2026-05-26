// Copyright 2026. Triad National Security, LLC. All rights reserved.

package clicmd

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/PaesslerAG/jsonpath"
	"github.com/google/uuid"
	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/defaults"
	"github.com/lanl/conduit/internal/cli/client"
	"github.com/lanl/conduit/internal/cli/processing"
	"github.com/lanl/conduit/internal/cli/querygen"
	"github.com/lanl/conduit/internal/cli/util"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/protobuf/encoding/protojson"
	"gopkg.in/yaml.v2"
)

var (
	describeCmdLimitNum     int    // Placeholder for '-n'
	describeCmdOutputFormat string // Placeholder for '-o'
	describeCmdJsonPath     string
)

// Prep the transfer details before doing the final marshal
func prepSliceTD(s []*proto.TransferDetails) ([]map[string]interface{}, error) {
	mo := protojson.MarshalOptions{
		Multiline:       true,
		EmitUnpopulated: true,
	}

	var transferList struct {
		transfers []map[string]interface{}
	}

	// For each TransferDetails object,
	for _, td := range s {
		// Convert to JSON and back
		bj, err := mo.Marshal(td)
		if err != nil {
			return nil, err
		}
		jobj := map[string]interface{}{}
		err = json.Unmarshal(bj, &jobj)
		if err != nil {
			return nil, err
		}

		// Convert xfer timestamps to local time
		if jobj, err = ProcessAttributes(jobj, LocalTimeRFC3339, []string{
			"startTime",
			"endTime",
			"createdTime"}); err != nil {
			return nil, err
		}
		// Convert byte string to nice strings
		if jobj, err = ProcessAttributes(jobj, BytesFromString, []string{"dataTransferred"}); err != nil {
			return nil, err
		}
		// Convert byte string to nice strings per sec
		if jobj, err = ProcessAttributes(jobj, BytesPerSecFromString, []string{"bandwidth"}); err != nil {
			return nil, err
		}

		// Append to list of transfers
		transferList.transfers = append(transferList.transfers, jobj)
	}

	return transferList.transfers, nil
}

// Convert a list of TransferDetails into a YAML byte slice
func MarshalYAMLSliceTD(s []*proto.TransferDetails) ([]byte, error) {
	transfers, err := prepSliceTD(s)
	if err != nil {
		return nil, err
	}

	// Convert to YAML
	y, err := yaml.Marshal(transfers)
	if err != nil {
		return nil, err
	}

	return y, nil
}

// Convert a list of TransferDetails into a JSON byte slice
func MarshalJSONSliceTD(s []*proto.TransferDetails, cleanJsonPath string) ([]byte, error) {
	transfers, err := prepSliceTD(s)
	if err != nil {
		return nil, err
	}

	// convert to json
	j, err := json.Marshal(transfers)
	if err != nil {
		return nil, err
	}

	var filteredValues interface{}
	if cleanJsonPath != "" {
		v := interface{}(nil)

		err := json.Unmarshal(j, &v)
		if err != nil {
			return nil, err
		}

		filteredValues, err = jsonpath.Get(cleanJsonPath, v)
		if err != nil {
			return nil, fmt.Errorf("jsonpath failed: %v", err)
		}
	}

	if filteredValues != nil {
		j, err = json.Marshal(filteredValues)
		if err != nil {
			return nil, err
		}
	}

	return j, nil
}

// Take an unmarshalled JSON object and process the output.
func ProcessAttributes(obj map[string]interface{}, processor func(interface{}) (interface{}, error), attributes []string) (map[string]interface{}, error) {
	for _, path := range attributes {
		var err error
		subpaths := strings.Split(path, ".")
		if obj[subpaths[0]] == nil {
			obj[subpaths[0]] = ""
			continue
		}
		_, ok := obj[subpaths[0]].(string)
		if !ok {
			if obj[subpaths[0]] != nil {
				return obj, fmt.Errorf("could not assert as string: %v", obj[subpaths[0]])
			}
		}
		if obj[subpaths[0]] != nil {
			obj[subpaths[0]], err = processor(obj[subpaths[0]])
		} else {
			obj[subpaths[0]], err = processing.Nil, nil
		}
		if err != nil {
			return obj, fmt.Errorf("could not run processor for '%v': %v", subpaths[0], err)
		}
	}
	return obj, nil
}

func LocalTimeRFC3339(i interface{}) (interface{}, error) {
	currTimeStr, err := UTCToLocal(i, time.RFC3339)
	if err != nil {
		return nil, err
	}
	return currTimeStr, nil
}

// This is necessary for describe (and not get), since marshalling to YAML
// requires returning interfaces as opposed to strings
func BytesFromString(i interface{}) (interface{}, error) {
	return processing.ProcessNiceBytes(i)
}

// This the same as BytesFromString, but appends a '/sec' to the result
func BytesPerSecFromString(i interface{}) (interface{}, error) {
	nb, err := processing.ProcessNiceBytes(i)
	return nb + "/sec", err
}

func UTCToLocal(rfc3339utc interface{}, layout string) (string, error) {
	utcStr, validStr := rfc3339utc.(string)
	if !validStr {
		return "", fmt.Errorf("given interface cannot be asserted to string")
	}
	timeVal, assigned := time.Parse(layout, utcStr)
	if assigned != nil {
		return "", fmt.Errorf("given time string / layout string combination failed")
	}
	return timeVal.Local().Format(layout), nil
}

var describeCmd = &cobra.Command{
	Use:   "describe [TRANSFER_ID | SLURM_JOB_ID | TRANSFER_TIMESTAMP]",
	Short: "get the details of transfer(s)",
	Long:  `get the details of one or more transfer. If no transfer id is provided, the details of every transfer is returned`,
	Args: func(cmd *cobra.Command, args []string) error {
		return IsValidOutputFormat()
	},
	Run: func(cmd *cobra.Command, args []string) {
		logger := logrus.New()
		if debug {
			logger.SetLevel(logrus.DebugLevel)
			logger.Debugf("loaded cli config from: %v", viper.ConfigFileUsed())
		}

		// If transfer ID is specified, assign it
		queryStrings := []string{}
		if len(args) > 0 {
			queryStrings = args
		}

		clientCertKeyBundle, err := cmd.Flags().GetString("cert-key-bundle")
		if err != nil {
			fmt.Printf("failed to get cert-key-bundle flag: %v\n", err)
			os.Exit(1)
		}
		clientCert, clientKey, err := util.GetUserCertAndKey(viper.GetString(defaults.ConfigClientCertKey), viper.GetString(defaults.ConfigClientKeyKey), clientCertKeyBundle, defaults.DefaultBundlePath)
		if err != nil {
			fmt.Printf("failed to get client cert and key: %v\n", err)
			os.Exit(1)
		}
		logger.Debugf("using user cert [%v] and key [%v]", clientCert, clientKey)

		// Create client to send the gRPC request
		cc, err := client.NewClient(logger, quiet, clientCert, clientKey)
		if err != nil {
			logger.Fatalf("Failed to create client: %v", err)
		}

		doneChan := make(chan bool)
		if !quiet {
			go printErrantPaths(logger, cc, doneChan)
		}

		// Create location to store all results
		detailsSlice := []*proto.TransferDetails{}

		// Generate and submit query to server
		var mtd *proto.MultiTransferDetails
		qs := querygen.GenerateQueryStringMap(queryStrings)
		logger.Debugf("query string: %v", qs)
		qo := &proto.QueryOptions{
			QueryMap:       qs,
			QueryOperation: proto.QueryOperation_OR,
			User:           providedUser,
		}
		mtd, err = cc.Query(qo)
		if err != nil {
			logger.Fatalf("describe failed: %v", err)
		}

		// Get slice of TransferDetails from map of TransferDetails
		details := mtd.GetDetails()
		for _, td := range details {
			// check if this is a validation only transfer and completed successfully
			if !(td.GetValidationOnly() && td.GetError() == proto.Error_ERROR_NONE && td.GetState() == proto.TransferState_TRANSFER_VALIDATION_COMPLETE && describeCmdLimitNum > 0) {
				detailsSlice = append(detailsSlice, td)
			}
		}

		// Sort by TransferDetails object creation time
		sort.Slice(detailsSlice, func(p, q int) bool {
			if detailsSlice[p].GetCreatedTime().AsTime().Equal(detailsSlice[q].GetCreatedTime().AsTime()) {
				uuid1, err := uuid.Parse(detailsSlice[p].GetTransferID())
				if err != nil {
					return false
				}
				uuid2, err := uuid.Parse(detailsSlice[q].GetTransferID())
				if err != nil {
					return true
				}
				_, uuid1NSec := uuid1.Time().UnixTime()
				_, uuid2NSec := uuid2.Time().UnixTime()
				return uuid1NSec > uuid2NSec
			}
			return detailsSlice[p].GetCreatedTime().AsTime().After(detailsSlice[q].GetCreatedTime().AsTime())
		})

		if describeCmdLimitNum >= 0 && describeCmdLimitNum < len(detailsSlice) {
			detailsSlice = detailsSlice[0:describeCmdLimitNum]
		}

		var res []byte
		if describeCmdOutputFormat == "json" || strings.TrimSpace(describeCmdJsonPath) != "" {
			res, err = MarshalJSONSliceTD(detailsSlice, strings.TrimSpace(describeCmdJsonPath))
		} else {
			res, err = MarshalYAMLSliceTD(detailsSlice)
		}

		if err != nil {
			logger.Fatalf("describe failed: %v", err)
		}

		if !quiet {
			// wait for errant stuff to print
			<-doneChan
		}

		fmt.Println(string(res))

	},
}

func init() {
	RootCmd.AddCommand(describeCmd)
	describeCmd.Flags().IntVarP(&describeCmdLimitNum, "limit", "n", 20, "Output the most recent NUM of transfers (-1 for all)")
	describeCmd.Flags().StringVar(&providedUser, "user", "", "Only get transfers owned by this user. Requires an admin cert & key to be provided")
	describeCmd.Flags().StringVarP(&describeCmdOutputFormat, "output", "o", "", "Output format, one of (yaml|json); default is 'json'")
	describeCmd.Flags().StringVar(&describeCmdJsonPath, "jsonpath", "", "Print the fields defined in a jsonpath expression. forces output (-o) to 'json'")
	describeCmd.Flags().BoolVarP(&quiet, "quiet", "q", false, "reduce command output to only transfer information")
}
