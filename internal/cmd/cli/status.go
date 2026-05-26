// Copyright 2026. Triad National Security, LLC. All rights reserved.

package clicmd

import (
	"fmt"
	"os"
	"sort"

	"github.com/google/uuid"
	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/defaults"
	"github.com/lanl/conduit/internal/cli/client"
	"github.com/lanl/conduit/internal/cli/processing"
	pp "github.com/lanl/conduit/internal/cli/progressprinter"
	"github.com/lanl/conduit/internal/cli/querygen"
	"github.com/lanl/conduit/internal/cli/util"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	statusCmdLimitNum     int    // Placeholder for '-n'
	statusCmdOutputFormat string // Placeholder for '-o'
)

type SortDetails []*proto.TransferDetails

func (a SortDetails) Len() int           { return len(a) }
func (a SortDetails) Less(i, j int) bool { return a[i].GetTransferID() < a[j].GetTransferID() }
func (a SortDetails) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

/*===
 * Post-processing functions
 *=== */

// Determine if '-o' parameter is valid, if given
func IsValidOutputFormat() error {
	switch statusCmdOutputFormat {
	case "":
	case "normal":
	case "wide":
	default:
		return fmt.Errorf("invalid output format: '%s'", statusCmdOutputFormat)
	}
	return nil
}

/*===
 * Command function
 *=== */
var getCmd = &cobra.Command{
	Hidden: true,
	Use:    "get [TRANSFER_ID | SLURM_JOB_ID | TRANSFER_TIMESTAMP]",
	Short:  "get basic status of transfer(s)",
	Long:   `get the basic status of one or more transfer. If no transfer id is provided, the status of every transfer is returned`,
	Args: func(cmd *cobra.Command, args []string) error {
		return IsValidOutputFormat()
	},
	Run: func(cmd *cobra.Command, args []string) {
		statusCmd.Run(cmd, args)
	},
}

var statusCmd = &cobra.Command{
	Use:   "status [TRANSFER_ID | SLURM_JOB_ID | TRANSFER_TIMESTAMP]",
	Short: "get basic status of transfer(s)",
	Long:  `get the basic status of one or more transfer. If no transfer id is provided, the status of every transfer is returned`,
	Args: func(cmd *cobra.Command, args []string) error {
		return IsValidOutputFormat()
	},
	Run: func(cmd *cobra.Command, args []string) {
		logger := logrus.New()
		if debug {
			logger.SetLevel(logrus.DebugLevel)
			logger.Debugf("loaded cli config from: %v", viper.ConfigFileUsed())
		}

		// If query strings are specified, assign it
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
			fmt.Printf("failed to create client: %v\n", err)
			os.Exit(1)
		}

		doneChan := make(chan bool)
		if !quiet {
			go printErrantPaths(logger, cc, doneChan)
		}

		// Generate and submit query to server
		var mtd *proto.MultiTransferDetails
		qs := querygen.GenerateQueryStringMap(queryStrings)
		logger.Debugf("query string: %v", qs)
		qo := &proto.QueryOptions{
			QueryMap:       qs,
			QueryOperation: proto.QueryOperation_OR,
			User:           providedUser,
		}
		logger.Debugf("querying for transfers")
		mtd, err = cc.Query(qo)
		if err != nil {
			logger.Fatalf("query failed: %v", err)
		}
		logger.Debugf("received transfers")

		// Create location to store all results
		detailsSlice := []proto.TransferDetails{}

		// Get slice of TransferDetails from map of TransferDetails
		details := mtd.GetDetails()
		for ti, td := range details {
			// check if this is a validation only transfer and completed successfully
			if !(td.GetValidationOnly() && td.GetError() == proto.Error_ERROR_NONE && td.GetState() == proto.TransferState_TRANSFER_VALIDATION_COMPLETE) {
				detailsSlice = append(detailsSlice, *details[ti])
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

		if statusCmdLimitNum >= 0 && statusCmdLimitNum < len(detailsSlice) {
			detailsSlice = detailsSlice[0:statusCmdLimitNum]
		}

		// Create printer to use for printing the status
		var writer pp.Writer
		writer, err = pp.NewWriter(os.Stdout, true)
		if err != nil {
			logger.Fatalf("status: failed to create printer object: %v", err)
		}

		switch {
		// -o wide
		case statusCmdOutputFormat == "wide":
			// Print wide transfer details
			err = writer.SetProcessing(
				[]string{
					"TRANSFER_ID",
					"STATE",
					"ERROR",
					"SOURCE",
					"DESTINATION",
					"CREATED",
					"DATA_TRANSFERRED",
					"FILES_TRANSFERRED",
					"USER"},
				[]string{
					"GetTransferID",
					"GetState",
					"GetError",
					"GetSource",
					"GetDestination",
					"GetCreatedTime",
					"GetDataTransferred",
					"GetFilesTransferred",
					"GetUser"},
				[]func(interface{}) (string, error){
					processing.ProcessString,
					processing.ProcessCapsSinglePrefix,
					processing.ProcessCapsSinglePrefix,
					processing.ProcessStringSlice,
					processing.ProcessString,
					processing.ProcessCreationDate,
					processing.ProcessNiceBytes,
					processing.ProcessInt,
					processing.ProcessString},
				"GetActive")
			if err != nil {
				logger.Fatalf("status: writer.SetProcessing wide failed: %v", err)
			}
		// [-o normal]
		case statusCmdOutputFormat == "" || statusCmdOutputFormat == "normal":
			// Print transfer details
			err = writer.SetProcessing(
				[]string{
					"TRANSFER_ID",
					"STATE",
					"ERROR",
					"SOURCE",
					"DESTINATION",
					"CREATED",
					"DATA_TRANSFERRED"},
				[]string{
					"GetTransferID",
					"GetState",
					"GetError",
					"GetSource",
					"GetDestination",
					"GetCreatedTime",
					"GetDataTransferred"},
				[]func(interface{}) (string, error){
					processing.ProcessString,
					processing.ProcessCapsSinglePrefix,
					processing.ProcessCapsSinglePrefix,
					processing.ProcessStringSlice,
					processing.ProcessString,
					processing.ProcessCreationDate,
					processing.ProcessNiceBytes},
				"GetActive")
			if err != nil {
				logger.Fatalf("status: writer.SetProcessing normal failed: %v", err)
			}
		}
		// Send transfer details to printer
		for t := range detailsSlice {
			err = writer.Event(&detailsSlice[t])
			if err != nil {
				logger.Fatalf("status: failed to print xfer details: %v", err)
			}
		}

		if !quiet {
			// wait for errant stuff to print
			<-doneChan
		}

		// Print
		writer.Start()
		writer.Stop()
	},
}

func init() {
	RootCmd.AddCommand(statusCmd)
	statusCmd.Flags().StringVar(&providedUser, "user", "", "Only get transfers owned by this user. Requires an admin cert & key to be provided")
	statusCmd.Flags().StringVarP(&statusCmdOutputFormat, "output", "o", "", "Output format, one of (wide|normal); default is 'normal'")
	statusCmd.Flags().IntVarP(&statusCmdLimitNum, "limit", "n", 20, "Output the most recent NUM of transfers (-1 for all)")

	RootCmd.AddCommand(getCmd)
	getCmd.Flags().StringVar(&providedUser, "user", "", "Only get transfers owned by this user. Requires an admin cert & key to be provided")
	getCmd.Flags().StringVarP(&statusCmdOutputFormat, "output", "o", "", "Output format, one of (wide|normal); default is 'normal'")
	getCmd.Flags().IntVarP(&statusCmdLimitNum, "limit", "n", 20, "Output the most recent NUM of transfers (-1 for all)")
}
