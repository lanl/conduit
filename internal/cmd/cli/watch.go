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
	watchCmdLimitNum int // Placeholder for '-n'
)

// Error: at least one transfer failed
type ErrXferFailed int

func (e ErrXferFailed) Error() string {
	return "One or more transfers failed"
}

var EXFERFAILED = ErrXferFailed(1)

var watchCmd = &cobra.Command{
	Use:   "watch TRANSFER_ID ...",
	Short: "watch the status of one or more transfers",
	Long:  `watch the status of one or more transfers. Optionally one or more transfer IDs can be supplied.`,
	Args: func(cmd *cobra.Command, args []string) error {
		return IsValidOutputFormat()
	},
	Run: func(cmd *cobra.Command, args []string) {
		logger := logrus.New()
		if debug {
			logger.SetLevel(logrus.DebugLevel)
			logger.Debugf("loaded cli config from: %v", viper.ConfigFileUsed())
		}

		// If transfer ID(s) are specified, assign it/them
		var tIDs []string
		if len(args) > 0 {
			tIDs = args
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

		var mtd *proto.MultiTransferDetails
		var details map[string]*proto.TransferDetails
		var detailsSlice []proto.TransferDetails
		// Get transfer IDs if none specified
		if len(tIDs) == 0 {
			// Construct query
			qs := querygen.GenerateQueryStringMap(tIDs)
			logger.Debugf("query string: %v", qs)
			qo := &proto.QueryOptions{
				QueryMap:       qs,
				QueryOperation: proto.QueryOperation_OR,
				User:           providedUser,
			}
			// Send query
			mtd, err = cc.Query(qo)
			if err != nil {
				fmt.Printf("query failed: %v\n", err)
				os.Exit(1)
			}
			// Get slice of TransferDetails from map of TransferDetails
			details = mtd.GetDetails()
			for td := range details {
				detailsSlice = append(detailsSlice, *details[td])
			}

			// Sort by TransferDetails object creation time (inverse)
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
					return uuid1NSec < uuid2NSec
				}
				return detailsSlice[p].GetCreatedTime().AsTime().After(detailsSlice[q].GetCreatedTime().AsTime())
			})
			tIDs = []string{}
			// Get transfer IDs
			for tdi, td := range detailsSlice {
				// check if this is a validation only transfer and completed successfully
				if !(td.GetValidationOnly() && td.GetError() == proto.Error_ERROR_NONE && td.GetState() == proto.TransferState_TRANSFER_VALIDATION_COMPLETE) {
					tIDs = append(tIDs, detailsSlice[tdi].GetTransferID())
				}
			}
			// Get only the last n transfers if specified
			if len(args) == 0 && watchCmdLimitNum >= 0 {
				if watchCmdLimitNum < len(tIDs) {
					tIDs = tIDs[0:watchCmdLimitNum]
				}
			}
		}

		// Send gRPC request to watch transfer, getting back:
		// the watch client object, the watch cancel function, and any error
		wc, wCancel, err := cc.WatchStatus(tIDs, providedUser)

		go func() {
			<-wc.Context().Done()
			logger.Debug("watch: grpc stream closed")
		}()
		// If watch request not successful, err
		if err != nil {
			fmt.Printf("watch: failed to watch status of transfer: %v\n", err)
			os.Exit(1)
		}
		// Handle cancelling the watch command
		defer func() {
			logger.Debug("watch: closing grpc stream")
			wCancel()
		}()
		// Close the send side of grpc stream since we are only receiving
		err = wc.CloseSend()
		// Err if could not close send side of grpc stream
		if err != nil {
			fmt.Printf("watch: failed to close the send side of the stream: %v\n", err)
			os.Exit(1)
		}

		logger.Debug("watch: starting to listen to the watch stream")

		// Create printer to use for printing the status
		var writer pp.Writer
		writer, err = pp.NewWriter(os.Stdout, false)
		if err != nil {
			logger.Fatalf("watch: failed to create printer object: %v", err)
		}
		err = writer.SetProcessing(
			[]string{
				"TRANSFER_ID",
				"STATE",
				"ERROR",
				"SOURCE",
				"DATA_TRANSFERRED"},
			[]string{
				"GetTransferID",
				"GetState",
				"GetError",
				"GetSource",
				"GetDataTransferred"},
			[]func(interface{}) (string, error){
				processing.ProcessString,
				processing.ProcessCapsSinglePrefix,
				processing.ProcessCapsSinglePrefix,
				processing.ProcessStringSlice,
				processing.ProcessNiceBytes},
			"GetActive")
		if err != nil {
			logger.Fatalf("watch: failed to configure printer object: %v", err)
		}

		// If user has no transfers, just print headers
		if len(tIDs) == 0 {
			writer.Start()
			writer.Stop()
			return
		}

		var first bool = true
		var allDone bool

		if !quiet {
			// wait for errant paths
			<-doneChan
		}

		// Continue watching until cancelled
		for {
			// Receive status
			mtd, err = wc.Recv()
			logger.Debugf("watch: received status message: %+v", mtd)
			// If error in receiving status, err
			if err != nil {
				fmt.Printf("watch: error while watching status: %v\n", err)
				os.Exit(1)
			}
			// Get slice of TransferDetails from map of TransferDetails
			details = mtd.GetDetails()
			detailsSlice = []proto.TransferDetails{}
			for td := range details {
				detailsSlice = append(detailsSlice, *details[td])
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
			// Print status
			for t := range detailsSlice {
				err = writer.Event(&detailsSlice[t])
				if err != nil {
					logger.Fatalf("watch: failed to print xfer details: %v", err)
				}
			}
			// Start printing after first transfers
			if first {
				if !quiet {
					writer.Start()
				}
				first = false
			}

			// Check if all watched transfers are finalized
			if allDone, err = writer.AllEventsDone(); allDone {
				break
			}
		}
		// Stop printing
		if !quiet {
			writer.Stop()
		}

		if err != nil {
			logger.Fatalf("Error: %v", err)
		}
	},
}

func init() {
	RootCmd.AddCommand(watchCmd)
	watchCmd.Flags().IntVarP(&watchCmdLimitNum, "limit", "n", 20, "Output the most recent NUM of transfers")
	watchCmd.Flags().StringVar(&providedUser, "user", "", "Only watch transfers owned by this user. Requires an admin cert & key to be provided")
}
