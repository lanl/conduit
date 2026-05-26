// Copyright 2026. Triad National Security, LLC. All rights reserved.

package clicmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/google/uuid"
	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/defaults"
	"github.com/lanl/conduit/internal/cli/client"
	"github.com/lanl/conduit/internal/cli/util"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// errorCmd represents the error command
var errorCmd = &cobra.Command{
	Use:   "error TRANSFER_ID...",
	Short: "get error information for a transfer(s)",
	Long:  "get only error information for a transfer(s)",
	Args: func(cmd *cobra.Command, args []string) error {
		// check for at least one argument
		if err := cobra.MinimumNArgs(1)(cmd, args); err != nil {
			return err
		}

		// check if transferIDs are valid uuid
		for _, a := range args {
			_, err := uuid.Parse(a)
			if err != nil {
				return fmt.Errorf("invalid uuid specified %v: %v", a, err)
			}
		}

		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		logger := logrus.New()
		if debug {
			logger.SetLevel(logrus.DebugLevel)
			logger.Debugf("loaded cli config from: %v", viper.ConfigFileUsed())
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

		client, err := client.NewClient(logger, quiet, clientCert, clientKey)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		tids := []string{}

		for _, a := range args {
			tid, err := uuid.Parse(a)
			if err != nil {
				fmt.Printf("failed to parse transferID[%s]: %v\n", a, err)
				os.Exit(1)
			}
			tids = append(tids, tid.String())
		}

		// Generate and submit query to server
		qs := map[string]string{"TransferID": strings.Join(tids, "|")}
		logger.Debugf("query string: %v", qs)
		qo := &proto.QueryOptions{
			QueryMap:       qs,
			QueryOperation: proto.QueryOperation_OR,
			User:           providedUser,
		}
		mtd, err := client.Query(qo)
		if err != nil {
			logger.Fatalf("query failed: %v", err)
		}

		// print error for each transfer
		for _, td := range mtd.GetDetails() {
			fmt.Printf("TransferID: %s\nTransfer State: %s\nTransfer Error State: %s\nTransfer Error Message: %s\n\n", td.GetTransferID(), td.GetState(), td.GetError(), td.GetErrorMessage())
		}
	},
}

func init() {
	errorCmd.Flags().StringVar(&providedUser, "user", "", "Only get errors for transfers owned by this user. Requires an admin cert & key to be provided")

	RootCmd.AddCommand(errorCmd)
}
