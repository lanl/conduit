// Copyright 2026. Triad National Security, LLC. All rights reserved.

package clicmd

import (
	"fmt"
	"os"

	"github.com/google/uuid"
	"github.com/lanl/conduit/defaults"
	"github.com/lanl/conduit/internal/cli/client"
	"github.com/lanl/conduit/internal/cli/util"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// abortCmd represents the abort command
var abortCmd = &cobra.Command{
	Use:   "abort TRANSFER_ID...",
	Short: "abort a running transfer(s)",
	Long:  "abort a running transfer or transfers",
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

		mtd, err := client.StopTransfer(tids)
		if err != nil {
			fmt.Printf("abort request failed: %v\n", err)
			os.Exit(1)
		} else {
			ids := []string{}
			for _, t := range mtd.GetDetails() {
				ids = append(ids, t.GetTransferID())
			}

			if len(ids) > 1 {
				if quiet {
					fmt.Printf("%v\n", ids)
				} else {
					fmt.Printf("successfully aborted transfers: %v\n", ids)
				}
			} else {
				if quiet {
					fmt.Printf("%v\n", ids[0])
				} else {
					fmt.Printf("successfully aborted transfer: %v\n", ids[0])
				}
			}
		}
	},
}

func init() {
	abortCmd.Flags().BoolVarP(&quiet, "quiet", "q", false, "reduce command output to only a transferID")

	RootCmd.AddCommand(abortCmd)
}
