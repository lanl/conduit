// Copyright 2026. Triad National Security, LLC. All rights reserved.

package clicmd

import (
	"fmt"

	"github.com/google/uuid"
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
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := logrus.New()
		if debug {
			logger.SetLevel(logrus.DebugLevel)
			logger.Debugf("loaded cli config from: %v", viper.ConfigFileUsed())
		}

		client, err := newCLIClient(cmd, logger, quiet)
		if err != nil {
			return fmt.Errorf("failed to create client: %v", err)
		}
		defer client.Close()

		tids := []string{}

		for _, a := range args {
			tid, err := uuid.Parse(a)
			if err != nil {
				return fmt.Errorf("failed to parse transferID[%s]: %v", a, err)
			}
			tids = append(tids, tid.String())
		}

		mtd, err := client.StopTransfer(tids)
		if err != nil {
			return fmt.Errorf("abort request failed: %v", err)
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

		return nil
	},
}

func init() {
	abortCmd.Flags().BoolVarP(&quiet, "quiet", "q", false, "reduce command output to only a transferID")

	RootCmd.AddCommand(abortCmd)
}
