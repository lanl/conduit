// Copyright 2026. Triad National Security, LLC. All rights reserved.

package clicmd

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var rerunCmd = &cobra.Command{
	Use:    "rerun [TRANSFER_ID]",
	Short:  "rerun transfer(s)",
	Long:   `rerun will delete a transfer from rqlite and rerun it with the exact same options and transfer ID`,
	Hidden: true,
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return fmt.Errorf("at least one transfer id is required")
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

		// the args function already validated the uuids (args)
		mtd, err := client.ReRunTransfers(args)
		if err != nil {
			return fmt.Errorf("rerun request failed: %v", err)
		}

		tids := []string{}
		for _, t := range mtd.GetDetails() {
			tids = append(tids, t.GetTransferID())
		}

		if len(tids) == 0 {
			return fmt.Errorf("no transfers found")
		}

		fmt.Printf("successfully reran transfers: %v\n", tids)
		return nil
	},
}

func init() {

	RootCmd.AddCommand(rerunCmd)
}
