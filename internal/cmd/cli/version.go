// Copyright 2026. Triad National Security, LLC. All rights reserved.

package clicmd

import (
	"fmt"

	"github.com/lanl/conduit/internal/version"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// versionCmd represents the version command
var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "print cli version and conduit server version",
	Long:  `print cli version and conduit server version`,
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Printf("conduit-cli version: %s\n", version.Current())

		logger := logrus.New()
		if debug {
			logger.SetLevel(logrus.DebugLevel)
		}

		client, err := newCLIClient(cmd, logger, quiet)
		if err != nil {
			return fmt.Errorf("failed to create client: %v", err)
		}
		defer client.Close()

		cvi, err := client.GetVersion()
		if err != nil {
			return fmt.Errorf("failed to check conduit-server version: %v", err)
		}
		cv := cvi.GetVersion()

		fmt.Printf("conduit-server version: %s\n", cv)

		return nil
	},
}

func init() {
	RootCmd.AddCommand(versionCmd)
}
