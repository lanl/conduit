// Copyright 2026. Triad National Security, LLC. All rights reserved.

package clicmd

import (
	"fmt"
	"os"

	"github.com/lanl/conduit/internal/version"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// versionCmd represents the version command
var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "print cli version and conduit server version",
	Long:  `print cli version and conduit server version`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("conduit-cli version: %s\n", version.Current())

		logger := logrus.New()
		if debug {
			logger.SetLevel(logrus.DebugLevel)
		}

		client, err := newCLIClient(cmd, logger, quiet)
		if err != nil {
			fmt.Printf("failed to check conduit-server version: %v\n", err)
			os.Exit(1)
		}

		cvi, err := client.GetVersion()
		if err != nil {
			fmt.Printf("failed to check conduit-server version: %v\n", err)
			os.Exit(1)
		}
		cv := cvi.GetVersion()

		fmt.Printf("conduit-server version: %s\n", cv)
	},
}

func init() {
	RootCmd.AddCommand(versionCmd)
}
