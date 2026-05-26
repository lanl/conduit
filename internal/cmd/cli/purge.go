// Copyright 2026. Triad National Security, LLC. All rights reserved.

package clicmd

import (
	"fmt"
	"os"

	"github.com/common-nighthawk/go-figure"
	"github.com/fatih/color"
	"github.com/lanl/conduit/defaults"
	"github.com/lanl/conduit/internal/cli/client"
	"github.com/lanl/conduit/internal/cli/util"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	errantWarning     = "You have failed transfer trash that needs to be cleaned up:"
	errantDescription = "Please manually move or delete trash and then use the \"conduit purge\" command to continue using conduit"
)

var purgeCmd = &cobra.Command{
	Use:   "purge TRASH_PATH...",
	Short: "remove errored transfer trash path from conduit",
	Long:  `if files are edited mid transfer its possible some files may end up in an errored trash location. Use this command after you've relocated these trashed items`,
	Args: func(cmd *cobra.Command, args []string) error {
		// check for at least one argument
		if err := cobra.MinimumNArgs(1)(cmd, args); err != nil {
			return err
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

		resp, err := client.PurgeErrantPaths(args, providedUser)
		if err != nil {
			fmt.Printf("purge request failed: %v\n", err)
			os.Exit(1)
		} else {
			paths := []string{}
			for p := range resp.GetPaths() {
				paths = append(paths, p)
			}

			if len(paths) > 1 {
				if quiet {
					fmt.Printf("%v\n", paths)
				} else {
					fmt.Printf("successfully purged paths: %v\n", paths)
				}
			} else if len(paths) > 0 {
				if quiet {
					fmt.Printf("%v\n", paths[0])
				} else {
					fmt.Printf("successfully purged transfer: %v\n", paths[0])
				}
			} else {
				fmt.Printf("no transfers purged\n")
			}
		}
	},
}

func init() {
	purgeCmd.Flags().StringVar(&providedUser, "user", "", "The user to purge the transfer as. Requires an admin cert & key to be provided")

	RootCmd.AddCommand(purgeCmd)

}

func printErrantPaths(logger *logrus.Logger, client *client.ConduitClient, doneChan chan bool) {
	logger.Debugf("getting errant paths")
	// check if the user has any errant paths
	errantPaths, err := client.ErrantPaths()
	logger.Debugf("got errant paths: %v", errantPaths)
	if err != nil {
		logger.Warnf("failed to get errant paths from conduit: %v", err)
	} else {
		finalErrantPaths := make(map[string]*timestamppb.Timestamp)
		for p, t := range errantPaths.GetPaths() {
			if !t.AsTime().IsZero() {
				finalErrantPaths[p] = t
			}
		}

		if len(finalErrantPaths) > 0 {
			warningFigure := figure.NewColorFigure("WARNING", "", "yellow", false)
			fmt.Println()
			warningFigure.Print()
			fmt.Println(errantWarning)
			for p := range finalErrantPaths {
				println(color.YellowString(p))
			}
			fmt.Printf("%s\n\n", errantDescription)
		}

	}

	doneChan <- true
}
