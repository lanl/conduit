// Copyright 2026. Triad National Security, LLC. All rights reserved.

package clicmd

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"

	"github.com/lanl/conduit/defaults"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var certCmd = &cobra.Command{
	Use:    "cert",
	Short:  "gets a cert for the user to use for authentication",
	Long:   `This command authenticates with conduit using kerberos or a TLS certificate and retreives a cert for the provided user`,
	Hidden: false,
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

		resp, err := client.GetCert(providedUser)
		if err != nil {
			return fmt.Errorf("cert request failed: %v", err)
		}

		outputPath, err := cmd.Flags().GetString("output")
		if err != nil {
			return fmt.Errorf("failed to get output flag: %v", err)
		}

		// if the user hasn't changed the default output path, then find the users home directory
		if outputPath == defaults.DefaultBundlePath {
			// get the users home directory
			homeDir, err := os.UserHomeDir()
			if err != nil {
				return fmt.Errorf("failed to get users home directory: %v", err)
			}

			outputPath = filepath.Join(homeDir, defaults.DefaultBundleName)
		}

		// check if overwritting existing cert
		if _, err := os.Stat(outputPath); err == nil {
			fmt.Printf("overwritting existing keypair at [%s]\n", outputPath)
		} else if errors.Is(err, os.ErrNotExist) {
			fmt.Printf("writing keypair to [%s]\n", outputPath)
		} else {
			return fmt.Errorf("failed to check for existing keypair at [%s]: %v", outputPath, err)
		}

		// Write the data to the file
		permissions := 0600
		err = os.WriteFile(outputPath, resp.GetCert(), os.FileMode(permissions))
		if err != nil {
			return fmt.Errorf("failed to write keypair to file: %v", err)
		}

		// print useful conduit information
		conduitIP := viper.GetString(defaults.ConfigConduitIPKey)
		conduitPort := strconv.Itoa(viper.GetInt(defaults.ConfigConduitPortKey))
		conduitAddr := net.JoinHostPort(conduitIP, conduitPort)
		caPath := viper.GetString(defaults.ConfigConduitCAKey)

		fmt.Printf("Conduit Server Address: %s\n", conduitAddr)
		if caPath != "" {
			fmt.Printf("Conduit CA Path: %s\n", caPath)
		}

		return nil
	},
}

func init() {
	RootCmd.AddCommand(certCmd)

	certCmd.Flags().StringVar(&providedUser, "user", "", "Retrieve a cert for this user. Requires an admin cert & key to be provided")
	certCmd.Flags().StringP("output", "o", defaults.DefaultBundlePath, "path to write the cert-key-bundle. Defaults to users home directory")
}
