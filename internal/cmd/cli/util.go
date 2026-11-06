// Copyright 2026. Triad National Security, LLC. All rights reserved.

package clicmd

import (
	"fmt"

	"github.com/lanl/conduit/defaults"
	"github.com/lanl/conduit/internal/cli/client"
	"github.com/lanl/conduit/internal/cli/util"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// cliClientCredentials uses exactly the same cert/bundle resolution as
// describe, cp, and other CLI commands. The cert-key-bundle flag should be
// inherited from the root command.
func cliClientCredentials(cmd *cobra.Command) (string, string, error) {
	bundle, err := cmd.Flags().GetString("cert-key-bundle")
	if err != nil {
		return "", "", fmt.Errorf("failed to get cert-key-bundle flag: %w", err)
	}

	cert, key, err := util.GetUserCertAndKey(
		viper.GetString(defaults.ConfigClientCertKey),
		viper.GetString(defaults.ConfigClientKeyKey),
		bundle,
		defaults.DefaultBundlePath,
	)
	if err != nil {
		return "", "", fmt.Errorf("failed to resolve client certificate and key: %w", err)
	}
	return cert, key, nil
}

// newCLIClient gets the mtls cert from the user's flags and returns a conduit client
func newCLIClient(cmd *cobra.Command, log *logrus.Logger, quiet bool) (*client.ConduitClient, error) {
	cert, key, err := cliClientCredentials(cmd)
	if err != nil {
		return nil, err
	}
	return client.NewClient(log, quiet, cert, key)
}
