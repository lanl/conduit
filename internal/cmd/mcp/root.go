// Copyright 2026. Triad National Security, LLC. All rights reserved.

package mcpcmd

import (
	"fmt"
	"net"
	"os"
	"strconv"

	"github.com/lanl/conduit/defaults"
	cliutil "github.com/lanl/conduit/internal/cli/util"
	"github.com/lanl/conduit/internal/logger"
	"github.com/lanl/conduit/internal/mcp"
	"github.com/lanl/conduit/internal/pki"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile string
	debug   bool

	// RootCmd represents the base command when called without any subcommands
	RootCmd = &cobra.Command{
		Use:   "conduit-mcp",
		Short: "start the conduit mcp server",
		Long:  `start the conduit mcp server`,
		RunE: func(cmd *cobra.Command, args []string) error {

			log := logger.NewConduitLogger(logrus.InfoLevel, "")
			if debug {
				log = logger.NewConduitLogger(logrus.DebugLevel, "")
			}

			port := viper.GetInt(defaults.ConfigServerPortKey)
			serverIP := net.ParseIP(viper.GetString(defaults.ConfigServerIPKey))

			mcpAddr := net.JoinHostPort(serverIP.String(), strconv.Itoa(port))

			clientCertConfigPath := viper.GetString(defaults.ConfigClientCertKey)
			clientKeyConfigPath := viper.GetString(defaults.ConfigClientKeyKey)

			if clientCertConfigPath == "" && clientKeyConfigPath == "" {
				return fmt.Errorf("no client cert or key is configured")
			}

			clientCertPath, clientKeyPath, err := cliutil.GetUserCertAndKey(clientCertConfigPath, clientKeyConfigPath, "", "")
			if err != nil {
				return fmt.Errorf("failed to get client cert and key: %v", err)
			}

			log.Debugf("using user cert [%v] and key [%v]", clientCertPath, clientKeyPath)

			tlsCert, err := pki.GetKeyPairFromFile(clientCertPath, clientKeyPath)
			if err != nil {
				return fmt.Errorf("failed to load cert[%s] and key[%s] from file: %v", clientCertPath, clientKeyPath, err)
			}

			certPool, err := cliutil.GetCertPoolFromViper(defaults.ConfigConduitCAKey)
			if err != nil {
				return fmt.Errorf("failed to get CA cert from config: %v", err)
			}

			conduitIP := viper.GetString(defaults.ConfigConduitIPKey)
			conduitPort := strconv.Itoa(viper.GetInt(defaults.ConfigConduitPortKey))
			conduitAddr := net.JoinHostPort(conduitIP, conduitPort)

			mcpServer, err := mcp.CreateMCPServer(log, mcpAddr, tlsCert, certPool, conduitAddr)
			if err != nil {
				return fmt.Errorf("failed to create mcp server: %v", err)
			}

			err = mcpServer.StartMCPServer()
			if err != nil {
				return fmt.Errorf("failed to run mcp server: %v", err)
			}

			return nil
		},
	}
)

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		logrus.Errorf("failed to execute root command: %v", err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(func() { initConfig(cfgFile) })

	// global flags
	RootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", fmt.Sprintf("config file (default is %s%s.%s)", DefaultConfigLocation, ConfigName, ConfigType))
	RootCmd.PersistentFlags().BoolVarP(&debug, "debug", "d", false, "enable debugging")
}
