// Copyright 2026. Triad National Security, LLC. All rights reserved.

package runnercmd

import (
	"fmt"
	"net"
	"os"

	"github.com/lanl/conduit/defaults"
	"github.com/lanl/conduit/internal/etcd"
	"github.com/lanl/conduit/internal/etcd/util"
	"github.com/lanl/conduit/internal/logger"
	"github.com/lanl/conduit/internal/pki"
	internal "github.com/lanl/conduit/internal/runner"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile string
	debug   bool

	// RootCmd represents the base command when called without any subcommands
	RootCmd = &cobra.Command{
		Use:   "conduit-runner",
		Short: "start the conduit runner",
		Long:  `start the conduit runner`,
		RunE: func(cmd *cobra.Command, args []string) error {

			log := logger.NewConduitLogger(logrus.InfoLevel, "")
			if debug {
				log = logger.NewConduitLogger(logrus.DebugLevel, "")
			}

			// Initializing keys and IPs for signing certificate (cert)
			caCertPath := viper.GetString(defaults.ConfigInternalCACertKey)
			caKeyPath := viper.GetString(defaults.ConfigInternalCAKeyKey)
			serverIPStrings := viper.GetStringSlice(defaults.ConfigServerIPKey)
			serverIPs := []net.IP{}

			for _, sips := range serverIPStrings {
				sip := net.ParseIP(sips)
				serverIPs = append(serverIPs, sip)
			}
			serverHostnames := viper.GetStringSlice(defaults.ConfigServerHostnameKey)

			// Creating internal cert manager
			icm, err := pki.NewInternalCertManager(log, caCertPath, caKeyPath, serverIPs, serverHostnames)
			if err != nil {
				return fmt.Errorf("failed to create cert manager: %v", err)
			}

			cm := &pki.CertManager{
				InternalCertManager: icm,
			}

			// Giving the transport layer security (tls) certificate from the etcd client
			log.Info("getting etcd client tls cert")
			tlsCert, err := icm.GetETCDClientTLSCert()
			if err != nil {
				return fmt.Errorf("failed to get tls cert for etcd client: %v", err)
			}

			log.Info("creating etcd cert pool")
			certPool, err := cm.GetCertPool(pki.INTERNAL)
			if err != nil {
				return fmt.Errorf("Failed to get cert pool for server cert: %v", err)
			}

			endpoints, err := util.GetEtcdEndpointsFromViper()
			if err != nil {
				return fmt.Errorf("failed to get etcd endpoints from config: %v", err)
			}

			em := etcd.NewETCDManager(log, tlsCert, certPool, endpoints)

			// Starting new runner with internal tls cert
			runner := internal.NewRunner(log, icm, em)
			err = runner.StartRunner()
			if err != nil {
				return fmt.Errorf("failed to start runner: %v", err)
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

	RootCmd.PersistentFlags().IntP("port", "p", DefaultPort, "Port to run conduit server on")
	RootCmd.PersistentFlags().StringSliceP("ip", "i", DefaultIPNet, "IP to run conduit server on")
	RootCmd.PersistentFlags().StringSlice("hostname", DefaultHostname, "The hostname for the conduit server. This is used for generating the tls cert")
	RootCmd.PersistentFlags().String("internal-ca-cert", DefaultInternalCACertLocation, "location of the internal ca cert .pem file")
	RootCmd.PersistentFlags().String("internal-ca-key", DefaultInternalCAKeyLocation, "location of the internal ca key .pem file")
	RootCmd.PersistentFlags().String("fta-path", DefaultFTAPath, "location of the conduit-fta executable on the fta node")

	viper.BindPFlag(defaults.ConfigServerPortKey, RootCmd.PersistentFlags().Lookup("port"))
	viper.BindPFlag(defaults.ConfigServerIPKey, RootCmd.PersistentFlags().Lookup("ip"))
	viper.BindPFlag(defaults.ConfigServerHostnameKey, RootCmd.PersistentFlags().Lookup("hostname"))
	viper.BindPFlag(defaults.ConfigInternalCACertKey, RootCmd.PersistentFlags().Lookup("internal-ca-cert"))
	viper.BindPFlag(defaults.ConfigInternalCAKeyKey, RootCmd.PersistentFlags().Lookup("internal-ca-key"))
}
