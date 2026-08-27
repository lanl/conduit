// Copyright 2026. Triad National Security, LLC. All rights reserved.

package servercmd

import (
	"fmt"
	"net"
	"net/http"
	"os"

	_ "net/http/pprof"

	"github.com/lanl/conduit/defaults"
	"github.com/lanl/conduit/internal/server/grpcserver"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile         string
	debug           bool
	etcdIPs         []net.IP
	etcdPorts       []int
	etcdHostnames   []string
	rqliteIPs       []net.IP
	rqlitePorts     []int
	rqliteHostnames []string

	// RootCmd represents the base command when called without any subcommands
	RootCmd = &cobra.Command{
		Use:   "conduit",
		Short: "run conduit commands",
		Long:  `This is conduit`,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if len(etcdIPs) != len(etcdHostnames) || len(etcdHostnames) != len(etcdPorts) {
				return fmt.Errorf("must provide equal numbers of etcd-ip, etcd-port, and etcd-hostname")
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			if viper.GetBool(defaults.ConfigTestKey) {
				logrus.Error("CONDUIT IN TEST MODE! THIS SHOULD NEVER HAPPEN IN PRODUCTION")

				if debug {
					go func() {
						if err := http.ListenAndServe(":6060", nil); err != nil {
							logrus.Errorf("pprof server failed: %v", err)
						}
					}()
				}
			}
			s, err := grpcserver.CreateConduitServer(debug)
			if err != nil {
				logrus.Errorf("failed to create conduit server: %v", err)
				os.Exit(1)
			}
			err = s.StartConduitServer()
			if err != nil {
				logrus.Errorf("conduit exiting with err: %v", err)
				os.Exit(1)
			}
			os.Exit(0)
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
	RootCmd.PersistentFlags().String("keytab", DefaultKeytabLocation, "location of the krb5 keytab file for the conduit service")
	RootCmd.PersistentFlags().String("external-ca-cert", DefaultExternalCACertLocation, "location of the external ca cert .pem file")
	RootCmd.PersistentFlags().String("external-ca-key", DefaultExternalCAKeyLocation, "location of the external ca key .pem file")
	RootCmd.PersistentFlags().String("internal-ca-cert", DefaultInternalCACertLocation, "location of the internal ca cert .pem file")
	RootCmd.PersistentFlags().String("internal-ca-key", DefaultInternalCAKeyLocation, "location of the internal ca key .pem file")
	RootCmd.PersistentFlags().IPSliceVar(&etcdIPs, "etcd-ip", DefaultETCDIPNet, "ip address(es) of etcd")
	RootCmd.PersistentFlags().IntSliceVar(&etcdPorts, "etcd-port", DefaultETCDPort, "client port(s) of etcd")
	RootCmd.PersistentFlags().StringSliceVar(&etcdHostnames, "etcd-hostname", DefaultETCDHostname, "hostname(s) of etcd")
	RootCmd.PersistentFlags().IPSliceVar(&rqliteIPs, "rqlite-ip", DefaultRqliteIPNet, "ip address(es) of rqlite")
	RootCmd.PersistentFlags().IntSliceVar(&rqlitePorts, "rqlite-port", DefaultRqlitePort, "client port(s) of rqlite")
	RootCmd.PersistentFlags().StringSliceVar(&rqliteHostnames, "rqlite-hostname", DefaultRqliteHostname, "hostname(s) of rqlite")

	viper.BindPFlag(defaults.ConfigServerPortKey, RootCmd.PersistentFlags().Lookup("port"))
	viper.BindPFlag(defaults.ConfigServerIPKey, RootCmd.PersistentFlags().Lookup("ip"))
	viper.BindPFlag(defaults.ConfigServerHostnameKey, RootCmd.PersistentFlags().Lookup("hostname"))
	viper.BindPFlag(defaults.ConfigAuthKeytabKey, RootCmd.PersistentFlags().Lookup("keytab"))
	viper.BindPFlag(defaults.ConfigInternalCACertKey, RootCmd.PersistentFlags().Lookup("internal-ca-cert"))
	viper.BindPFlag(defaults.ConfigInternalCAKeyKey, RootCmd.PersistentFlags().Lookup("internal-ca-key"))
	viper.BindPFlag(defaults.ConfigExternalCACertKey, RootCmd.PersistentFlags().Lookup("external-ca-cert"))
	viper.BindPFlag(defaults.ConfigExternalCAKeyKey, RootCmd.PersistentFlags().Lookup("external-ca-key"))

	RootCmd.PersistentFlags().BoolP("test", "t", false, "Test mode flag. This should never be used in production")
	viper.BindPFlag(defaults.ConfigTestKey, RootCmd.PersistentFlags().Lookup("test"))
}
