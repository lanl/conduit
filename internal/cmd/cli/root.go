// Copyright 2026. Triad National Security, LLC. All rights reserved.

package clicmd

import (
	"fmt"
	"os"
	"time"

	"github.com/lanl/conduit/defaults"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile      string
	debug        bool
	quiet        bool
	providedUser string
)

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "conduit",
	Short: "run conduit commands",
	Long:  `This application provides a method for interacting with the conduit API`,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(func() { initConfig(cfgFile) })

	timeout, err := time.ParseDuration(defaults.DefaultReqTimeout)
	if err != nil {
		timeout = time.Second * time.Duration(defaults.DefaultReqTimeoutSeconds)
	}

	// global flags
	RootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", fmt.Sprintf("config file (default is %s%s.%s)", defaultSystemConfigLocation, configName, configType))
	RootCmd.PersistentFlags().BoolVarP(&debug, "debug", "d", false, "enable debugging")

	RootCmd.PersistentFlags().IntP("port", "p", defaults.DefaultConduitPort, "Port of the conduit server")
	RootCmd.PersistentFlags().StringP("ip", "i", defaults.DefaultConduitHost, "Addr of the conduit server")
	RootCmd.PersistentFlags().StringP("ca", "c", defaults.DefaultConduitCA, "Location of conduit root CA")
	RootCmd.PersistentFlags().String("krb-config", defaults.DefaultKrb5Config, "Location of the krb5 config file")
	RootCmd.PersistentFlags().String("krb-cache", defaults.DefaultKrb5Cache, "Location of the krb5 ticket cache")
	RootCmd.PersistentFlags().String("krb-cache-prefix", defaults.DefaultKrb5CachePrefix, "The Prefix before the UID of the tickets located in the krb5 cache")
	RootCmd.PersistentFlags().String("krb-spn", defaults.DefaultSPN, "The conduit spn")
	RootCmd.PersistentFlags().Duration("req-timeout", timeout, "Timeout as a duration for requests made to conduit")
	RootCmd.PersistentFlags().Int("grpc-limit", defaults.DefaultClientGRPCLimit, "The size limit (in bytes) of grpc messages received from conduit")
	RootCmd.PersistentFlags().String("cert", defaults.DefaultClientCert, "The path to the mTLS client cert to use for the request")
	RootCmd.PersistentFlags().String("key", defaults.DefaultClientKey, "The path to the mTLS client key to use for the request")
	RootCmd.PersistentFlags().String("cert-key-bundle", defaults.DefaultBundlePath, "shorthand for setting --cert and --key to the same path")

	viper.BindPFlag("debug", RootCmd.PersistentFlags().Lookup("debug"))
	viper.BindPFlag(defaults.ConfigKrbConfigKey, RootCmd.PersistentFlags().Lookup("krb-config"))
	viper.BindPFlag(defaults.ConfigKrbCacheKey, RootCmd.PersistentFlags().Lookup("krb-cache"))
	viper.BindPFlag(defaults.ConfigKrbCachePrefixKey, RootCmd.PersistentFlags().Lookup("krb-cache-prefix"))
	viper.BindPFlag(defaults.ConfigKrbSpnKey, RootCmd.PersistentFlags().Lookup("krb-spn"))
	viper.BindPFlag(defaults.ConfigConduitTimeoutKey, RootCmd.PersistentFlags().Lookup("req-timeout"))
	viper.BindPFlag(defaults.ConfigConduitPortKey, RootCmd.PersistentFlags().Lookup("port"))
	viper.BindPFlag(defaults.ConfigConduitIPKey, RootCmd.PersistentFlags().Lookup("ip"))
	viper.BindPFlag(defaults.ConfigConduitCAKey, RootCmd.PersistentFlags().Lookup("ca"))
	viper.BindPFlag(defaults.ConfigClientGrpcLimitKey, RootCmd.PersistentFlags().Lookup("grpc-limit"))
	viper.BindPFlag(defaults.ConfigClientCertKey, RootCmd.PersistentFlags().Lookup("cert"))
	viper.BindPFlag(defaults.ConfigClientKeyKey, RootCmd.PersistentFlags().Lookup("key"))
}
