// Copyright 2026. Triad National Security, LLC. All rights reserved.

package runnercmd

import (
	"fmt"
	"net"
	"path/filepath"
	"strings"

	"github.com/lanl/conduit/defaults"
	eutil "github.com/lanl/conduit/internal/etcd/util"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	DefaultPort               = 23457
	DefaultConfigLocation     = "/etc/conduit/"
	ConfigName                = "conduit-runner-config"
	ConfigType                = "yaml"
	envPrefix                 = "CONDUIT_RUNNER"
	DefaultInternalCACertName = "conduit-internal-ca.pem"
	DefaultInternalCAKeyName  = "conduit-internal-key.pem"
	DefaultFTAPath            = "conduit-fta"
)

var (
	finalConfigPath = ""

	DefaultETCDIPNet    = []net.IP{net.IPv4(127, 0, 0, 1)}
	DefaultETCDHostname = []string{"etcd.example.com"}
	DefaultETCDPort     = []int{2379}
	DefaultEtcdConfig   = eutil.EViperConfig{
		Hostname: DefaultETCDHostname[0],
		IP:       DefaultETCDIPNet[0].String(),
		Port:     DefaultETCDPort[0],
	}
	DefaultIPNet                  = []string{"127.0.0.1"}
	DefaultInternalCACertLocation = fmt.Sprintf("%v/%v", filepath.Clean(DefaultConfigLocation), DefaultInternalCACertName)
	DefaultInternalCAKeyLocation  = fmt.Sprintf("%v/%v", filepath.Clean(DefaultConfigLocation), DefaultInternalCAKeyName)
	DefaultFTAOptions             = []string{""}
	DefaultHostname               = []string{"conduitrunner.example.com"}

	DefaultEnvironment = map[string]string{
		"PATH":            "/bin:/usr/bin/:/usr/local/bin/",
		"LD_LIBRARY_PATH": "/lib/:/lib64/:/usr/local/lib",
	}
)

func initConfig(cfgFile string) {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
		finalConfigPath = cfgFile

		viper.SetConfigName(strings.Split(filepath.Base(cfgFile), ".")[0])
		viper.SetConfigType(strings.TrimPrefix(filepath.Ext(cfgFile), "."))
		viper.AddConfigPath(filepath.Dir(cfgFile))
	} else {
		viper.SetConfigName(ConfigName)
		viper.SetConfigType(ConfigType)
		viper.AddConfigPath(DefaultConfigLocation)

		finalConfigPath = filepath.Join(DefaultConfigLocation, ConfigName+"."+ConfigType)
	}

	createDefaultConfig()

	// Attempt to read the config file, gracefully ignoring errors
	// caused by a config file not being found. Return an error
	// if we cannot parse the config file.
	if err := viper.ReadInConfig(); err != nil {
		logrus.Errorf("failed to read config file: %v", err)
	}

	// When we bind flags to environment variables expect that the
	// environment variables are prefixed, e.g. a flag like --number
	// binds to an environment variable STING_NUMBER. This helps
	// avoid conflicts.
	viper.SetEnvPrefix(envPrefix)

	// Bind to environment variables
	// Works great for simple config names, but needs help for names
	// like --favorite-color which we fix in the bindFlags function
	viper.AutomaticEnv()
}

func createDefaultConfig() {
	viper.SetDefault(defaults.ConfigInternalCACertKey, DefaultInternalCACertLocation)
	viper.SetDefault(defaults.ConfigInternalCAKeyKey, DefaultInternalCAKeyLocation)

	viper.SetDefault(defaults.ConfigETCDKey, []eutil.EViperConfig{DefaultEtcdConfig})

	viper.SetDefault(defaults.ConfigServerIPKey, DefaultIPNet)
	viper.SetDefault(defaults.ConfigServerPortKey, DefaultPort)
	viper.SetDefault(defaults.ConfigServerHostnameKey, DefaultHostname)

	viper.SetDefault(defaults.ConfigFTAPathKey, DefaultFTAPath)
	viper.SetDefault(defaults.ConfigFTAOptionsKey, DefaultFTAOptions)
	viper.SetDefault(defaults.ConfigFTAEnvKey, DefaultEnvironment)

	err := viper.SafeWriteConfig()
	if err != nil {
		logrus.Warnf("failed to write default config: %v", err)
	} else {
		logrus.Infof("wrote default config to: %v", finalConfigPath)
	}
}
