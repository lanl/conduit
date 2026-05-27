// Copyright 2026. Triad National Security, LLC. All rights reserved.

package ftacmd

import (
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"time"

	"github.com/lanl/conduit/defaults"
	"github.com/lanl/conduit/internal/etcd/util"
	"github.com/lanl/conduit/internal/fta"
	"github.com/lanl/conduit/internal/fta/plugin"
	"github.com/lanl/conduit/internal/fta/plugins/posix"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	DefaultConfigLocation       = "/etc/conduit/"
	ConfigName                  = "conduit-fta-config"
	ConfigType                  = "yaml"
	envPrefix                   = "CONDUIT_FTA"
	DefaultCACertName           = "conduit-ca.pem"
	DefaultExpiryUpdateInterval = "30s"
	DefaultExpiryAdvance        = "5m"
	DefaultVerifySleepDuration  = 5 * time.Second
	DefaultVerifyRetryCount     = 20
)

var (
	DefaultETCDPort   = []int{2379}
	DefaultETCDIPNet  = []net.IP{net.IPv4(127, 0, 0, 1)}
	DefaultEtcdConfig = util.EViperConfig{
		IP:   DefaultETCDIPNet[0].String(),
		Port: DefaultETCDPort[0],
	}
	DefaultCACertLocation = fmt.Sprintf("%v/%v", filepath.Clean(DefaultConfigLocation), DefaultCACertName)
	DefaultFileSystems    = map[string]plugin.ViperFSConfig{plugin.DefaultFileSystemName: DefaultFileSystem}
	DefaultFileSystem     = plugin.ViperFSConfig{
		UserPathRegex:    `.*`,
		FTAPathSub:       `$0`,
		FTARootFSPathSub: `/`,
		PluginStages: plugin.ViperPluginStagesConfig{
			Validation:  `posix`,
			SetupSrc:    `posix`,
			SetupDst:    `posix`,
			TransferSrc: []string{`pftool`},
			TransferDst: []string{`pftool`},
			TeardownSrc: `posix`,
			TeardownDst: `posix`,
		},
		CustomPluginFSConfig: map[string]any{
			posix.CustomPluginConfigTrashKey: ``,
		},
	}
	finalConfigPath = ""
)

func initConfig(cfgFile string) {
	if debug {
		logrus.SetLevel(logrus.DebugLevel)
	}

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

	logrus.Infof("using fta config from: %v", viper.ConfigFileUsed())

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
	viper.SetDefault(defaults.ConfigInternalCACertKey, DefaultCACertLocation)

	viper.SetDefault(defaults.ConfigExpiryIntervalKey, DefaultExpiryUpdateInterval)
	viper.SetDefault(defaults.ConfigExpiryAdvanceKey, DefaultExpiryAdvance)

	pluginConfs := map[string]any{}
	for pluginKey, plugin := range fta.PluginMap {
		pluginConfig := plugin.GetDefaultConfig()
		if pluginConfig != nil {
			pluginConfs[pluginKey] = pluginConfig
		}
	}

	viper.SetDefault(defaults.ConfigPluginsKey, pluginConfs)

	viper.SetDefault(defaults.ConfigFTAVerifyRetryCountKey, DefaultVerifyRetryCount)
	viper.SetDefault(defaults.ConfigFTAVerifySleepDurationKey, DefaultVerifySleepDuration)

	viper.SetDefault(defaults.ConfigFilesystemsKey, DefaultFileSystems)

	if (RootCmd.PersistentFlags().Changed("etcd-ip") ||
		RootCmd.PersistentFlags().Changed("etcd-port")) &&
		len(etcdIPs) == len(etcdPorts) {
		etcdConfs := []*util.EViperConfig{}
		for i := 0; i < len(etcdIPs); i++ {
			ec := &util.EViperConfig{
				IP:   etcdIPs[i].String(),
				Port: etcdPorts[i],
			}
			etcdConfs = append(etcdConfs, ec)
		}
		viper.Set(defaults.ConfigETCDKey, etcdConfs)
	}

	viper.SetDefault(defaults.ConfigETCDKey, []util.EViperConfig{DefaultEtcdConfig})

	// logrus.Info(viper.AllSettings())
	// logrus.Info(viper.AllKeys())

	err := viper.SafeWriteConfig()
	if err != nil {
		logrus.Warnf("failed to write default config: %v", err)
	} else {
		logrus.Infof("wrote default config to: %v", finalConfigPath)
	}
}
