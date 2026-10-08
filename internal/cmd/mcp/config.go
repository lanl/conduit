// Copyright 2026. Triad National Security, LLC. All rights reserved.

package mcpcmd

import (
	"path/filepath"
	"strings"

	"github.com/lanl/conduit/defaults"
	"github.com/lanl/conduit/internal/mcp"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	DefaultPort           = 8081
	DefaultConfigLocation = "/etc/conduit/"
	ConfigName            = "conduit-mcp-config"
	ConfigType            = "yaml"
	envPrefix             = "CONDUIT_MCP"
	DefaultDebug          = false
	DefaultIP             = "127.0.0.1"
)

var (
	finalConfigPath = ""
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

	// Bind to environment variables
	viper.SetEnvPrefix(envPrefix)
	viper.SetEnvKeyReplacer(strings.NewReplacer(
		".", "_",
		"-", "_",
	))
	viper.AutomaticEnv()
}

func createDefaultConfig() {
	// mcp config
	viper.SetDefault(defaults.ConfigServerIPKey, DefaultIP)
	viper.SetDefault(defaults.ConfigServerPortKey, DefaultPort)
	viper.SetDefault(defaults.ConfigMCPPublicBaseURLKey, "")
	viper.SetDefault(defaults.ConfigMCPResourcePathKey, mcp.DefaultResourcePath)
	viper.SetDefault(defaults.ConfigMCPResourceMetadataPathKey, mcp.DefaultMetadataPath)
	viper.SetDefault(defaults.ConfigServerHTTPAllowedOriginsKey, []string{})
	viper.SetDefault(defaults.ConfigServerHTTPTLSCertKey, "")
	viper.SetDefault(defaults.ConfigServerHTTPTLSKeyKey, "")

	// conduit config
	viper.SetDefault(defaults.ConfigConduitIPKey, defaults.DefaultConduitHost)
	viper.SetDefault(defaults.ConfigConduitPortKey, defaults.DefaultConduitPort)
	viper.SetDefault(defaults.ConfigConduitCAKey, defaults.DefaultConduitCA)
	viper.SetDefault(defaults.ConfigConduitTimeoutKey, defaults.DefaultReqTimeout)

	// client config
	viper.SetDefault(defaults.ConfigClientGrpcLimitKey, defaults.DefaultClientGRPCLimit)
	viper.SetDefault(defaults.ConfigClientCertKey, defaults.DefaultClientCert)
	viper.SetDefault(defaults.ConfigClientKeyKey, defaults.DefaultClientKey)

	// oauth config
	viper.SetDefault(defaults.ConfigOAuthUserFallbackKey, defaults.DefaultOAuthUserFallback)
	viper.SetDefault(defaults.ConfigOAuthUserClaimsKey, defaults.DefaultUsernameClaims)
	viper.SetDefault(defaults.ConfigOAuthDiscoveryKey, "")
	viper.SetDefault(defaults.ConfigOAuthclientIDKey, "")
	viper.SetDefault(defaults.ConfigOAuthclientSecretKey, "")
	viper.SetDefault(defaults.ConfigOAuthRequiredScopesKey, []string{})
	viper.SetDefault(defaults.ConfigOAuthSupportedScopesKey, defaults.DefaultSupportedScopes)
	viper.SetDefault(defaults.ConfigOAuthTokenFallbackTTLKey, mcp.DefaultTokenFallbackTTL)
	viper.SetDefault(defaults.ConfigOAuthCAKey, "")

	viper.SetDefault(defaults.ConfigDebugKey, DefaultDebug)

	err := viper.SafeWriteConfig()
	if err != nil {
		logrus.Warnf("failed to write default config: %v", err)
	} else {
		logrus.Infof("wrote default config to: %v", finalConfigPath)
	}
}
