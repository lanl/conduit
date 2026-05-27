// Copyright 2026. Triad National Security, LLC. All rights reserved.

package servercmd

import (
	"fmt"
	"net"
	"path/filepath"
	"strings"

	"github.com/lanl/conduit/defaults"
	eutil "github.com/lanl/conduit/internal/etcd/util"
	rutil "github.com/lanl/conduit/internal/server/rqlite/util"
	sutil "github.com/lanl/conduit/internal/server/scheduler/util"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	DefaultPort                     = 23456
	DefaultWSPort                   = 8080
	DefaultConfigLocation           = "/etc/conduit/"
	ConfigName                      = "conduit-server-config"
	ConfigType                      = "yaml"
	envPrefix                       = "CONDUIT"
	DefaultKeytabName               = "conduit.keytab"
	DefaultInternalCACertName       = "conduit-internal-ca.pem"
	DefaultInternalCAKeyName        = "conduit-internal-key.pem"
	DefaultExternalCACertName       = "conduit-external-ca.pem"
	DefaultExternalCAKeyName        = "conduit-external-key.pem"
	DefaultClientCertExpirationDays = 10

	DefaultNodeAllocationsValidationNodes  = 1
	DefaultNodeAllocationsValidationMemory = "10MB"
	DefaultNodeAllocationsSetupNodes       = 1
	DefaultNodeAllocationsSetupMemory      = "10MB"
	DefaultNodeAllocationsTransferNodes    = 2
	DefaultNodeAllocationsTransferMemory   = "500MB"
	DefaultNodeAllocationsTeardownNodes    = 1
	DefaultNodeAllocationsTeardownMemory   = "10MB"

	DefaultLDAPHost = ""
	DefaultLDAPPort = 389

	DefaultMaxSourceBytes = 4000
	DefaultExpiryAdvance  = "60s"

	DefaultErrantExpiration      = "336h" // two weeks
	DefaultRequestedCertLifetime = "24h"

	DefaultNodesMinMem  = "1GB"
	DefaultNodesMaxJobs = 4

	DefaultConcurrentWorkers    = 2
	DefaultConcurrentWatchDogs  = 2
	DefaultConcurrentSchedulers = 2
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
	DefaultNodesIPNet  = []net.IP{net.IPv4(127, 0, 0, 1)}
	DefaultNodesPort   = []int{23457}
	DefaultNodesConfig = sutil.NodesViperConfig{
		Nodes: map[string]*sutil.NViperConfig{
			"node1": &sutil.NViperConfig{
				Address:   DefaultNodesIPNet[0].String(),
				Port:      DefaultNodesPort[0],
				MinMemory: DefaultNodesMinMem,
				MaxJobs:   DefaultNodesMaxJobs,
			},
		},
	}

	DefaultRqliteIPNet    = []net.IP{net.IPv4(127, 0, 0, 1)}
	DefaultRqliteHostname = []string{"rqlite.example.com"}
	DefaultRqlitePort     = []int{4001}
	DefaultRqliteConfig   = rutil.RViperConfig{
		Hostname: DefaultRqliteHostname[0],
		IP:       DefaultRqliteIPNet[0].String(),
		Port:     DefaultRqlitePort[0],
	}
	DefaultIPNet                  = []string{"127.0.0.1"}
	DefaultKeytabLocation         = fmt.Sprintf("%v/%v", filepath.Clean(DefaultConfigLocation), DefaultKeytabName)
	DefaultInternalCACertLocation = fmt.Sprintf("%v/%v", filepath.Clean(DefaultConfigLocation), DefaultInternalCACertName)
	DefaultInternalCAKeyLocation  = fmt.Sprintf("%v/%v", filepath.Clean(DefaultConfigLocation), DefaultInternalCAKeyName)
	DefaultExternalCACertLocation = fmt.Sprintf("%v/%v", filepath.Clean(DefaultConfigLocation), DefaultExternalCACertName)
	DefaultExternalCAKeyLocation  = fmt.Sprintf("%v/%v", filepath.Clean(DefaultConfigLocation), DefaultExternalCAKeyName)
	// DefaultFTAOptions             = []string{""}
	DefaultHostname = []string{"conduit-server.example.com"}

	DefaultLDAPBaseDN              = []string{}
	DefaultLDAPKrb5Attributes      = []string{}
	DefaultLDAPUnameAttributes     = []string{"uid"}
	DefaultLDAPUIDNumberAttributes = []string{"uidNumber"}
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
	viper.SetDefault(defaults.ConfigServerIPKey, DefaultIPNet)
	viper.SetDefault(defaults.ConfigServerPortKey, DefaultPort)
	viper.SetDefault(defaults.ConfigServerWSPortKey, DefaultWSPort)
	viper.SetDefault(defaults.ConfigServerHostnameKey, DefaultHostname)
	viper.SetDefault(defaults.ConfigAuthKeytabKey, DefaultKeytabLocation)
	viper.SetDefault(defaults.ConfigInternalCACertKey, DefaultInternalCACertLocation)
	viper.SetDefault(defaults.ConfigInternalCAKeyKey, DefaultInternalCAKeyLocation)
	viper.SetDefault(defaults.ConfigExternalCACertKey, DefaultExternalCACertLocation)
	viper.SetDefault(defaults.ConfigExternalCAKeyKey, DefaultExternalCAKeyLocation)
	viper.SetDefault(defaults.ConfigCertOrganizationKey, defaults.DefaultCertOrganization)
	viper.SetDefault(defaults.ConfigCertCountryKey, defaults.DefaultCertOrganization)
	viper.SetDefault(defaults.ConfigCertProvinceKey, defaults.DefaultCertProvince)
	viper.SetDefault(defaults.ConfigCertLocalityKey, defaults.DefaultCertLocality)
	viper.SetDefault(defaults.ConfigCertPostalCodeKey, defaults.DefaultCertPostalCode)

	// format etcd stuff in yaml file
	if (RootCmd.PersistentFlags().Changed("etcd-ip") ||
		RootCmd.PersistentFlags().Changed("etcd-hostname") ||
		RootCmd.PersistentFlags().Changed("etcd-port")) &&
		len(etcdIPs) == len(etcdPorts) &&
		len(etcdPorts) == len(etcdHostnames) {
		etcdConfs := []*eutil.EViperConfig{}
		for i := 0; i < len(etcdIPs); i++ {
			ec := &eutil.EViperConfig{
				IP:       etcdIPs[i].String(),
				Hostname: etcdHostnames[i],
				Port:     etcdPorts[i],
			}
			etcdConfs = append(etcdConfs, ec)
		}
		viper.Set(defaults.ConfigETCDKey, etcdConfs)
	}

	viper.SetDefault(defaults.ConfigETCDKey, []eutil.EViperConfig{DefaultEtcdConfig})

	// format rqlite stuff in yaml file
	if (RootCmd.PersistentFlags().Changed("rqlite-ip") ||
		RootCmd.PersistentFlags().Changed("rqlite-hostname") ||
		RootCmd.PersistentFlags().Changed("rqlite-port")) &&
		len(rqliteIPs) == len(rqlitePorts) &&
		len(rqlitePorts) == len(rqliteHostnames) {
		rqliteConfs := []*rutil.RViperConfig{}
		for i := 0; i < len(rqliteIPs); i++ {
			rc := &rutil.RViperConfig{
				IP:       rqliteIPs[i].String(),
				Hostname: rqliteHostnames[i],
				Port:     rqlitePorts[i],
			}
			rqliteConfs = append(rqliteConfs, rc)
		}
		viper.Set(defaults.ConfigRqliteKey, rqliteConfs)
	}

	viper.SetDefault(defaults.ConfigRqliteKey, []rutil.RViperConfig{DefaultRqliteConfig})

	viper.SetDefault(defaults.ConfigNodesKey, DefaultNodesConfig.Nodes)

	viper.SetDefault(defaults.ConfigNodeAllocationsValidationNodesKey, DefaultNodeAllocationsValidationNodes)
	viper.SetDefault(defaults.ConfigNodeAllocationsValidationMemoryKey, DefaultNodeAllocationsValidationMemory)
	viper.SetDefault(defaults.ConfigNodeAllocationsSetupNodesKey, DefaultNodeAllocationsSetupNodes)
	viper.SetDefault(defaults.ConfigNodeAllocationsSetupMemoryKey, DefaultNodeAllocationsSetupMemory)
	viper.SetDefault(defaults.ConfigNodeAllocationsTransferNodesKey, DefaultNodeAllocationsTransferNodes)
	viper.SetDefault(defaults.ConfigNodeAllocationsTransferMemoryKey, DefaultNodeAllocationsTransferMemory)
	viper.SetDefault(defaults.ConfigNodeAllocationsTeardownNodesKey, DefaultNodeAllocationsTeardownNodes)
	viper.SetDefault(defaults.ConfigNodeAllocationsTeardownMemoryKey, DefaultNodeAllocationsTeardownMemory)

	viper.SetDefault(defaults.ConfigLDAPHostKey, DefaultLDAPHost)
	viper.SetDefault(defaults.ConfigLDAPPortKey, DefaultLDAPPort)
	viper.SetDefault(defaults.ConfigLDAPBaseDNKey, DefaultLDAPBaseDN)
	viper.SetDefault(defaults.ConfigLDAPKrb5AttributesKey, DefaultLDAPKrb5Attributes)
	viper.SetDefault(defaults.ConfigLDAPUnameAttributesKey, DefaultLDAPUnameAttributes)
	viper.SetDefault(defaults.ConfigLDAPUIDNumber5AttributesKey, DefaultLDAPUIDNumberAttributes)

	viper.SetDefault(defaults.ConfigTestKey, false)

	viper.SetDefault(defaults.ConfigExpiryAdvanceKey, DefaultExpiryAdvance)
	viper.SetDefault(defaults.ConfigMaxSourceBytesKey, DefaultMaxSourceBytes)

	viper.SetDefault(defaults.ConfigErrantExpiration, DefaultErrantExpiration)
	viper.SetDefault(defaults.ConfigRequestedCertLifetime, DefaultRequestedCertLifetime)

	viper.SetDefault(defaults.ConfigConcurrentSchedulersKey, DefaultConcurrentSchedulers)
	viper.SetDefault(defaults.ConfigConcurrentTransferWorkersKey, DefaultConcurrentWorkers)
	viper.SetDefault(defaults.ConfigConcurrentWatchdogsKey, DefaultConcurrentWatchDogs)

	err := viper.SafeWriteConfig()
	if err != nil {
		logrus.Warnf("failed to write default config: %v", err)
	} else {
		logrus.Infof("wrote default config to: %v", finalConfigPath)
	}
}
