// Copyright 2026. Triad National Security, LLC. All rights reserved.

package defaults

import (
	"fmt"
	"time"
)

// VIPER DEFINITIONS
const (
	// server config keys
	ConfigServerIPKey                        = "server.ip"
	ConfigServerPortKey                      = "server.port"
	ConfigServerWSPortKey                    = "server.ws-port"
	ConfigServerHostnameKey                  = "server.hostname"
	ConfigAuthKeytabKey                      = "auth.keytab"
	ConfigInternalCACertKey                  = "auth.internal-ca-cert"
	ConfigInternalCAKeyKey                   = "auth.internal-ca-key"
	ConfigExternalCACertKey                  = "auth.external-ca-cert"
	ConfigExternalCAKeyKey                   = "auth.external-ca-key"
	ConfigCertOrganizationKey                = "auth.cert.organization"
	ConfigCertCountryKey                     = "auth.cert.country"
	ConfigCertProvinceKey                    = "auth.cert.province"
	ConfigCertLocalityKey                    = "auth.cert.locality"
	ConfigCertPostalCodeKey                  = "auth.cert.postal-code"
	ConfigFTAPathKey                         = "fta.path"
	ConfigFTAOptionsKey                      = "fta.options"
	ConfigFTAEnvKey                          = "fta.environment"
	ConfigETCDKey                            = "etcd"
	ConfigRqliteKey                          = "rqlite"
	ConfigNodeAllocationsValidationNodesKey  = "node-allocations.validation.nodes"
	ConfigNodeAllocationsValidationMemoryKey = "node-allocations.validation.memory"
	ConfigNodeAllocationsSetupNodesKey       = "node-allocations.setup.nodes"
	ConfigNodeAllocationsSetupMemoryKey      = "node-allocations.setup.memory"
	ConfigNodeAllocationsTransferNodesKey    = "node-allocations.transfer.nodes"
	ConfigNodeAllocationsTransferMemoryKey   = "node-allocations.transfer.memory"
	ConfigNodeAllocationsTeardownNodesKey    = "node-allocations.teardown.nodes"
	ConfigNodeAllocationsTeardownMemoryKey   = "node-allocations.teardown.memory"
	ConfigTestKey                            = "test"
	ConfigExpiryAdvanceKey                   = "transfer.expiry-advance"
	ConfigMaxSourceBytesKey                  = "transfer.max-source-bytes"
	ConfigLDAPHostKey                        = "ldap.host"
	ConfigLDAPPortKey                        = "ldap.port"
	ConfigLDAPBaseDNKey                      = "ldap.base-dn"
	ConfigLDAPKrb5AttributesKey              = "ldap.krb5-attributes"
	ConfigLDAPUnameAttributesKey             = "ldap.uname-attributes"
	ConfigLDAPUIDNumber5AttributesKey        = "ldap.uid-number-attributes"
	ConfigErrantExpiration                   = "errant-lock"
	ConfigRequestedCertLifetime              = "auth.requested-cert-lifetime"
	ConfigNodesKey                           = "nodes"
	ConfigConcurrentSchedulersKey            = "server.concurrency.schedulers"
	ConfigConcurrentWatchdogsKey             = "server.concurrency.watchdogs"
	ConfigConcurrentTransferWorkersKey       = "server.concurrency.transfer-workers"

	// FTA config keys
	ConfigExpiryIntervalKey         = "transfer.expiry-interval"
	ConfigFTAVerifyRetryCountKey    = "fta.verify-retry-count"
	ConfigFTAVerifySleepDurationKey = "fta.verify-sleep-duration"
	ConfigFilesystemsKey            = "filesystems"
	ConfigPluginsKey                = "plugins"

	// CLI config keys
	ConfigKrbConfigKey      = "krb.config"
	ConfigKrbCacheKey       = "krb.cache"
	ConfigKrbCachePrefixKey = "krb.cache-prefix"
	ConfigKrbSpnKey         = "krb.spn"
	ConfigKrbKinitPathKey   = "krb.kinit-path"

	ConfigConduitIPKey      = "conduit.ip"
	ConfigConduitPortKey    = "conduit.port"
	ConfigConduitTimeoutKey = "conduit.request-timeout"
	ConfigConduitCAKey      = "conduit.ca"

	ConfigClientGrpcLimitKey = "client.grpc-limit"
	ConfigClientCertKey      = "client.cert"
	ConfigClientKeyKey       = "client.key"
)

const (
	MaxRetries                    = 5
	RetryDelay                    = 5 * time.Second
	DefaultLDAPTimeout            = 30 * time.Second
	DefaultETCDTimeout            = 30 * time.Second
	DefaultRqliteTimeout          = 30 * time.Second
	DefaultRunnerTimeout          = 10 * time.Second
	DefaultRunnerMemMonitorDelay  = 10 * time.Second
	DefaultSchedulerSubmitTimeout = DefaultETCDTimeout

	DefaultCertOrganization = "Los Alamos National Laboratory"
	DefaultCertCountry      = "US"
	DefaultCertProvince     = "NM"
	DefaultCertLocality     = "Los Alamos"
	DefaultCertPostalCode   = "87545"
)

// CLI default values
const (
	DefaultKrb5Config        = "/etc/krb5.conf"
	DefaultKrb5Cache         = "/tmp"
	DefaultKrb5CachePrefix   = "krb5cc_"
	DefaultKDCHost           = "" // "kdc.example.com"
	DefaultKDCPort           = 0  // 88
	DefaultSPN               = "conduit/conduit-server.example.com"
	DefaultReqTimeout        = "60s"
	DefaultReqTimeoutSeconds = 60
	DefaultConduitHost       = "conduit-server.example.com"
	DefaultConduitPort       = 23456
	DefaultConduitCA         = ""
	DefaultClientGRPCLimit   = 100000000
	DefaultKinitPath         = ""
	DefaultClientCert        = ""
	DefaultClientKey         = ""
	DefaultBundleName        = ".conduit-cert-key-bundle.pem"
)

var (
	DefaultBundlePath = fmt.Sprintf("~/%s", DefaultBundleName)
)
