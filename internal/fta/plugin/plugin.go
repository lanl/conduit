// Copyright 2026. Triad National Security, LLC. All rights reserved.

package plugin

import (
	"github.com/google/uuid"
	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/internal/logger"
)

type PluginData struct {
	SourcePluginInfo       map[string]*PluginPathInfo `json:"sourcePluginInfo"`       // key: original user path
	DestinationsPluginInfo map[string]*PluginPathInfo `json:"destinationsPluginInfo"` // key: resolved user path
	DestinationPluginInfo  *PluginPathInfo            `json:"destinationPluginInfo"`  // PluginInfo for the overall destination
	PluginPathData         map[string]*string         `json:"pluginPathData"`         // key: original user path, value: json encoded data for path
}

type PluginPathInfo struct {
	OriginalUserPath string            `json:"originalUserPath"` // this is the path that was provided from the user
	ResolvedUserPath string            `json:"resolvedUserPath"` // this is the original user path after symlinks have been resolved
	ResolvedFTAPath  string            `json:"resolvedFTAPath"`  // this is the path on the fta after symlinks have been resolved
	TransferPath     string            `json:"transferPath"`     // the path that will be used in the transfer stage. This should be an fta path and be set by the "setup" stage
	Plugin           ConduitFTAPlugin  `json:"-"`
	FSC              *FileSystemConfig `json:"filesystemConfig"` // this is the correlating filesystem config for this path
}

type PluginErrors struct {
	Errors   []*FTAPathError
	Warnings []*FTAPathError
}

// UpdateTransferProgress will update the status details in etcd. WARNING: setup and teardown are run in parallel for each source and destination so this can get overwritten very easily!
type UpdateTransferProgress func(proto.ETCDStatusDetails) error

// UpdateAction will update a transfers Action in etcd. This is useful if a plugin needs to change a move to a copy for some reason
type UpdateAction func(currentAction proto.Action, newAction proto.Action) error

type PluginCapability int

const (
	VALIDATION PluginCapability = iota
	SETUP
	TRANSFER
	TEARDOWN
)

// ConduitFTAPlugin is the interface that we're exposing as a plugin.
type ConduitFTAPlugin interface {
	// Initialize will provide a logger and transferID to the plugin and the plugin should return its capabilities
	Initialize(transferID uuid.UUID, log *logger.ConduitLogger) []PluginCapability
	// ValidateSource will execute any validation the plugin needs to do per source. The plugin should return all pluginPathData which should include leases for that source
	ValidateSource(pathInfo *PluginPathInfo, action proto.Action) (pluginErrors PluginErrors, pluginPathData *string)
	// GetResolvedPath returns a fully resolved ftaPath from a userPath. This includes following symlinks. Return any symlinks that point to a different filesystem to have conduit retry with that filesystems plugin
	GetResolvedPath(userPath string, pathType proto.LeaseType, fsc *FileSystemConfig) (resolvedFTAPath string, foundSymlink string, pathError *FTAPathError)
	// ValidateDestination will execute any validation the plugin needs to do for the destination. The plugin should return userPathDestinations, ftaDestinations, destInfo, and pluginPathData
	ValidateDestination(sourceBases []string, userDestination string, ftaDestination string, fsConfig *FileSystemConfig) (pluginErrors PluginErrors, userDestinations []string, resolvedFTADestinations []string, destInfo proto.DestInfo, pluginPathData map[string]*string)
	// Setup runs before the Transfer. newPathInfo.TransferPath must be set for the transfer plugin to know what path to use
	Setup(transferID uuid.UUID, pathInfo *PluginPathInfo, pathType proto.LeaseType, action proto.Action, baseDest bool, updateTransferProgress UpdateTransferProgress) (pluginErrors PluginErrors, newPathInfo *PluginPathInfo)
	// Transfer should be when the plugin actually moves data
	Transfer(transferID uuid.UUID, pluginData *PluginData, destInfo proto.DestInfo, action proto.Action, updateTransferProgress UpdateTransferProgress, updateAction UpdateAction) (pluginErrors PluginErrors)
	// Teardown runs after Transfer
	Teardown(transferID uuid.UUID, transferDetails *proto.TransferDetails, pathInfo *PluginPathInfo, pathType proto.LeaseType, action proto.Action, baseDest bool, updateTransferProgress UpdateTransferProgress) (pluginErrors PluginErrors)
	// returns a default config for the plugin to be used in the conduit fta yaml config
	GetDefaultConfig() any
}
