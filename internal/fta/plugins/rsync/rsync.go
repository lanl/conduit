// Copyright 2026. Triad National Security, LLC. All rights reserved.

package rsync

import (
	"github.com/google/uuid"
	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/internal/fta/plugin"
	"github.com/lanl/conduit/internal/logger"
)

const (
	RsyncPluginKey       = "rsync"
	DefaultRsyncLocation = "rsync"
)

var _ plugin.ConduitFTAPlugin = (*RsyncPlugin)(nil)

type ViperRsyncPluginConfig struct {
	RsyncPath string `mapstructure:"rsync-path" yaml:"rsync-path"`
}

type RsyncPlugin struct {
	log        *logger.ConduitLogger
	transferID uuid.UUID
}

func (p *RsyncPlugin) Initialize(transferID uuid.UUID, log *logger.ConduitLogger) []plugin.PluginCapability {
	p.log = log
	p.transferID = transferID

	return []plugin.PluginCapability{
		plugin.TRANSFER,
	}
}

func (p *RsyncPlugin) GetResolvedPath(userPath string, pathType proto.LeaseType, fsc *plugin.FileSystemConfig) (resolvedFTAPath string, foundSymlink string, _ *plugin.FTAPathError) {
	return "", "", nil
}

func (p *RsyncPlugin) ValidateDestination(sourceBases []string, userDestination string, ftaDestination string, fsConfig *plugin.FileSystemConfig) (pluginErrors plugin.PluginErrors, userDestinations []string, resolvedFTADestinations []string, destInfo proto.DestInfo, pluginPathData map[string]*string) {
	return plugin.PluginErrors{}, []string{}, []string{}, proto.DestInfo_DEST_NONE, make(map[string]*string)
}

// no op
func (p *RsyncPlugin) ValidateSource(pluginPathInfo *plugin.PluginPathInfo, action proto.Action) (pluginErrors plugin.PluginErrors, pluginPathData *string) {
	return plugin.PluginErrors{}, nil
}

// no op
func (p *RsyncPlugin) Setup(transferID uuid.UUID, pathInfo *plugin.PluginPathInfo, pathType proto.LeaseType, action proto.Action, baseDest bool, updateTransferProgress plugin.UpdateTransferProgress) (plugin.PluginErrors, *plugin.PluginPathInfo) {
	return plugin.PluginErrors{}, nil
}

// no op
func (p *RsyncPlugin) Teardown(transferID uuid.UUID, transferDetails *proto.TransferDetails, pathInfo *plugin.PluginPathInfo, pathType proto.LeaseType, action proto.Action, baseDest bool, updateTransferProgress plugin.UpdateTransferProgress) (_ plugin.PluginErrors) {
	return plugin.PluginErrors{}
}

func (p *RsyncPlugin) GetDefaultConfig() any {
	return ViperRsyncPluginConfig{
		RsyncPath: DefaultRsyncLocation,
	}
}
