// Copyright 2026. Triad National Security, LLC. All rights reserved.

package scoutam

import (
	"github.com/google/uuid"
	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/internal/fta/plugin"
	"github.com/lanl/conduit/internal/logger"
	"google.golang.org/protobuf/types/known/anypb"
)

const (
	ScoutAMPluginKey     = "scoutam"
	DefaultScoutAMStager = "samnfs"
)

var _ plugin.ConduitFTAPlugin = (*ScoutAMPlugin)(nil)

type ViperScoutAMPluginConfig struct {
	StagerPath string `mapstructure:"stager-path" yaml:"stager-path"`
}

type ScoutAMPlugin struct {
	log        *logger.ConduitLogger
	transferID uuid.UUID
}

func (p *ScoutAMPlugin) Initialize(transferID uuid.UUID, log *logger.ConduitLogger) []plugin.PluginCapability {
	p.log = log
	p.transferID = transferID

	return []plugin.PluginCapability{
		plugin.SETUP,
	}
}

// no op
func (p *ScoutAMPlugin) GetResolvedPath(userPath string, pathType proto.LeaseType, fsc *plugin.FileSystemConfig) (resolvedFTAPath string, foundSymlink string, _ *plugin.FTAPathError) {
	return "", "", nil
}

// no op
func (p *ScoutAMPlugin) ValidateSource(pluginPathInfo *plugin.PluginPathInfo, action string, options map[string]*anypb.Any) (pluginErrors plugin.PluginErrors, pluginPathData *string, omit bool) {
	return plugin.PluginErrors{}, nil, false
}

// no op
func (p *ScoutAMPlugin) ValidateDestination(sourceBases []string, userDestination string, ftaDestination string, fsConfig *plugin.FileSystemConfig) (pluginErrors plugin.PluginErrors, userDestinations []string, resolvedFTADestinations []string, destInfo proto.DestInfo, pluginPathData map[string]*string) {
	return plugin.PluginErrors{}, []string{}, []string{}, proto.DestInfo_DEST_NONE, make(map[string]*string)
}

// no op
func (p *ScoutAMPlugin) Transfer(transferID uuid.UUID, pluginData *plugin.PluginData, destInfo proto.DestInfo, action string, options map[string]*anypb.Any, updateTransferProgress plugin.UpdateTransferProgress, updateAction plugin.UpdateAction) plugin.PluginErrors {
	return plugin.PluginErrors{}
}

// no op
func (p *ScoutAMPlugin) Teardown(transferID uuid.UUID, transferDetails *proto.TransferDetails, pathInfo *plugin.PluginPathInfo, pathType proto.LeaseType, action string, options map[string]*anypb.Any, baseDest bool, updateTransferProgress plugin.UpdateTransferProgress) (_ plugin.PluginErrors) {
	return plugin.PluginErrors{}
}

func (p *ScoutAMPlugin) GetDefaultConfig() any {
	return DefaultScoutAMPluginConfig()
}

func DefaultScoutAMPluginConfig() ViperScoutAMPluginConfig {
	return ViperScoutAMPluginConfig{
		StagerPath: DefaultScoutAMStager,
	}
}
