// Copyright 2026. Triad National Security, LLC. All rights reserved.

package pftool

import (
	"github.com/google/uuid"
	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/internal/fta/plugin"
	"github.com/lanl/conduit/internal/logger"
	"google.golang.org/protobuf/types/known/anypb"
)

const (
	DefaultPftoolTimeoutHours = 1
	PftoolPluginKey           = "pftool"
	DefaultPFCPLocation       = "/etc/pftool/bin/pfcp"
)

var _ plugin.ConduitFTAPlugin = (*PftoolPlugin)(nil)

type ViperPftoolPluginConfig struct {
	PfcpPath     string  `mapstructure:"pfcp-path" yaml:"pfcp-path"`
	TimeoutHours float64 `mapstructure:"no-progress-timeout-hours" yaml:"no-progress-timeout-hours"`
}

type PftoolPlugin struct {
	log        *logger.ConduitLogger
	transferID uuid.UUID
}

func (p *PftoolPlugin) Initialize(transferID uuid.UUID, log *logger.ConduitLogger) []plugin.PluginCapability {
	p.log = log
	p.transferID = transferID

	return []plugin.PluginCapability{
		plugin.TRANSFER,
	}
}

func (p *PftoolPlugin) GetResolvedPath(userPath string, pathType proto.LeaseType, fsc *plugin.FileSystemConfig) (resolvedFTAPath string, foundSymlink string, _ *plugin.FTAPathError) {
	return "", "", nil
}

func (p *PftoolPlugin) ValidateDestination(sourceBases []string, userDestination string, ftaDestination string, fsConfig *plugin.FileSystemConfig) (pluginErrors plugin.PluginErrors, userDestinations []string, resolvedFTADestinations []string, destInfo proto.DestInfo, pluginPathData map[string]*string) {
	return plugin.PluginErrors{}, []string{}, []string{}, proto.DestInfo_DEST_NONE, make(map[string]*string)
}

// no op
func (p *PftoolPlugin) ValidateSource(pluginPathInfo *plugin.PluginPathInfo, action string, options map[string]*anypb.Any) (pluginErrors plugin.PluginErrors, pluginPathData *string, omit bool) {
	return plugin.PluginErrors{}, nil, false
}

// no op
func (p *PftoolPlugin) Setup(transferID uuid.UUID, pathInfo *plugin.PluginPathInfo, pathType proto.LeaseType, action string, options map[string]*anypb.Any, baseDest bool, updateTransferProgress plugin.UpdateTransferProgress) (plugin.PluginErrors, *plugin.PluginPathInfo) {
	return plugin.PluginErrors{}, nil
}

// no op
func (p *PftoolPlugin) Teardown(transferID uuid.UUID, transferDetails *proto.TransferDetails, pathInfo *plugin.PluginPathInfo, pathType proto.LeaseType, action string, options map[string]*anypb.Any, baseDest bool, updateTransferProgress plugin.UpdateTransferProgress) (_ plugin.PluginErrors) {
	return plugin.PluginErrors{}
}

func (p *PftoolPlugin) GetDefaultConfig() any {
	return ViperPftoolPluginConfig{
		PfcpPath:     DefaultPFCPLocation,
		TimeoutHours: DefaultPftoolTimeoutHours,
	}
}
