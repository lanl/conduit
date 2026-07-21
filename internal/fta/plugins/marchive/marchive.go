// Copyright 2026. Triad National Security, LLC. All rights reserved.

package marchive

import (
	"github.com/google/uuid"
	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/internal/fta/plugin"
	"github.com/lanl/conduit/internal/logger"
	"google.golang.org/protobuf/types/known/anypb"
)

const (
	MarchivePluginKey         = "marchive"
	DefaultMarchiveTMRequest  = "marchive-tmrequest"
	DefaultMarchiveObjectList = "mustang"
)

var _ plugin.ConduitFTAPlugin = (*MarchivePlugin)(nil)

type ViperMarchivePluginConfig struct {
	ObjlistPath   string `mapstructure:"objlist-path" yaml:"objlist-path"`
	TmrequestPath string `mapstructure:"tmrequest-path" yaml:"tmrequest-path"`
}

type MarchivePlugin struct {
	log        *logger.ConduitLogger
	transferID uuid.UUID
}

func (p *MarchivePlugin) Initialize(transferID uuid.UUID, log *logger.ConduitLogger) []plugin.PluginCapability {
	p.log = log
	p.transferID = transferID

	return []plugin.PluginCapability{
		plugin.SETUP,
		plugin.TEARDOWN,
	}
}

// no op
func (p *MarchivePlugin) GetResolvedPath(userPath string, pathType proto.LeaseType, fsc *plugin.FileSystemConfig) (resolvedFTAPath string, foundSymlink string, _ *plugin.FTAPathError) {
	return "", "", nil
}

// no op
func (p *MarchivePlugin) ValidateSource(pluginPathInfo *plugin.PluginPathInfo, action string, options map[string]*anypb.Any) (pluginErrors plugin.PluginErrors, pluginPathData *string, omit bool) {
	return plugin.PluginErrors{}, nil, false
}

// no op
func (p *MarchivePlugin) ValidateDestination(sourceBases []string, userDestination string, ftaDestination string, fsConfig *plugin.FileSystemConfig) (pluginErrors plugin.PluginErrors, userDestinations []string, resolvedFTADestinations []string, destInfo proto.DestInfo, pluginPathData map[string]*string) {
	return plugin.PluginErrors{}, []string{}, []string{}, proto.DestInfo_DEST_NONE, make(map[string]*string)
}

// no op
func (p *MarchivePlugin) Transfer(transferID uuid.UUID, pluginData *plugin.PluginData, destInfo proto.DestInfo, action string, options map[string]*anypb.Any, updateTransferProgress plugin.UpdateTransferProgress, updateAction plugin.UpdateAction) plugin.PluginErrors {
	return plugin.PluginErrors{}
}

func (p *MarchivePlugin) GetDefaultConfig() any {
	return ViperMarchivePluginConfig{
		ObjlistPath:   DefaultMarchiveObjectList,
		TmrequestPath: DefaultMarchiveTMRequest,
	}
}
