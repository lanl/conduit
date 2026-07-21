// Copyright 2026. Triad National Security, LLC. All rights reserved.

package posix

import (
	"github.com/google/uuid"
	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/internal/fta/plugin"
	"github.com/lanl/conduit/internal/logger"
	"google.golang.org/protobuf/types/known/anypb"
)

const (
	CustomPluginConfigTrashKey = "posix-src-trash"
	PosixPluginKey             = "posix"
)

var _ plugin.ConduitFTAPlugin = (*PosixPlugin)(nil)

type PosixPlugin struct {
	log        *logger.ConduitLogger
	transferID uuid.UUID
}

func (p *PosixPlugin) Initialize(transferID uuid.UUID, log *logger.ConduitLogger) []plugin.PluginCapability {
	p.log = log
	p.transferID = transferID

	return []plugin.PluginCapability{
		plugin.VALIDATION,
		plugin.SETUP,
		plugin.TEARDOWN,
	}
}

// no op
func (p *PosixPlugin) Transfer(transferID uuid.UUID, pluginData *plugin.PluginData, destInfo proto.DestInfo, action string, options map[string]*anypb.Any, updateTransferProgress plugin.UpdateTransferProgress, updateAction plugin.UpdateAction) plugin.PluginErrors {
	return plugin.PluginErrors{}
}

// no config
func (p *PosixPlugin) GetDefaultConfig() any {
	return nil
}
