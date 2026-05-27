// Copyright 2026. Triad National Security, LLC. All rights reserved.

package fta

import (
	"github.com/lanl/conduit/internal/fta/plugin"
	"github.com/lanl/conduit/internal/fta/plugins/marchive"
	"github.com/lanl/conduit/internal/fta/plugins/pftool"
	"github.com/lanl/conduit/internal/fta/plugins/posix"
	"github.com/lanl/conduit/internal/fta/plugins/rsync"
)

var PluginMap = map[string]plugin.ConduitFTAPlugin{
	// "staging": &staging.StagingPlugin{},
	rsync.RsyncPluginKey:       &rsync.RsyncPlugin{},
	posix.PosixPluginKey:       &posix.PosixPlugin{},
	pftool.PftoolPluginKey:     &pftool.PftoolPlugin{},
	marchive.MarchivePluginKey: &marchive.MarchivePlugin{},
}
