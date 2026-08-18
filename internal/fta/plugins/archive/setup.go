// Copyright 2026. Triad National Security, LLC. All rights reserved.

package archive

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/google/uuid"
	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/internal/fta/plugin"
	"google.golang.org/protobuf/types/known/anypb"
)

// Setup for archive - used only when reading data from a Versity/ScoutFSfilesystem. Calls the
// archive-stager utility/script to stage all requested files onto the Archive's disk cache. This
// call waits/hangs until this script completes, which occurs once all requested files are staged
// to cache.
func (p *ArchivePlugin) Setup(transferID uuid.UUID, pathInfo *plugin.PluginPathInfo, pathType proto.LeaseType, action string, options map[string]*anypb.Any, baseDest bool, updateTransferProgress plugin.UpdateTransferProgress) (plugin.PluginErrors, *plugin.PluginPathInfo) {
	archiveConfig := &ViperArchivePluginConfig{}
	err := plugin.GetPluginConfigsFromViper(ArchivePluginKey, archiveConfig)
	if err != nil {
		return plugin.PluginErrors{
			Errors: []*plugin.FTAPathError{
				{
					LeasePath:  "",
					PErr:       proto.Error_ERROR_INVALID_CONDUIT_CONFIG,
					ErrMessage: fmt.Errorf("failed to get archive config: %v", err),
				},
			},
		}, nil
	}
	scriptRelPath := archiveConfig.StagerPath    // Ensure this script exists and is executable

	// pathInfo.TransferPath tells the transfer plugin what final path to use for its transfer
	pathInfo.TransferPath = pathInfo.ResolvedFTAPath

	// If the file path is not a SOURCE for the transfer -> no need to get the
	// file from tape
	if pathType != proto.LeaseType_SOURCE {
		return plugin.PluginErrors{}, pathInfo
	}

	// Arguments for the Archive Stager
	args := []string{"stage", pathInfo.OriginalUserPath}
	p.log.Debugf("Archive Stager Command Generator: %v %v", scriptRelPath, args)

	cmd := exec.Command(scriptRelPath, args...)
	cmd.Env = os.Environ() // Make sure command is using existing environment

	updateTransferProgress(proto.ETCDStatusDetails{
		PluginStatus: fmt.Sprintf("(setup) data staging for %v started", pathInfo.OriginalUserPath),
	})
	output, err := cmd.CombinedOutput()
	if err != nil {
		p.log.Errorf("Error running script %s: %v\nOutput: %v", scriptRelPath, err, output)
		return plugin.PluginErrors{
			Errors: []*plugin.FTAPathError{
				{
					LeasePath:  "",
					PErr:       proto.Error_ERROR_STAT_FAILED,
					ErrMessage: fmt.Errorf("Archive Stager non zero exit code: %v", err),
				},
			},
		}, nil
	}

	updateTransferProgress(proto.ETCDStatusDetails{
		PluginStatus: fmt.Sprintf("(setup) data staging for %v complete", pathInfo.OriginalUserPath),
	})

	return plugin.PluginErrors{}, pathInfo
}
