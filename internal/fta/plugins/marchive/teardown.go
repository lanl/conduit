// Copyright 2026. Triad National Security, LLC. All rights reserved.

package marchive

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/google/uuid"
	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/internal/fta/plugin"
	"google.golang.org/protobuf/types/known/anypb"
)

func (p *MarchivePlugin) Teardown(transferID uuid.UUID, transferDetails *proto.TransferDetails, pathInfo *plugin.PluginPathInfo, pathType proto.LeaseType, action string, options map[string]*anypb.Any, baseDest bool, updateTransferProgress plugin.UpdateTransferProgress) *proto.FTAPluginErrors {
	marchiveConfig := &ViperMarchivePluginConfig{}
	err := plugin.GetPluginConfigsFromViper(MarchivePluginKey, marchiveConfig)
	if err != nil {
		return &proto.FTAPluginErrors{
			Errors: []*proto.FTAPathError{
				{
					LeasePath:  "",
					PErr:       proto.Error_ERROR_INVALID_CONDUIT_CONFIG,
					ErrMessage: fmt.Sprintf("failed to get marchive config: %v", err),
				},
			},
		}
	}
	scriptRelPath := marchiveConfig.TmrequestPath // Ensure this script exists and is executable

	// If the file path is not a SOURCE for the transfer -> no need to
	// clean up the Tape Manager tree
	if pathType != proto.LeaseType_SOURCE {
		return &proto.FTAPluginErrors{}
	}

	// Arguments for the Tape Request Generator
	args := []string{"-j", transferID.String(), "-X", "read"}

	cmd := exec.Command(scriptRelPath, args...)
	cmd.Env = os.Environ() // Make sure command is using existing environment

	updateTransferProgress(&proto.ETCDStatusDetails{
		PluginStatus: fmt.Sprintf("(teardown) tape request cleanup for %v started", pathInfo.OriginalUserPath),
	})
	output, err := cmd.CombinedOutput()
	if err != nil {
		p.log.Errorf("Error running script: %v\nOutput: %s", err, output)
		return &proto.FTAPluginErrors{
			Errors: []*proto.FTAPathError{
				{
					LeasePath:  "",
					PErr:       proto.Error_ERROR_REMOVE_FAILED,
					ErrMessage: fmt.Sprintf("errors removing Tape Manager request files: %v", err),
				},
			},
		}
	}

	updateTransferProgress(&proto.ETCDStatusDetails{
		PluginStatus: fmt.Sprintf("(teardown) tape request cleanup for %v complete", pathInfo.OriginalUserPath),
	})
	return &proto.FTAPluginErrors{}
}
