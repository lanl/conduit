// Copyright 2026. Triad National Security, LLC. All rights reserved.

package posix

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/internal/fta/actions"
	"github.com/lanl/conduit/internal/fta/plugin"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func (p *PosixPlugin) Setup(transferID uuid.UUID, pathInfo *plugin.PluginPathInfo, pathType proto.LeaseType, action string, options map[string]*anypb.Any, baseDest bool, updateTransferProgress plugin.UpdateTransferProgress) (*proto.FTAPluginErrors, *plugin.PluginPathInfo) {

	// pathInfo.TransferPath tells the transfer plugin what final path to use for its transfer
	pathInfo.TransferPath = pathInfo.ResolvedFTAPath

	// check for --mkdir-parents option only for destination
	if pathType == proto.LeaseType_DESTINATION {
		if opt, ok := options[actions.ParentsFlag]; ok {
			var parents wrapperspb.BoolValue
			if err := opt.UnmarshalTo(&parents); err != nil {
				return &proto.FTAPluginErrors{
					Errors: []*proto.FTAPathError{{
						LeasePath:  pathInfo.OriginalUserPath,
						PErr:       proto.Error_ERROR_INVALID_INPUT,
						ErrMessage: fmt.Sprintf("failed to unmarshal parents flag: %v", err),
					}},
				}, pathInfo
			}

			if parents.GetValue() {
				parentPath := filepath.Dir(pathInfo.ResolvedFTAPath)
				if err := os.MkdirAll(parentPath, 0700); err != nil {
					return &proto.FTAPluginErrors{
						Errors: []*proto.FTAPathError{{
							LeasePath:  pathInfo.OriginalUserPath,
							PErr:       proto.Error_ERROR_PERMISSIONS,
							ErrMessage: fmt.Sprintf("failed to create destination parent directories[%s]: %v", parentPath, err),
						}},
					}, pathInfo
				}
			}
		}
	}

	updateTransferProgress(&proto.ETCDStatusDetails{
		PluginStatus: fmt.Sprintf("setup %v complete", pathInfo.OriginalUserPath),
	})

	return &proto.FTAPluginErrors{}, pathInfo
}
