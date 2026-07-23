// Copyright 2026. Triad National Security, LLC. All rights reserved.

package posix

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"github.com/google/uuid"
	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/internal/fta/actions"
	"github.com/lanl/conduit/internal/fta/plugin"
	"google.golang.org/protobuf/types/known/anypb"
)

func (p *PosixPlugin) Teardown(transferID uuid.UUID, transferDetails *proto.TransferDetails, pathInfo *plugin.PluginPathInfo, pathType proto.LeaseType, action string, options map[string]*anypb.Any, baseDest bool, updateTransferProgress plugin.UpdateTransferProgress) plugin.PluginErrors {
	if action == actions.Action_MOVE {
		// delete source data if this is a move
		if pathType == proto.LeaseType_SOURCE {
			trashPath, pErr, err := getTrashPathFromConfig(pathInfo.FSC, pathInfo.ResolvedUserPath, transferID)
			if err != nil {
				return plugin.PluginErrors{
					Errors: []*plugin.FTAPathError{
						{
							LeasePath:  pathInfo.ResolvedFTAPath,
							PErr:       pErr,
							ErrMessage: fmt.Errorf("failed to get trash path: %v", err),
						},
					},
				}
			}

			if trashPath != "" {
				// create the trash area
				err = os.MkdirAll(filepath.Dir(trashPath), os.FileMode(0700))
				if err != nil {
					p.log.Errorf("failed to make trash path[%v]: %v", filepath.Dir(trashPath), err)
				}

				p.log.Warnf("moving %v to %v", pathInfo.ResolvedFTAPath, trashPath)

				updateTransferProgress(proto.ETCDStatusDetails{
					PluginStatus: fmt.Sprintf("moving %v to %v", pathInfo.ResolvedFTAPath, trashPath),
				})

				errors := []*plugin.FTAPathError{}

				// move the source files in the staging area to the trash
				err = os.Rename(pathInfo.ResolvedFTAPath, trashPath)
				if err != nil {
					errors = append(errors, &plugin.FTAPathError{
						LeasePath:  pathInfo.ResolvedFTAPath,
						PErr:       proto.Error_ERROR_RENAME_FAILED,
						ErrMessage: fmt.Errorf("failed to rename source to trash %v -> %v: %v", pathInfo.ResolvedFTAPath, trashPath, err),
					})
				}

				mTime := transferDetails.GetStartTime().AsTime()
				if !mTime.IsZero() {
					var aTime time.Time
					err = os.Chtimes(filepath.Dir(trashPath), aTime, mTime)
					if err != nil {
						errors = append(errors, &plugin.FTAPathError{
							LeasePath:  pathInfo.ResolvedFTAPath,
							PErr:       proto.Error_ERROR_CHTIME_FAILED,
							ErrMessage: fmt.Errorf("failed to change modified time of trash dir %v: %v", filepath.Dir(trashPath), err),
						})
					}
				}

				if len(errors) > 0 {
					return plugin.PluginErrors{
						Errors: errors,
					}
				}

			} else {
				// remove the source files in the staging area
				p.log.Warnf("removing: %v", pathInfo.ResolvedFTAPath)

				updateTransferProgress(proto.ETCDStatusDetails{
					PluginStatus: fmt.Sprintf("removing %v", pathInfo.ResolvedFTAPath),
				})

				err := os.RemoveAll(pathInfo.ResolvedFTAPath)
				if err != nil {
					return plugin.PluginErrors{
						Errors: []*plugin.FTAPathError{
							{
								LeasePath:  pathInfo.ResolvedFTAPath,
								PErr:       proto.Error_ERROR_REMOVE_FAILED,
								ErrMessage: fmt.Errorf("failed to remove source files %v: %v", pathInfo.ResolvedFTAPath, err),
							},
						},
					}
				}
			}
		}
	}

	updateTransferProgress(proto.ETCDStatusDetails{
		PluginStatus: fmt.Sprintf("teardown %v complete", pathInfo.OriginalUserPath),
	})

	return plugin.PluginErrors{}
}

func getTrashPathFromConfig(fsc *plugin.FileSystemConfig, cleanUserPath string, transferID uuid.UUID) (string, proto.Error, error) {
	trashSub := ""
	trashSubAny, ok := fsc.CustomPluginFSConfig[CustomPluginConfigTrashKey]
	if !ok {
		return "", proto.Error_ERROR_NONE, nil
	} else {
		trashSub, ok = trashSubAny.(string)
		if !ok {
			return "", proto.Error_ERROR_INVALID_CONDUIT_CONFIG, fmt.Errorf("expected string for %q, got %T", CustomPluginConfigTrashKey, trashSubAny)
		} else if trashSub == "" {
			return "", proto.Error_ERROR_NONE, nil
		}
	}

	re, err := regexp.Compile(fsc.UserPathRegex)
	if err != nil {
		return "", proto.Error_ERROR_INVALID_REGEX, fmt.Errorf("failed to compile regex for filesystem userpath[%v]: %v", fsc.UserPathRegex, err)
	}

	incompleteTrashPath := ""
	incompleteTrashPath = re.ReplaceAllString(cleanUserPath, trashSub)
	if incompleteTrashPath == "" {
		return "", proto.Error_ERROR_NONE, nil
	}

	newTrashPath := incompleteTrashPath + transferID.String()
	newTrashPath = filepath.Join(newTrashPath, filepath.Base(cleanUserPath))

	return newTrashPath, proto.Error_ERROR_NONE, nil
}
