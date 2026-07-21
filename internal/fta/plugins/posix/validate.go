// Copyright 2026. Triad National Security, LLC. All rights reserved.

package posix

import (
	"fmt"
	"os"
	"os/user"
	"path/filepath"

	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/internal/fta/actions"
	"github.com/lanl/conduit/internal/fta/plugin"
	"github.com/lanl/conduit/internal/logger"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func (p *PosixPlugin) ValidateSource(pluginPathInfo *plugin.PluginPathInfo, action string, options map[string]*anypb.Any) (pluginErrors plugin.PluginErrors, pluginPathData *string, omit bool) {
	p.log.Debugf("Starting posix plugin validation on: %v(%v)", pluginPathInfo.OriginalUserPath, pluginPathInfo.ResolvedFTAPath)
	p.log.Debugf("using fta source: %v", pluginPathInfo.ResolvedFTAPath)

	// validate that we have proper permissions
	p.log.Debugf("validating permissions")

	// get recursive flag if it was provided by the user
	recursive := wrapperspb.Bool(false)

	if _, ok := options[actions.RecursiveFlag]; ok {
		if err := options[actions.RecursiveFlag].UnmarshalTo(recursive); err != nil {
			p.log.Errorf("failed to unmarshal recursive flag: %v", err)
		}
		p.log.Debugf("recursive flag exists: %v", options[actions.RecursiveFlag])
	}

	p.log.Debugf("recursive flag: %v", recursive)
	p.log.Debugf("options: %+v", options)

	// get omit missing flag if it was provided by the user
	omitMissing := wrapperspb.Bool(false)

	if _, ok := options[actions.OmitMissingFlag]; ok {
		if err := options[actions.OmitMissingFlag].UnmarshalTo(omitMissing); err != nil {
			p.log.Errorf("failed to unmarshal omit-missing flag: %v", err)
		}
	}

	permErr, isDir := validateSourcePermissions(p.log, pluginPathInfo.OriginalUserPath, pluginPathInfo.ResolvedFTAPath, action, options)
	if permErr != nil {
		if omitMissing.GetValue() && permErr.PErr == proto.Error_ERROR_FILE_NOT_EXIST {
			// this source is missing, but the user wants to ignore missing sources
			return plugin.PluginErrors{Warnings: []*plugin.FTAPathError{permErr}}, nil, true
		}
		return plugin.PluginErrors{Errors: []*plugin.FTAPathError{permErr}}, nil, false
	}

	if !recursive.GetValue() && isDir {
		// this source is a dir, but the user didn't provide a recusrive flag. Add it to warnings
		return plugin.PluginErrors{Warnings: []*plugin.FTAPathError{{
			LeasePath:  pluginPathInfo.OriginalUserPath,
			PErr:       proto.Error_ERROR_INVALID_INPUT,
			ErrMessage: fmt.Errorf("omitting directory [%v] use recursive flag to include directories", pluginPathInfo.OriginalUserPath),
		}}}, nil, true
	}

	return plugin.PluginErrors{}, nil, false
}

// ValidateDestination validates the destination and gets all resolved destinations. This assumes the ftaDestination is already resolved of symlinks
func (p *PosixPlugin) ValidateDestination(userSources []string, userDestination string, ftaDestination string, fsConfig *plugin.FileSystemConfig) (pluginErrors plugin.PluginErrors, userDestinations []string, resolvedFTADestinations []string, destInfo proto.DestInfo, pluginPathData map[string]*string) {
	p.log.Debugf("posix plugin validating destination[%v](%v) with sources %v", userDestination, ftaDestination, userSources)

	// get destinfo for destination to see if it's a directory or not
	destInfo, pErr, err := isFTADestDir(ftaDestination)
	if err != nil {
		// tErr := fmt.Errorf("failed to determine if destination is directory[%v]: %v", destFTAPath, err)
		tErr := &plugin.FTAPathError{
			LeasePath:  userDestination,
			ErrMessage: fmt.Errorf("failed to determine if destination is directory[%v]: %v", ftaDestination, err),
			PErr:       pErr,
		}
		// return destInfo, foundSymlink, leaseErrors
		return plugin.PluginErrors{Errors: []*plugin.FTAPathError{tErr}}, []string{}, []string{}, destInfo, make(map[string]*string)
	}

	// get all source bases
	sourceBases := []string{}
	for _, s := range userSources {
		sourceBases = append(sourceBases, filepath.Base(s))

		// check if any sources are exactly the same as the destination
		if s == userDestination {
			tErr := &plugin.FTAPathError{
				LeasePath:  userDestination,
				ErrMessage: fmt.Errorf("no source can match the destination: %v", s),
				PErr:       proto.Error_ERROR_VALIDATION,
			}
			return plugin.PluginErrors{Errors: []*plugin.FTAPathError{tErr}}, []string{}, []string{}, destInfo, make(map[string]*string)
		}
	}

	// the dest must exist and be a directory if multiple sources are provided
	if len(sourceBases) > 1 && (destInfo == proto.DestInfo_DEST_NOT_EXIST || destInfo == proto.DestInfo_DEST_NOT_DIR) {
		tErr := &plugin.FTAPathError{
			LeasePath:  userDestination,
			ErrMessage: fmt.Errorf("destination must exist and be a directory if multiple sources are specified"),
			PErr:       proto.Error_ERROR_VALIDATION,
		}
		return plugin.PluginErrors{Errors: []*plugin.FTAPathError{tErr}}, []string{}, []string{}, destInfo, make(map[string]*string)
	}

	userDestinations = getUserDests(sourceBases, userDestination, fsConfig, destInfo)

	resolvedFTADests := getResolvedFTADests(sourceBases, ftaDestination, fsConfig, destInfo)
	p.log.Debugf("using fta destinations: %v", resolvedFTADests)

	// validate that we have proper permissions
	p.log.Debugf("validating permissions")
	pathErrors := validateDestPermissions(p.log, userDestination, resolvedFTADests)
	if len(pathErrors) > 0 {
		return plugin.PluginErrors{Errors: pathErrors}, userDestinations, resolvedFTADests, destInfo, make(map[string]*string)
	}

	return plugin.PluginErrors{}, userDestinations, resolvedFTADests, destInfo, make(map[string]*string)
}

// getUserDests will append all source bases to the user dest depending on what destinfo is
func getUserDests(sourceBases []string, userDest string, fsConfig *plugin.FileSystemConfig, destinfo proto.DestInfo) (userDests []string) {
	userDests = []string{}

	for _, sb := range sourceBases {
		thisUserDest := ""

		if destinfo == proto.DestInfo_DEST_IS_DIR {
			thisUserDest = filepath.Join(userDest, sb)
		} else {
			thisUserDest = userDest
		}

		userDests = append(userDests, thisUserDest)

	}

	return userDests
}

// getResolvedFTADests will append all source bases to the resolved fta dest depending on what destinfo is
func getResolvedFTADests(sourceBases []string, resolvedFTADest string, fsConfig *plugin.FileSystemConfig, destinfo proto.DestInfo) (resolvedFTADests []string) {
	resolvedFTADests = []string{}

	for _, sb := range sourceBases {
		thisFTADest := ""

		if destinfo == proto.DestInfo_DEST_IS_DIR {
			thisFTADest = filepath.Join(resolvedFTADest, sb)
		} else {
			thisFTADest = resolvedFTADest
		}

		resolvedFTADests = append(resolvedFTADests, thisFTADest)

	}

	return resolvedFTADests
}

// validateDestPermissions checks that we have write permission on all destination fta paths
func validateDestPermissions(log *logger.ConduitLogger, userDestination string, ftaDestinations []string) []*plugin.FTAPathError {
	pathErrors := []*plugin.FTAPathError{}

	currUser := ""
	u, err := user.Current()
	if err != nil {
		log.Errorf("failed to get current user: %v", err)
	} else {
		currUser = u.Username
	}

	for _, d := range ftaDestinations {
		// validate destination permissions
		cleanDestParentPath := filepath.Dir(d)
		err := unix.Faccessat(
			unix.AT_FDCWD,
			cleanDestParentPath,
			unix.W_OK|unix.X_OK,
			unix.AT_EACCESS,
		)
		if err != nil {
			// failed to get access.
			tErr := fmt.Errorf("user[%v] does not have write permissions for dest parent path[%v]: %v", currUser, cleanDestParentPath, err)
			pathErrors = append(pathErrors, &plugin.FTAPathError{LeasePath: userDestination, PErr: proto.Error_ERROR_PERMISSIONS, ErrMessage: tErr})
		}
	}

	return pathErrors
}

// validateSourcePermissions performs advisory source permission checks for copy/move actions.
// Symlink sources are validated as symlinks and are not followed.
func validateSourcePermissions(log *logger.ConduitLogger, transferSource string, ftaSource string, action string, options map[string]*anypb.Any) (ftaPathError *plugin.FTAPathError, isDir bool) {
	currUser := ""
	u, err := user.Current()
	if err != nil {
		log.Errorf("failed to get current user: %v", err)
	} else {
		currUser = u.Username
	}

	switch action {
	case actions.Action_COPY:
		info, err := os.Lstat(ftaSource)
		if err != nil {
			tErr := fmt.Errorf("user[%v] cannot stat source path[%v](%v): %v", currUser, ftaSource, transferSource, err)
			if os.IsNotExist(err) {
				return &plugin.FTAPathError{LeasePath: transferSource, PErr: proto.Error_ERROR_FILE_NOT_EXIST, ErrMessage: tErr}, isDir
			} else {
				return &plugin.FTAPathError{LeasePath: transferSource, PErr: proto.Error_ERROR_PERMISSIONS, ErrMessage: tErr}, isDir
			}
		}

		if info.Mode()&os.ModeSymlink != 0 {
			// can we read the link text without following it?
			if _, err := os.Readlink(ftaSource); err != nil {
				tErr := fmt.Errorf("user[%v] cannot read symlink source path[%v](%v): %v", currUser, ftaSource, transferSource, err)
				if os.IsNotExist(err) {
					return &plugin.FTAPathError{LeasePath: transferSource, PErr: proto.Error_ERROR_FILE_NOT_EXIST, ErrMessage: tErr}, isDir
				} else {
					return &plugin.FTAPathError{LeasePath: transferSource, PErr: proto.Error_ERROR_PERMISSIONS, ErrMessage: tErr}, isDir
				}
			}
		} else {
			mode := uint32(unix.R_OK)
			if info.IsDir() {
				mode |= unix.X_OK
				isDir = true
			}

			err := unix.Faccessat(
				unix.AT_FDCWD,
				ftaSource,
				mode,
				unix.AT_EACCESS,
			)
			if err != nil {
				tErr := fmt.Errorf("user[%v] does not have read permissions for source path[%v](%v): %v", currUser, ftaSource, transferSource, err)
				return &plugin.FTAPathError{LeasePath: transferSource, PErr: proto.Error_ERROR_PERMISSIONS, ErrMessage: tErr}, isDir
			}
		}
	case actions.Action_MOVE:
		info, err := os.Lstat(ftaSource)
		if err != nil {
			tErr := fmt.Errorf("user[%v] cannot stat source path[%v](%v): %v", currUser, ftaSource, transferSource, err)
			if os.IsNotExist(err) {
				return &plugin.FTAPathError{LeasePath: transferSource, PErr: proto.Error_ERROR_FILE_NOT_EXIST, ErrMessage: tErr}, isDir
			} else {
				return &plugin.FTAPathError{LeasePath: transferSource, PErr: proto.Error_ERROR_PERMISSIONS, ErrMessage: tErr}, isDir
			}
		}

		isDir = info.IsDir()

		sourceParent := filepath.Dir(ftaSource)

		err = unix.Faccessat(
			unix.AT_FDCWD,
			sourceParent,
			unix.W_OK|unix.X_OK,
			unix.AT_EACCESS,
		)
		if err != nil {
			tErr := fmt.Errorf("user[%v] does not have write/search permissions for source parent path[%v] for source[%v](%v): %v",
				currUser, sourceParent, ftaSource, transferSource, err)
			return &plugin.FTAPathError{LeasePath: transferSource, PErr: proto.Error_ERROR_PERMISSIONS, ErrMessage: tErr}, isDir
		}
	default:
		tErr := fmt.Errorf("cannot determine permissions because action is unrecognized by posix plugin: %v", action)
		return &plugin.FTAPathError{LeasePath: transferSource, PErr: proto.Error_ERROR_CONDUIT_INTERNAL, ErrMessage: tErr}, isDir
	}

	return nil, isDir
}
