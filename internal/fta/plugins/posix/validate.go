// Copyright 2026. Triad National Security, LLC. All rights reserved.

package posix

import (
	"fmt"
	"os"
	"os/user"
	"path/filepath"

	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/internal/fta/plugin"
	"github.com/lanl/conduit/internal/logger"
	"golang.org/x/sys/unix"
)

func (p *PosixPlugin) ValidateSource(pluginPathInfo *plugin.PluginPathInfo, action proto.Action) (pluginErrors plugin.PluginErrors, pluginPathData *string) {
	p.log.Debugf("Starting posix plugin validation on: %v(%v)", pluginPathInfo.OriginalUserPath, pluginPathInfo.ResolvedFTAPath)
	p.log.Debugf("using fta source: %v", pluginPathInfo.ResolvedFTAPath)

	// validate that we have proper permissions
	p.log.Debugf("validating permissions")
	permErr := validateSourcePermissions(p.log, pluginPathInfo.OriginalUserPath, pluginPathInfo.ResolvedFTAPath, action)
	if permErr != nil {
		return plugin.PluginErrors{Errors: []*plugin.FTAPathError{permErr}}, nil
	}

	return plugin.PluginErrors{}, nil
}

// ValidateDestination validates the destination and gets all resolved destinations. This assumes the ftaDestination is already resolved of symlinks
func (p *PosixPlugin) ValidateDestination(sourceBases []string, userDestination string, ftaDestination string, fsConfig *plugin.FileSystemConfig) (pluginErrors plugin.PluginErrors, userDestinations []string, resolvedFTADestinations []string, destInfo proto.DestInfo, pluginPathData map[string]*string) {
	p.log.Debugf("posix plugin validating destination[%v](%v) with source bases %v", userDestination, ftaDestination, sourceBases)

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
func validateSourcePermissions(log *logger.ConduitLogger, transferSource string, ftaSource string, action proto.Action) *plugin.FTAPathError {
	currUser := ""
	u, err := user.Current()
	if err != nil {
		log.Errorf("failed to get current user: %v", err)
	} else {
		currUser = u.Username
	}

	switch action {
	case proto.Action_COPY, proto.Action_RECURSIVE_COPY:
		info, err := os.Lstat(ftaSource)
		if err != nil {
			tErr := fmt.Errorf("user[%v] cannot stat source path[%v](%v): %v", currUser, ftaSource, transferSource, err)
			if os.IsNotExist(err) {
				return &plugin.FTAPathError{LeasePath: transferSource, PErr: proto.Error_ERROR_FILE_NOT_EXIST, ErrMessage: tErr}
			} else {
				return &plugin.FTAPathError{LeasePath: transferSource, PErr: proto.Error_ERROR_PERMISSIONS, ErrMessage: tErr}
			}
		}

		if info.Mode()&os.ModeSymlink != 0 {
			// can we read the link text without following it?
			if _, err := os.Readlink(ftaSource); err != nil {
				tErr := fmt.Errorf("user[%v] cannot read symlink source path[%v](%v): %v", currUser, ftaSource, transferSource, err)
				if os.IsNotExist(err) {
					return &plugin.FTAPathError{LeasePath: transferSource, PErr: proto.Error_ERROR_FILE_NOT_EXIST, ErrMessage: tErr}
				} else {
					return &plugin.FTAPathError{LeasePath: transferSource, PErr: proto.Error_ERROR_PERMISSIONS, ErrMessage: tErr}
				}
			}
		} else {
			mode := uint32(unix.R_OK)
			if info.IsDir() {
				mode |= unix.X_OK
			}

			err := unix.Faccessat(
				unix.AT_FDCWD,
				ftaSource,
				mode,
				unix.AT_EACCESS,
			)
			if err != nil {
				tErr := fmt.Errorf("user[%v] does not have read permissions for source path[%v](%v): %v", currUser, ftaSource, transferSource, err)
				return &plugin.FTAPathError{LeasePath: transferSource, PErr: proto.Error_ERROR_PERMISSIONS, ErrMessage: tErr}
			}
		}
	case proto.Action_MOVE, proto.Action_RECURSIVE_MOVE:
		if _, err := os.Lstat(ftaSource); err != nil {
			tErr := fmt.Errorf("user[%v] cannot stat source path[%v](%v): %v", currUser, ftaSource, transferSource, err)
			if os.IsNotExist(err) {
				return &plugin.FTAPathError{LeasePath: transferSource, PErr: proto.Error_ERROR_FILE_NOT_EXIST, ErrMessage: tErr}
			} else {
				return &plugin.FTAPathError{LeasePath: transferSource, PErr: proto.Error_ERROR_PERMISSIONS, ErrMessage: tErr}
			}
		}

		sourceParent := filepath.Dir(ftaSource)

		err := unix.Faccessat(
			unix.AT_FDCWD,
			sourceParent,
			unix.W_OK|unix.X_OK,
			unix.AT_EACCESS,
		)
		if err != nil {
			tErr := fmt.Errorf("user[%v] does not have write/search permissions for source parent path[%v] for source[%v](%v): %v",
				currUser, sourceParent, ftaSource, transferSource, err)
			return &plugin.FTAPathError{LeasePath: transferSource, PErr: proto.Error_ERROR_PERMISSIONS, ErrMessage: tErr}
		}
	default:
		tErr := fmt.Errorf("cannot determine permissions because action is unrecognized: %v", action)
		return &plugin.FTAPathError{LeasePath: transferSource, PErr: proto.Error_ERROR_CONDUIT_INTERNAL, ErrMessage: tErr}
	}

	return nil
}
