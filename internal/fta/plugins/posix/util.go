// Copyright 2026. Triad National Security, LLC. All rights reserved.

package posix

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/internal/fta/actions"
	"github.com/lanl/conduit/internal/fta/plugin"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func (p *PosixPlugin) GetResolvedPath(userPath string, pathType proto.LeaseType, fsc *plugin.FileSystemConfig, options map[string]*anypb.Any) (resolvedFTAPath string, foundSymlink string, _ *proto.FTAPathError) {
	// check for --parents option only for destination
	parents := false
	if pathType == proto.LeaseType_DESTINATION {
		if opt, ok := options[actions.ParentsFlag]; ok {
			var parentsFlag wrapperspb.BoolValue
			if err := opt.UnmarshalTo(&parentsFlag); err != nil {
				p.log.Errorf("failed to unmarshal parents flag")
			}

			parents = parentsFlag.GetValue()
		}
	}

	// get fsr for path
	ftaPath, foundSymlink, pErr, err := ResolvePathWithConfig(userPath, pathType, fsc, parents)
	if err != nil {
		return ftaPath, foundSymlink, &proto.FTAPathError{LeasePath: userPath, PErr: pErr, ErrMessage: fmt.Sprintf("failed to get fs result for path[%v]: %v", userPath, err)}
	}

	return ftaPath, foundSymlink, nil
}

// ResolvePathWithConfig returns the correct substituted paths for a given user path
func ResolvePathWithConfig(cleanUserPath string, lt proto.LeaseType, fsc *plugin.FileSystemConfig, parentsFlag bool) (ftaPath string, foundSymlink string, _ proto.Error, _ error) {
	logrus.Debugf("using filesystem [%v] for %s[%v]", fsc.Name, lt, cleanUserPath)
	logrus.Debugf("using filesystem config: %+v", fsc)

	// compile the regex to use for substitution
	re, err := regexp.Compile(fsc.UserPathRegex)
	if err != nil {
		return "", "", proto.Error_ERROR_INVALID_REGEX, fmt.Errorf("failed to compile regex for filesystem userpath[%v]: %v", fsc.UserPathRegex, err)
	}

	// execute the substitution to get the fta path
	subbedFTAPath := re.ReplaceAllString(cleanUserPath, fsc.FTAPathSub)
	subbedFTARootFSPath := re.ReplaceAllString(cleanUserPath, fsc.FTARootFSPathSub)
	logrus.Debugf("ftaPath after substitution: [%v]", subbedFTAPath)
	logrus.Debugf("sub: [%v]", fsc.FTAPathSub)
	logrus.Debugf("regex: [%v]", fsc.UserPathRegex)

	// split up the user path suffix from the fta path
	userPathSuffix, found := strings.CutPrefix(subbedFTAPath, subbedFTARootFSPath)
	if !found {
		return "", "", proto.Error_ERROR_INVALID_CONDUIT_CONFIG, fmt.Errorf("failed to cut fs prefix[%v] from path[%v]. Is fta-root-fs-path configured correctly?", subbedFTARootFSPath, subbedFTAPath)
	}

	logrus.Debugf("found user path suffix: %v", userPathSuffix)

	// remove the userPathSuffix from the user path to get the user path prefix
	userPathPrefix, found := strings.CutSuffix(cleanUserPath, userPathSuffix)
	if !found {
		return "", "", proto.Error_ERROR_INVALID_CONDUIT_CONFIG, fmt.Errorf("failed to cut fs suffix[%v] from path[%v]. Is fta-root-fs-path configured correctly?", userPathSuffix, cleanUserPath)
	}

	logrus.Debugf("found user path prefix: %v", userPathPrefix)

	foundAbsPath, finalPath, pErr, err := symWalk(userPathSuffix, subbedFTARootFSPath, userPathPrefix, lt, parentsFlag)
	if err != nil {
		return "", "", pErr, fmt.Errorf("failed to walk path for symlinks: %v", err)
	}

	if foundAbsPath != "" {
		return "", foundAbsPath, proto.Error_ERROR_NONE, nil
		// return initialFSResult(foundAbsPath, lt, allFileSystems, stagingInfo, conduitLinkBehavior)
	}

	return finalPath, "", proto.Error_ERROR_NONE, nil
}

// symWalk walks down a path. If it encounters a symlink it rebuilds the path.
// If the symlink is an absolute path it will rerun symWalk on the absolute path
// If the symlink is a relative path it will follow it unless it goes beyond the specified filesystem. If that happens we error
// parameters:
// pathSuffix: the path after the root of the filesystem
// ftaPathPrefix: the root of the fs from the fta perspective
// userPathPrefix: the root of the fs from the fe perspective
// lt: the lease type
// returns:
// foundAbsPath: if symwalk finds a symlink to an aboslute path, it will return it here
// finalFTAPath: the final absolute fta path after following symlinks (excluding conduit staging symlinks)
// PErr: the conduit error
// err: the error message
func symWalk(pathSuffix string, ftaPathPrefix string, userPathPrefix string, lt proto.LeaseType, parentsFlag bool) (foundAbsPath, finalFTAPath string, pErr proto.Error, err error) {
	// separate the path into a slice containing each section separated by the path separator ('/' for linux)
	pathSlice := strings.Split(filepath.Clean(pathSuffix), string(os.PathSeparator))
	logrus.Debugf("slice for path[%v]: %v", pathSuffix, pathSlice)
	var currentFullPath string
	// one-by-one rebuild the original path and check for symlinks
	for i := 1; i <= len(pathSlice); i++ {
		currentFullPath = filepath.Join(ftaPathPrefix, filepath.Join(pathSlice[0:i]...))
		logrus.Debugf("working with this path: %v", currentFullPath)

		fi, err := os.Lstat(currentFullPath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				if lt == proto.LeaseType_DESTINATION && parentsFlag {
					// Allow missing destination components. Setup may create
					// the parent directories if --parents was specified.
					return "", filepath.Join(ftaPathPrefix, filepath.Join(pathSlice...)), proto.Error_ERROR_NONE, nil
				}

				if i == len(pathSlice) {
					continue
				}
			}
			return "", "", proto.Error_ERROR_STAT_FAILED, fmt.Errorf("failed to lstat path[%v]: %v", currentFullPath, err)
		}

		currentPathIsSymlink := fi.Mode()&os.ModeSymlink == os.ModeSymlink

		// check for symlink:
		if currentPathIsSymlink {
			// found a symlink:
			newPath, err := os.Readlink(currentFullPath)
			if err != nil {
				return "", "", proto.Error_ERROR_SYMLINK_FAILED, fmt.Errorf("failed to read symlink for path[%v]: %v", currentFullPath, err)
			}
			logrus.Debugf("File is a symbolic link to: %v", newPath)

			// check if we're at the end of the path
			if i == len(pathSlice) {
				// check if the path is a destination
				if lt == proto.LeaseType_DESTINATION {
					logrus.Debugf("found a symlink[%s] at the end of the path[%s], but this is a destination. Following", newPath, currentFullPath)
				} else {
					logrus.Debugf("found a symlink[%s] at the end of the path[%s], but this is a source. Ignoring", newPath, currentFullPath)
					continue
				}
			} else {
				// its not at the end of the path
				logrus.Debugf("found a symlink[%s] in the path[%s]. Following", newPath, currentFullPath)
			}

			// check if the link is a relative path
			if !filepath.IsAbs(newPath) {
				// the link is a relative path
				// recreate the absolute user path with the new relative symlink
				return filepath.Join(userPathPrefix, filepath.Dir(filepath.Join(pathSlice[0:i]...)), newPath, filepath.Join(pathSlice[i:]...)), "", proto.Error_ERROR_NONE, nil
			} else {
				// the link is an absolute path. rerun the regex substitution and check for symlinks all over again
				return filepath.Join(newPath, filepath.Join(pathSlice[i:]...)), "", proto.Error_ERROR_NONE, nil
			}
		} else {
			// it's not a symlink. continue
			logrus.Debugf("File is not a symbolic link")
		}
	}
	return "", currentFullPath, proto.Error_ERROR_NONE, nil
}

// isUserDestDir will check if the destination path is a directory. It returns a DestInfo object for exact details
func isFTADestDir(destFTAPath string) (proto.DestInfo, proto.Error, error) {
	destExists := true
	// check if dest is a directory
	ds, err := os.Lstat(destFTAPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// destination path does not exist
			destExists = false
		} else {
			return proto.DestInfo_DEST_NONE, proto.Error_ERROR_STAT_FAILED, fmt.Errorf("failed to get stats for destination path[%v]: %v", destFTAPath, err)
		}
	}

	destInfo := proto.DestInfo_DEST_NONE
	switch {
	case !destExists:
		destInfo = proto.DestInfo_DEST_NOT_EXIST
	case !ds.IsDir():
		destInfo = proto.DestInfo_DEST_NOT_DIR
	case ds.IsDir():
		destInfo = proto.DestInfo_DEST_IS_DIR
	}

	// check if the file is a symlink and that we're told to succeed on conduit links.
	// If this is the case, we can safely assume that we're in a validation run and that we should set destinfo to a link.
	// This will tell upstream code that we most likely encountered a conduit symlink which should really succeed validation.
	// below is true if the file is a symlink
	// if !(ds.Mode()&os.ModeSymlink == 0) && destInfo == proto.DestInfo_DEST_NOT_DIR && conduitLinkBehavior == LINK_SUCCEED {
	// 	destInfo = proto.DestInfo_DEST_IS_LINK
	// }

	if destInfo == proto.DestInfo_DEST_NONE {
		return proto.DestInfo_DEST_NONE, proto.Error_ERROR_CONDUIT_INTERNAL, fmt.Errorf("conduit failed to determine destination info: %v", destFTAPath)
	}

	return destInfo, proto.Error_ERROR_NONE, nil
}

func findExistingParent(path string) (string, error) {
	parent := filepath.Dir(path)

	for {
		info, err := os.Stat(parent)
		if err == nil {
			if !info.IsDir() {
				return "", fmt.Errorf("path[%s] is not a directory", parent)
			}
			return parent, nil
		}

		if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}

		next := filepath.Dir(parent)
		if next == parent {
			return "", fmt.Errorf("no existing parent for path[%s]", path)
		}
		parent = next
	}
}
