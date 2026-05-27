// Copyright 2026. Triad National Security, LLC. All rights reserved.

package plugin

import (
	"fmt"
	"regexp"

	proto "github.com/lanl/conduit/api"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	DefaultFileSystemName = "default"
)

// FileSystemConfig defines how the file system configuration should look in the conduit-fta config file
type FileSystemConfig struct {
	Name                 string
	UserPathRegex        string // the regex that will define how the substitutions work for the final paths (this is from the perspective of the frontend nodes)
	FTAPathSub           string // the substitution that defines the location of the file on the FTAs
	FTARootFSPathSub     string // the substitution that defines the locations of the root of the FS from the perspective of the FTA
	PluginStages         ViperPluginStagesConfig
	CustomPluginFSConfig map[string]any
}

type ViperFSConfig struct {
	UserPathRegex        string                  `mapstructure:"user-path" yaml:"user-path"`               // the regex that will define how the substitutions work for the final paths (this is from the perspective of the frontend nodes)
	FTAPathSub           string                  `mapstructure:"fta-path" yaml:"fta-path"`                 // the substitution that defines the location of the file on the FTAs
	FTARootFSPathSub     string                  `mapstructure:"fta-root-fs-path" yaml:"fta-root-fs-path"` // the substitution that defines the location of the root of the FS from the perspective of the FTA
	PluginStages         ViperPluginStagesConfig `mapstructure:"plugin-stages" yaml:"plugin-stages"`
	CustomPluginFSConfig map[string]any          `mapstructure:"custom-plugin-config" yaml:"custom-plugin-config"`
}

type ViperPluginStagesConfig struct {
	Validation  string   `mapstructure:"validation" yaml:"validation"`
	SetupSrc    string   `mapstructure:"setup-src" yaml:"setup-src"`
	SetupDst    string   `mapstructure:"setup-dst" yaml:"setup-dst"`
	TransferSrc []string `mapstructure:"transfer-src" yaml:"transfer-src"`
	TransferDst []string `mapstructure:"transfer-dst" yaml:"transfer-dst"`
	TeardownSrc string   `mapstructure:"teardown-src" yaml:"teardown-src"`
	TeardownDst string   `mapstructure:"teardown-dst" yaml:"teardown-dst"`
}

// FTAPathError includes the lease, the protobuf error, and a detailed error message
type FTAPathError struct {
	LeasePath  string
	PErr       proto.Error
	ErrMessage error
}

// getFSCsFromViper will get the filesystem configuration from viper
func GetFSCsFromViper() (map[string]*FileSystemConfig, error) {
	vfs := make(map[string]*ViperFSConfig)
	err := viper.UnmarshalKey("filesystems", &vfs)
	if err != nil {
		return nil, fmt.Errorf("viper failed unmarshal filesystem config to struct: %v", err)
	}

	fscs := make(map[string]*FileSystemConfig)
	for fsn, vfsc := range vfs {
		fscs[fsn] = &FileSystemConfig{
			Name:                 fsn,
			UserPathRegex:        vfsc.UserPathRegex,
			FTAPathSub:           vfsc.FTAPathSub,
			FTARootFSPathSub:     vfsc.FTARootFSPathSub,
			PluginStages:         vfsc.PluginStages,
			CustomPluginFSConfig: vfsc.CustomPluginFSConfig,
		}
	}

	return fscs, nil
}

// GetFSCFromPath will check if a given user path matches any provided regex file systems in the config
func GetFSCFromPath(p string, allFileSystems map[string]*FileSystemConfig) (filesystemName string, _ *FileSystemConfig, _ proto.Error, _ error) {
	// search through the file systems in the config to determine where to put the staging area
	for fn, fsc := range allFileSystems {
		if fn == DefaultFileSystemName {
			continue
		}
		re, err := regexp.Compile(fsc.UserPathRegex)
		if err != nil {
			return "", nil, proto.Error_ERROR_INVALID_REGEX, fmt.Errorf("failed to compile regex for filesystem[%v] userpath[%v]: %v", fn, fsc.UserPathRegex, err)
		}
		if re.MatchString(p) {
			logrus.Debugf("Using file system[%v] for path[%v]", fn, p)
			return fn, fsc, proto.Error_ERROR_NONE, nil
		} else {
			logrus.Debugf("path[%v] does not match file system[%v]", p, fn)
		}
	}

	// fallback to default fs if it exists
	if fsc, ok := allFileSystems[DefaultFileSystemName]; ok {
		re, err := regexp.Compile(fsc.UserPathRegex)
		if err != nil {
			return "", nil, proto.Error_ERROR_INVALID_REGEX, fmt.Errorf("failed to compile regex for filesystem[%v] userpath[%v]: %v", DefaultFileSystemName, fsc.UserPathRegex, err)
		}
		if re.MatchString(p) {
			logrus.Debugf("Using file system[%v] for path[%v]", DefaultFileSystemName, p)
			return DefaultFileSystemName, fsc, proto.Error_ERROR_NONE, nil
		} else {
			logrus.Debugf("path[%v] does not match file system[%v]", p, DefaultFileSystemName)
		}
	}

	return "", nil, proto.Error_ERROR_CONDUIT_INTERNAL, fmt.Errorf("could not find a matching filesystem for path[%v]", p)
}

// GetPluginConfigsFromViper will get the plugin configuration from viper for a specified plugin
func GetPluginConfigsFromViper(pluginKey string) (any, error) {
	vpc := make(map[string]any)
	err := viper.UnmarshalKey("plugins", &vpc)
	if err != nil {
		return nil, fmt.Errorf("viper failed unmarshal plugin config to struct: %v", err)
	}

	if pc, ok := vpc[pluginKey]; ok {
		return pc, nil
	}

	return nil, fmt.Errorf("failed to find config for plugin in fta config file: %v", pluginKey)
}
