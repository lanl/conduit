// Copyright 2026. Triad National Security, LLC. All rights reserved.

package fta

import (
	"fmt"
	"sync"

	"github.com/google/uuid"
	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/defaults"
	"github.com/lanl/conduit/internal/fta/plugin"
	"github.com/lanl/conduit/internal/logger"
	"github.com/spf13/viper"
)

func StartPluginValidate(log *logger.ConduitLogger, t *proto.TransferDetails, nodeList string) (pluginData *plugin.PluginData, destInfo proto.DestInfo, _ *proto.FTAPluginErrors) {
	pluginData = &plugin.PluginData{
		SourcePluginInfo:       make(map[string]*plugin.PluginPathInfo),
		DestinationsPluginInfo: make(map[string]*plugin.PluginPathInfo),
		PluginPathData:         make(map[string]*string),
	}

	transferID, err := uuid.Parse(t.GetTransferID())
	if err != nil {
		return pluginData, proto.DestInfo_DEST_NONE, &proto.FTAPluginErrors{
			Errors: []*proto.FTAPathError{{
				PErr:       proto.Error_ERROR_VALIDATION,
				ErrMessage: fmt.Sprintf("failed to parse transfer id[%v]: %v", t.GetTransferID(), err),
			}},
		}
	}

	pluginErrors := &proto.FTAPluginErrors{}

	// glob the sources
	fscs, err := plugin.GetFSCsFromViper()
	if err != nil {
		return pluginData, proto.DestInfo_DEST_NONE, &proto.FTAPluginErrors{
			Errors: []*proto.FTAPathError{{
				PErr:       proto.Error_ERROR_CONDUIT_INTERNAL,
				ErrMessage: fmt.Sprintf("failed to get filesystem configurations from viper: %v", err),
			}},
		}
	}

	globbedSources := []string{}
	for _, s := range t.GetSource() {
		gs, pathErr := globSource(transferID, log, s, fscs)
		if pathErr != nil {
			pluginErrors.Warnings = append(pluginErrors.Warnings, pathErr)
		}

		globbedSources = append(globbedSources, gs...)
	}

	// limit character count. This is to prevent a user from passing a wildcard that blows up etcd and slows down queries (it would fail the arg limit at the transfer stage)
	maxSourceBytes := viper.GetInt(defaults.ConfigMaxSourceBytesKey)
	byteCount := 0
	for _, s := range globbedSources {
		byteCount += len(s)
	}

	if byteCount > maxSourceBytes {
		pluginErrors.Errors = []*proto.FTAPathError{{
			PErr:       proto.Error_ERROR_INVALID_INPUT,
			ErrMessage: fmt.Sprintf("request contains too many sources. byte limit: %v, received: %v. Please use a directory instead of a wildcard when transferring a large number of sources", maxSourceBytes, byteCount),
		}}

		return pluginData, proto.DestInfo_DEST_NONE, pluginErrors
	}

	srcPlugins, dstPlugin, pluginErrs := getSrcAndDstValidationPlugins(transferID, log, globbedSources, t.GetDestination())

	pluginErrors.Errors = append(pluginErrors.Errors, pluginErrs.Errors...)
	pluginErrors.Warnings = append(pluginErrors.Warnings, pluginErrs.Warnings...)

	if len(pluginErrors.Errors) > 0 {
		return pluginData, proto.DestInfo_DEST_NONE, pluginErrors
	}

	log.Debugf("sourceplugins: %+v", srcPlugins)

	// add destination to pluginData
	pluginData.DestinationPluginInfo = dstPlugin

	// add sources to pluginData
	for _, s := range globbedSources {
		pluginData.SourcePluginInfo[s] = srcPlugins[s]
	}

	var wg sync.WaitGroup

	var resolvedFTADestinations, userDestinations []string
	var ppd map[string]*string
	var pdLock sync.Mutex
	omitList := []string{}
	var omitLock sync.Mutex

	// get source plugins
	for _, sp := range srcPlugins {
		wg.Add(1)
		go func(goSourcePlugin *plugin.PluginPathInfo) {
			defer wg.Done()
			srcPluginErrors, ppd, omit := sp.Plugin.ValidateSource(goSourcePlugin, t.GetAction(), t.GetOptions())
			pdLock.Lock()
			if omit {
				omitLock.Lock()
				omitList = append(omitList, goSourcePlugin.OriginalUserPath)
				omitLock.Unlock()
			} else {
				pluginData.PluginPathData[goSourcePlugin.OriginalUserPath] = ppd
			}
			pluginErrors.Errors = append(pluginErrors.Errors, srcPluginErrors.Errors...)
			pluginErrors.Warnings = append(pluginErrors.Warnings, srcPluginErrors.Warnings...)
			pdLock.Unlock()
		}(sp)
	}

	wg.Wait()

	for _, o := range omitList {
		delete(pluginData.SourcePluginInfo, o)
	}

	filteredSources := []string{}
	for _, spi := range pluginData.SourcePluginInfo {
		filteredSources = append(filteredSources, spi.OriginalUserPath)
	}

	// check that there are any sources left
	if len(filteredSources) == 0 {
		warnings := ""
		for _, w := range pluginErrors.Warnings {
			if warnings == "" {
				warnings = w.ErrMessage
			} else {
				warnings = fmt.Sprintf("%v; %v", warnings, w.ErrMessage)
			}
		}

		pluginErrors.Errors = append(pluginErrors.Errors, &proto.FTAPathError{
			PErr:       proto.Error_ERROR_INVALID_INPUT,
			ErrMessage: fmt.Sprintf("No valid sources provided; %v", warnings),
		})

		return pluginData, proto.DestInfo_DEST_NONE, pluginErrors
	}

	var destPluginErrors *proto.FTAPluginErrors
	log.Debugf("sources: %v", globbedSources)
	log.Debugf("destination: %v", t.GetDestination())
	log.Debugf("dstPlugin.ResolvedFTAPath: %v", dstPlugin.ResolvedFTAPath)
	log.Debugf("dstPlugin.FSC: %v", dstPlugin.FSC)
	destPluginErrors, userDestinations, resolvedFTADestinations, destInfo, ppd = dstPlugin.Plugin.ValidateDestination(filteredSources, t.GetDestination(), dstPlugin.ResolvedFTAPath, dstPlugin.FSC)
	for i, d := range userDestinations {
		pluginData.DestinationsPluginInfo[d] = &plugin.PluginPathInfo{
			OriginalUserPath: t.GetDestination(),
			ResolvedUserPath: d,
			ResolvedFTAPath:  resolvedFTADestinations[i],
			Plugin:           dstPlugin.Plugin,
			FSC:              dstPlugin.FSC,
		}
		pluginData.PluginPathData[d] = ppd[d]
	}
	pluginErrors.Errors = append(pluginErrors.Errors, destPluginErrors.Errors...)
	pluginErrors.Warnings = append(pluginErrors.Warnings, destPluginErrors.Warnings...)

	return pluginData, destInfo, pluginErrors
}
