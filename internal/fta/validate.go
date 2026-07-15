// Copyright 2026. Triad National Security, LLC. All rights reserved.

package fta

import (
	"fmt"
	"sync"

	"github.com/google/uuid"
	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/internal/etcd"
	"github.com/lanl/conduit/internal/fta/plugin"
	"github.com/lanl/conduit/internal/logger"
)

func StartPluginValidate(log *logger.ConduitLogger, it proto.IncompleteTransfer, em *etcd.ETCDManager, action proto.Action, nodeList string) (pluginData *plugin.PluginData, destInfo proto.DestInfo, _ plugin.PluginErrors) {
	pluginData = &plugin.PluginData{
		SourcePluginInfo:       make(map[string]*plugin.PluginPathInfo),
		DestinationsPluginInfo: make(map[string]*plugin.PluginPathInfo),
		PluginPathData:         make(map[string]*string),
	}

	transferID, err := uuid.Parse(it.GetTransferID())
	if err != nil {
		return pluginData, proto.DestInfo_DEST_NONE, plugin.PluginErrors{
			Errors: []*plugin.FTAPathError{{
				PErr:       proto.Error_ERROR_VALIDATION,
				ErrMessage: fmt.Errorf("failed to parse transfer id[%v]: %v", it.GetTransferID(), err),
			}},
		}
	}

	// get sources and destination for transfer
	sources, destination, pErr, err := getSrcAndDstFromETCD(it, em)
	if err != nil {
		return pluginData, proto.DestInfo_DEST_NONE, plugin.PluginErrors{
			Errors: []*plugin.FTAPathError{{
				PErr:       pErr,
				ErrMessage: fmt.Errorf("failed to get source and destination from etcd: %v", err),
			}},
		}
	}

	sourcePlugins, dstPlugin, pluginErrs := getSrcAndDstValidationPlugins(transferID, log, sources, destination)
	if len(pluginErrs.Errors) > 0 {
		return pluginData, proto.DestInfo_DEST_NONE, pluginErrs
	}

	log.Debugf("sourceplugins: %+v", sourcePlugins)

	// add destination to pluginData
	pluginData.DestinationPluginInfo = dstPlugin

	// add sources to pluginData
	for _, s := range sources {
		pluginData.SourcePluginInfo[s] = sourcePlugins[s]
	}

	var wg sync.WaitGroup

	var pluginErrors plugin.PluginErrors
	var resolvedFTADestinations, userDestinations []string
	var ppd map[string]*string
	var pdLock sync.Mutex

	wg.Add(1)
	go func() {
		defer wg.Done()
		var destPluginErrors plugin.PluginErrors
		log.Debugf("sources: %v", sources)
		log.Debugf("destination: %v", destination)
		log.Debugf("dstPlugin.ResolvedFTAPath: %v", dstPlugin.ResolvedFTAPath)
		log.Debugf("dstPlugin.FSC: %v", dstPlugin.FSC)
		destPluginErrors, userDestinations, resolvedFTADestinations, destInfo, ppd = dstPlugin.Plugin.ValidateDestination(sources, destination, dstPlugin.ResolvedFTAPath, dstPlugin.FSC)
		pdLock.Lock()
		for i, d := range userDestinations {
			pluginData.DestinationsPluginInfo[d] = &plugin.PluginPathInfo{
				OriginalUserPath: destination,
				ResolvedUserPath: d,
				ResolvedFTAPath:  resolvedFTADestinations[i],
				Plugin:           dstPlugin.Plugin,
				FSC:              dstPlugin.FSC,
			}
			pluginData.PluginPathData[d] = ppd[d]
		}
		pluginErrors.Errors = append(pluginErrors.Errors, destPluginErrors.Errors...)
		pluginErrors.Warnings = append(pluginErrors.Warnings, destPluginErrors.Warnings...)
		pdLock.Unlock()
	}()

	// get source plugins
	for _, sp := range sourcePlugins {
		wg.Add(1)
		go func(goSourcePlugin *plugin.PluginPathInfo) {
			defer wg.Done()
			srcPluginErrors, ppd := dstPlugin.Plugin.ValidateSource(goSourcePlugin, action)
			pdLock.Lock()
			pluginData.PluginPathData[goSourcePlugin.OriginalUserPath] = ppd
			pluginErrors.Errors = append(pluginErrors.Errors, srcPluginErrors.Errors...)
			pluginErrors.Warnings = append(pluginErrors.Warnings, srcPluginErrors.Warnings...)
			pdLock.Unlock()
		}(sp)
	}

	wg.Wait()

	return pluginData, destInfo, pluginErrors
}
