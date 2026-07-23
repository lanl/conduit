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

func StartPluginSetup(log *logger.ConduitLogger, it proto.IncompleteTransfer, em *etcd.ETCDManager, nodeList string) (pluginData *plugin.PluginData, _ plugin.PluginErrors) {
	transferID, err := uuid.Parse(it.GetTransferID())
	if err != nil {
		return nil, plugin.PluginErrors{
			Errors: []*plugin.FTAPathError{{
				PErr:       proto.Error_ERROR_CONDUIT_INTERNAL,
				ErrMessage: fmt.Errorf("failed to parse transfer id[%v]: %v", it.GetTransferID(), err),
			}},
		}
	}

	// get action and options for transfer
	action, options, err := em.GetActionAndOptions(it)
	if err != nil {
		return pluginData, plugin.PluginErrors{
			Errors: []*plugin.FTAPathError{{
				PErr:       proto.Error_ERROR_ETCD_CONNECTION,
				ErrMessage: fmt.Errorf("failed to get action and options from etcd: %v", err),
			}},
		}
	}

	// get sources and destination for transfer
	pluginData, err = em.GetPluginData(it)
	if err != nil {
		return nil, plugin.PluginErrors{
			Errors: []*plugin.FTAPathError{{
				PErr:       proto.Error_ERROR_ETCD_CONNECTION,
				ErrMessage: fmt.Errorf("failed to get source and destination from etcd: %v", err),
			}},
		}
	}

	// get setup plugins for paths
	newPluginData, pluginErrs := getPathPlugins(transferID, log, plugin.SETUP, pluginData)
	if len(pluginErrs.Errors) > 0 {
		return nil, pluginErrs
	}

	updater := NewUpdater(log, em, it)

	// run the setup for each
	var wg sync.WaitGroup

	var pluginErrors plugin.PluginErrors
	var errorsLock sync.Mutex

	wg.Add(1)
	go func(destPluginInfo *plugin.PluginPathInfo) {
		defer wg.Done()
		dpErrors, newDpInfo := destPluginInfo.Plugin.Setup(transferID, destPluginInfo, proto.LeaseType_DESTINATION, action, options, true, updater.updateTransferProgress)
		errorsLock.Lock()
		pluginErrors.Errors = append(pluginErrors.Errors, dpErrors.Errors...)
		pluginErrors.Warnings = append(pluginErrors.Warnings, dpErrors.Warnings...)
		if newDpInfo != nil {
			destPluginInfo = newDpInfo
		}
		errorsLock.Unlock()
	}(newPluginData.DestinationPluginInfo)

	for _, dppi := range newPluginData.DestinationsPluginInfo {
		wg.Add(1)
		go func(destsPluginInfo *plugin.PluginPathInfo) {
			defer wg.Done()
			dpErrors, newDpInfo := destsPluginInfo.Plugin.Setup(transferID, destsPluginInfo, proto.LeaseType_DESTINATION, action, options, false, updater.updateTransferProgress)
			errorsLock.Lock()
			pluginErrors.Errors = append(pluginErrors.Errors, dpErrors.Errors...)
			pluginErrors.Warnings = append(pluginErrors.Warnings, dpErrors.Warnings...)
			if newDpInfo != nil {
				destsPluginInfo = newDpInfo
			}
			errorsLock.Unlock()
		}(dppi)
	}

	for _, sppi := range newPluginData.SourcePluginInfo {
		wg.Add(1)
		go func(srcPluginInfo *plugin.PluginPathInfo) {
			defer wg.Done()
			spErrors, newSpInfo := srcPluginInfo.Plugin.Setup(transferID, srcPluginInfo, proto.LeaseType_SOURCE, action, options, false, updater.updateTransferProgress)
			errorsLock.Lock()
			pluginErrors.Errors = append(pluginErrors.Errors, spErrors.Errors...)
			pluginErrors.Warnings = append(pluginErrors.Warnings, spErrors.Warnings...)
			if newSpInfo != nil {
				srcPluginInfo = newSpInfo
			}
			errorsLock.Unlock()
		}(sppi)
	}

	wg.Wait()

	return newPluginData, pluginErrors
}
