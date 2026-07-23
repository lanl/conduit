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

func StartPluginTeardown(log *logger.ConduitLogger, it proto.IncompleteTransfer, em *etcd.ETCDManager, nodeList string) plugin.PluginErrors {
	transferID, err := uuid.Parse(it.GetTransferID())
	if err != nil {
		return plugin.PluginErrors{
			Errors: []*plugin.FTAPathError{{
				PErr:       proto.Error_ERROR_CONDUIT_INTERNAL,
				ErrMessage: fmt.Errorf("failed to parse transfer id[%v]: %v", it.GetTransferID(), err),
			}},
		}
	}

	// get transfer information
	transferDetails, pErr, err := em.GetTransfer(transferID)
	if err != nil {
		return plugin.PluginErrors{
			Errors: []*plugin.FTAPathError{
				{
					PErr:       pErr,
					ErrMessage: fmt.Errorf("failed to get transfer details from etcd: %v", err),
				},
			},
		}
	}

	// get sources and destination for transfer
	pluginData, err := em.GetPluginData(it)
	if err != nil {
		return plugin.PluginErrors{
			Errors: []*plugin.FTAPathError{{
				PErr:       proto.Error_ERROR_ETCD_CONNECTION,
				ErrMessage: fmt.Errorf("failed to get source and destination from etcd: %v", err),
			}},
		}
	}

	// get setup plugins for paths
	newPluginData, pluginErrs := getPathPlugins(transferID, log, plugin.SETUP, pluginData)
	if len(pluginErrs.Errors) > 0 {
		return pluginErrs
	}

	updater := NewUpdater(log, em, it)

	// run the setup for each
	var wg sync.WaitGroup

	var pluginErrors plugin.PluginErrors
	var errorsLock sync.Mutex

	wg.Add(1)
	go func(destPluginInfo *plugin.PluginPathInfo) {
		defer wg.Done()
		dpErrors := destPluginInfo.Plugin.Teardown(transferID, transferDetails, destPluginInfo, proto.LeaseType_DESTINATION, transferDetails.GetAction(), transferDetails.GetOptions(), true, updater.updateTransferProgress)
		errorsLock.Lock()
		pluginErrors.Errors = append(pluginErrors.Errors, dpErrors.Errors...)
		pluginErrors.Warnings = append(pluginErrors.Warnings, dpErrors.Warnings...)
		errorsLock.Unlock()
	}(newPluginData.DestinationPluginInfo)

	for _, dppi := range newPluginData.DestinationsPluginInfo {
		wg.Add(1)
		go func(destsPluginInfo *plugin.PluginPathInfo) {
			defer wg.Done()
			dpErrors := destsPluginInfo.Plugin.Teardown(transferID, transferDetails, destsPluginInfo, proto.LeaseType_DESTINATION, transferDetails.GetAction(), transferDetails.GetOptions(), false, updater.updateTransferProgress)
			errorsLock.Lock()
			pluginErrors.Errors = append(pluginErrors.Errors, dpErrors.Errors...)
			pluginErrors.Warnings = append(pluginErrors.Warnings, dpErrors.Warnings...)
			errorsLock.Unlock()
		}(dppi)
	}

	for _, sppi := range newPluginData.SourcePluginInfo {
		wg.Add(1)
		go func(srcPluginInfo *plugin.PluginPathInfo) {
			defer wg.Done()
			spErrors := srcPluginInfo.Plugin.Teardown(transferID, transferDetails, srcPluginInfo, proto.LeaseType_SOURCE, transferDetails.GetAction(), transferDetails.GetOptions(), false, updater.updateTransferProgress)
			errorsLock.Lock()
			pluginErrors.Errors = append(pluginErrors.Errors, spErrors.Errors...)
			pluginErrors.Warnings = append(pluginErrors.Warnings, spErrors.Warnings...)
			errorsLock.Unlock()
		}(sppi)
	}

	wg.Wait()

	return pluginErrors
}
