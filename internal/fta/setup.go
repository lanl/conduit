// Copyright 2026. Triad National Security, LLC. All rights reserved.

package fta

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/google/uuid"
	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/internal/fta/plugin"
	"github.com/lanl/conduit/internal/logger"
)

func StartPluginSetup(log *logger.ConduitLogger, t *proto.TransferDetails, client *FTAClient, nodeList string) (*plugin.PluginData, *proto.FTAPluginErrors) {
	transferID, err := uuid.Parse(t.GetTransferID())
	if err != nil {
		return nil, &proto.FTAPluginErrors{
			Errors: []*proto.FTAPathError{{
				PErr:       proto.Error_ERROR_CONDUIT_INTERNAL,
				ErrMessage: fmt.Sprintf("failed to parse transfer id[%v]: %v", t.GetTransferID(), err),
			}},
		}
	}

	pluginData, err := plugin.DecodePluginData(bytes.NewReader(t.GetPluginData()))
	if err != nil {
		return nil, &proto.FTAPluginErrors{
			Errors: []*proto.FTAPathError{
				{
					PErr:       proto.Error_ERROR_ETCD_CONNECTION,
					ErrMessage: fmt.Sprintf("failed to decode plugin data in transfer details: %v", err),
				},
			},
		}
	}

	// get setup plugins for paths
	newPluginData, pluginErrs := getPathPlugins(transferID, log, plugin.SETUP, pluginData)
	if len(pluginErrs.Errors) > 0 {
		return nil, pluginErrs
	}

	updater := NewUpdater(log, client, t)

	// run the setup for each
	var wg sync.WaitGroup

	pluginErrors := &proto.FTAPluginErrors{}
	var errorsLock sync.Mutex

	wg.Add(1)
	go func(destPluginInfo *plugin.PluginPathInfo) {
		defer wg.Done()
		dpErrors, newDpInfo := destPluginInfo.Plugin.Setup(transferID, destPluginInfo, proto.LeaseType_DESTINATION, t.GetAction(), t.GetOptions(), true, updater.updateTransferProgress)
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
			dpErrors, newDpInfo := destsPluginInfo.Plugin.Setup(transferID, destsPluginInfo, proto.LeaseType_DESTINATION, t.GetAction(), t.GetOptions(), false, updater.updateTransferProgress)
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
			spErrors, newSpInfo := srcPluginInfo.Plugin.Setup(transferID, srcPluginInfo, proto.LeaseType_SOURCE, t.GetAction(), t.GetOptions(), false, updater.updateTransferProgress)
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
