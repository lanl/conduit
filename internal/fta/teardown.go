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

func StartPluginTeardown(log *logger.ConduitLogger, t *proto.TransferDetails, client *FTAClient, nodeList string) *proto.FTAPluginErrors {
	transferID, err := uuid.Parse(t.GetTransferID())
	if err != nil {
		return &proto.FTAPluginErrors{
			Errors: []*proto.FTAPathError{{
				PErr:       proto.Error_ERROR_CONDUIT_INTERNAL,
				ErrMessage: fmt.Sprintf("failed to parse transfer id[%v]: %v", t.GetTransferID(), err),
			}},
		}
	}

	pluginData, err := plugin.DecodePluginData(bytes.NewReader(t.GetPluginData()))
	if err != nil {
		return &proto.FTAPluginErrors{
			Errors: []*proto.FTAPathError{
				{
					PErr:       proto.Error_ERROR_ETCD_CONNECTION,
					ErrMessage: fmt.Sprintf("failed to decode plugin data in transfer details: %v", err),
				},
			},
		}
	}

	log.Debug("pluginData: %+v", pluginData)
	log.Debug("pluginDataraw: %s", string(t.GetPluginData()))
	log.Debugf("pluginData.DestinationPluginInfo: %+v", pluginData.DestinationPluginInfo)
	log.Debug("pluginData.DestinationPluginInfo.FSC: %+v", pluginData.DestinationPluginInfo.FSC)
	log.Debug("pluginData.DestinationPluginInfo.FSC.PluginStages: %+v", pluginData.DestinationPluginInfo.FSC.PluginStages)
	log.Debug("pluginData.DestinationPluginInfo.FSC.PluginStages.TeardownDst: %+v", pluginData.DestinationPluginInfo.FSC.PluginStages.TeardownDst)

	// get teardown plugins for paths
	newPluginData, pluginErrs := getPathPlugins(transferID, log, plugin.TEARDOWN, pluginData)
	if len(pluginErrs.Errors) > 0 {
		return pluginErrs
	}

	updater := NewUpdater(log, client, t)

	// run the setup for each
	var wg sync.WaitGroup

	pluginErrors := &proto.FTAPluginErrors{}
	var errorsLock sync.Mutex

	wg.Add(1)
	go func(destPluginInfo *plugin.PluginPathInfo) {
		defer wg.Done()
		dpErrors := destPluginInfo.Plugin.Teardown(transferID, t, destPluginInfo, proto.LeaseType_DESTINATION, t.GetAction(), t.GetOptions(), true, updater.updateTransferProgress)
		errorsLock.Lock()
		pluginErrors.Errors = append(pluginErrors.Errors, dpErrors.Errors...)
		pluginErrors.Warnings = append(pluginErrors.Warnings, dpErrors.Warnings...)
		errorsLock.Unlock()
	}(newPluginData.DestinationPluginInfo)

	for _, dppi := range newPluginData.DestinationsPluginInfo {
		wg.Add(1)
		go func(destsPluginInfo *plugin.PluginPathInfo) {
			defer wg.Done()
			dpErrors := destsPluginInfo.Plugin.Teardown(transferID, t, destsPluginInfo, proto.LeaseType_DESTINATION, t.GetAction(), t.GetOptions(), false, updater.updateTransferProgress)
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
			spErrors := srcPluginInfo.Plugin.Teardown(transferID, t, srcPluginInfo, proto.LeaseType_SOURCE, t.GetAction(), t.GetOptions(), false, updater.updateTransferProgress)
			errorsLock.Lock()
			pluginErrors.Errors = append(pluginErrors.Errors, spErrors.Errors...)
			pluginErrors.Warnings = append(pluginErrors.Warnings, spErrors.Warnings...)
			errorsLock.Unlock()
		}(sppi)
	}

	wg.Wait()

	return pluginErrors
}
