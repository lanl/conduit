// Copyright 2026. Triad National Security, LLC. All rights reserved.

package fta

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/google/uuid"
	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/defaults"
	"github.com/lanl/conduit/internal/etcd"
	"github.com/lanl/conduit/internal/fta/plugin"
	"github.com/lanl/conduit/internal/logger"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// ListenForKill will listen for a os kill signal (typically coming from the scheduler) and attempt to record the event in etcd
func ListenForKill(it proto.IncompleteTransfer, em *etcd.ETCDManager, sc proto.SchedulerCommand) {
	exit := make(chan os.Signal, 1) // we need to reserve to buffer size 1, so the notifier are not blocked
	signal.Notify(exit, os.Interrupt, syscall.SIGTERM)

	<-exit
	logrus.Error("received SIGTERM signal")

	txnCompare := []clientv3.Cmp{}
	txnActions := []clientv3.Op{}

	txnCompare = append(txnCompare, clientv3.Compare(clientv3.Value(it.ETCDErrorKey()), "=", proto.Error_ERROR_NONE.String()))
	// txnActions = append(txnActions, clientv3.OpPut(it.ETCDStateKey(), proto.TransferState_TRANSFER_ERROR.String()))
	txnActions = append(txnActions, clientv3.OpPut(it.ETCDErrorKey(), proto.Error_ERROR_SCHEDULER.String()))
	txnActions = append(txnActions, clientv3.OpPut(it.ETCDErrorMessageKey(), "received SIGTERM signal"))

	retryCount := viper.GetInt(defaults.ConfigFTAVerifyRetryCountKey)
	sleepDur := viper.GetDuration(defaults.ConfigFTAVerifySleepDurationKey)
	resp, err := em.RetryTxn(&txnCompare, &txnActions, retryCount, sleepDur)
	if err != nil || !resp.Succeeded {
		logrus.Fatalf("failed to set leases to error: %v", err)
	}

	os.Exit(62)
}

// getCommandStates will return the correlating "submitted", "running", and "complete" states for a given command
func getCommandStates(command proto.SchedulerCommand) (submitted proto.StringableState, running proto.StringableState, complete proto.StringableState, _ error) {
	switch command {
	case proto.SchedulerCommand_VALIDATION:
		return proto.TransferState_TRANSFER_VALIDATION_SUBMITTED, proto.TransferState_TRANSFER_VALIDATING, proto.TransferState_TRANSFER_VALIDATION_COMPLETE, nil
	case proto.SchedulerCommand_SETUP:
		return proto.TransferState_TRANSFER_SETUP_SUBMITTED, proto.TransferState_TRANSFER_SETUP, proto.TransferState_TRANSFER_SETUP_COMPLETE, nil
	case proto.SchedulerCommand_TRANSFER:
		return proto.TransferState_TRANSFER_DATA_SUBMITTED, proto.TransferState_TRANSFER_DATA_TRANSFERRING, proto.TransferState_TRANSFER_DATA_COMPLETE, nil
	case proto.SchedulerCommand_TEARDOWN:
		return proto.TransferState_TRANSFER_TEARDOWN_SUBMITTED, proto.TransferState_TRANSFER_TEARDOWN, proto.TransferState_TRANSFER_TEARDOWN_COMPLETE, nil
	}

	return proto.TransferState_TRANSFER_NONE, proto.TransferState_TRANSFER_NONE, proto.ArchiveState_ARCHIVE_NONE, fmt.Errorf("failed to get submitted state for command: %v", command)
}

// getSrcAndDstValidationPlugins will get the validatino plugin for all sources and destination and verify the resolved paths still correlate to the plugin
func getSrcAndDstValidationPlugins(transferID uuid.UUID, log *logger.ConduitLogger, sources []string, destination string) (sourcePlugins map[string]*plugin.PluginPathInfo, destinationPlugin *plugin.PluginPathInfo, _ plugin.PluginErrors) {
	fscs, err := plugin.GetFSCsFromViper()
	if err != nil {
		return sourcePlugins, destinationPlugin, plugin.PluginErrors{
			Errors: []*plugin.FTAPathError{{
				PErr:       proto.Error_ERROR_CONDUIT_INTERNAL,
				ErrMessage: fmt.Errorf("failed to get filesystem configurations from viper: %v", err),
			},
			},
		}
	}

	var wg sync.WaitGroup

	type PluginResults struct {
		PluginErrors plugin.PluginErrors
		PluginInfo   *plugin.PluginPathInfo
	}

	var destPluginResults *PluginResults
	srcPluginResults := make(map[string]*PluginResults)
	var srcPluginResultsLock sync.Mutex

	// get destination plugin
	wg.Add(1)
	go func() {
		defer wg.Done()
		destinationPlugin, pathErr := getPathValidationPlugin(transferID, log, destination, destination, fscs, proto.LeaseType_DESTINATION)
		destPluginResults = &PluginResults{
			PluginInfo: destinationPlugin,
		}
		if pathErr != nil {
			pathErr.ErrMessage = fmt.Errorf("failed to get destination[%v] plugin: %v", destination, pathErr.ErrMessage)
			destPluginResults.PluginErrors = plugin.PluginErrors{
				Errors:   []*plugin.FTAPathError{pathErr},
				Warnings: nil,
			}

			// return sourcePlugins, destinationPlugin, fmt.Errorf("failed to get destination[%v] plugin: %v", destination, err)
		}
	}()

	// get source plugins
	for _, s := range sources {
		wg.Add(1)
		go func(goSource string) {
			defer wg.Done()
			sourcePlugin, pathErr := getPathValidationPlugin(transferID, log, goSource, goSource, fscs, proto.LeaseType_SOURCE)
			srcPluginResultsLock.Lock()
			srcPluginResults[goSource] = &PluginResults{
				PluginInfo: sourcePlugin,
			}
			srcPluginResultsLock.Unlock()
			if pathErr != nil {
				pathErr.ErrMessage = fmt.Errorf("failed to get source[%v] plugin: %v", goSource, pathErr.ErrMessage)
				srcPluginResultsLock.Lock()
				srcPluginResults[goSource].PluginErrors = plugin.PluginErrors{
					Errors:   []*plugin.FTAPathError{pathErr},
					Warnings: nil,
				}
				srcPluginResultsLock.Unlock()

				// return sourcePlugins, destinationPlugin, fmt.Errorf("failed to get source[%v] plugin: %v", s, err)
			}

		}(s)
	}

	wg.Wait()

	// combine all errors together
	allErrors := plugin.PluginErrors{
		Errors: []*plugin.FTAPathError{},
	}
	if len(destPluginResults.PluginErrors.Errors) > 0 {
		allErrors.Errors = append(allErrors.Errors, destPluginResults.PluginErrors.Errors...)
	}
	destinationPlugin = destPluginResults.PluginInfo

	for sprk, spr := range srcPluginResults {
		if len(spr.PluginErrors.Errors) > 0 {
			allErrors.Errors = append(allErrors.Errors, spr.PluginErrors.Errors...)
		}
		if sourcePlugins == nil {
			sourcePlugins = make(map[string]*plugin.PluginPathInfo)
		}
		sourcePlugins[sprk] = spr.PluginInfo
	}

	// check for errors
	if len(allErrors.Errors) > 0 {
		return sourcePlugins, destinationPlugin, allErrors
	}

	return sourcePlugins, destinationPlugin, plugin.PluginErrors{}
}

// getPathValidationPlugin is a recursive function that will find the validation plugin for a given path, initialize the plugin, check that it doesn't change with a resolved path, and find the correct plugin for any different resolved paths. It also verifies that the plugin has the validation capability
func getPathValidationPlugin(transferID uuid.UUID, log *logger.ConduitLogger, originalUserPath string, newUserPath string, fscs map[string]*plugin.FileSystemConfig, pathType proto.LeaseType) (*plugin.PluginPathInfo, *plugin.FTAPathError) {
	fsn, fsc, pErr, err := plugin.GetFSCFromPath(newUserPath, fscs)
	if err != nil {
		return nil, &plugin.FTAPathError{
			LeasePath:  originalUserPath,
			PErr:       pErr,
			ErrMessage: fmt.Errorf("failed to get filesystem configuration for path[%v]: %v", newUserPath, err),
		}
	} else {
		log.Debugf("using filesystem [%v] for path [%v]", fsn, newUserPath)
	}

	pluginString := fsc.PluginStages.Validation

	pathPlugin, ok := PluginMap[pluginString]
	if !ok {
		return nil, &plugin.FTAPathError{
			LeasePath:  originalUserPath,
			PErr:       proto.Error_ERROR_INVALID_CONDUIT_CONFIG,
			ErrMessage: fmt.Errorf("failed to find [%v] in plugin map. plugin [%v] is not supported in this version of conduit-fta", pluginString, pluginString),
		}
	}

	pluginCaps := pathPlugin.Initialize(transferID, log)

	// see if the resolved path is the same
	ftaPath, symlink, pathError := pathPlugin.GetResolvedPath(newUserPath, pathType, fsc)
	if pathError != nil {
		return nil, pathError
	}

	if symlink != "" {
		log.Infof("destination plugin found a symlink while resolving path[%v]. Finding plugin for new path[%v]", newUserPath, symlink)
		return getPathValidationPlugin(transferID, log, originalUserPath, symlink, fscs, pathType)
	}

	// verify validation is in the plugins capabilities
	foundCap := false
	for _, c := range pluginCaps {
		if c == plugin.VALIDATION {
			foundCap = true
			break
		}
	}
	if !foundCap {
		return nil, &plugin.FTAPathError{
			LeasePath:  originalUserPath,
			PErr:       proto.Error_ERROR_INVALID_CONDUIT_CONFIG,
			ErrMessage: fmt.Errorf("this plugin[%v] does not support validation", pluginString),
		}
	}

	ppi := &plugin.PluginPathInfo{
		OriginalUserPath: originalUserPath,
		ResolvedUserPath: newUserPath,
		ResolvedFTAPath:  ftaPath,
		Plugin:           pathPlugin,
		FSC:              fsc,
	}

	return ppi, nil
}

// getPathPlugins will get the plugin for a transfer that's already gone through the validation phase
func getPathPlugins(transferID uuid.UUID, log *logger.ConduitLogger, currentStep plugin.PluginCapability, pluginData *plugin.PluginData) (*plugin.PluginData, plugin.PluginErrors) {
	pluginErrors := plugin.PluginErrors{
		Errors:   []*plugin.FTAPathError{},
		Warnings: []*plugin.FTAPathError{},
	}

	// get base destination plugin
	pluginString := ""
	switch currentStep {
	case plugin.SETUP:
		pluginString = pluginData.DestinationPluginInfo.FSC.PluginStages.SetupDst
	case plugin.TEARDOWN:
		pluginString = pluginData.DestinationPluginInfo.FSC.PluginStages.TeardownDst
	}

	pathPlugin, pErr, err := getPluginFromString(log, transferID, currentStep, pluginString)
	if err != nil {
		pluginErrors.Errors = append(pluginErrors.Errors, &plugin.FTAPathError{
			LeasePath:  pluginData.DestinationPluginInfo.OriginalUserPath,
			PErr:       pErr,
			ErrMessage: err,
		})
	}

	pluginData.DestinationPluginInfo.Plugin = pathPlugin

	// get plugins for each destination
	for _, dppi := range pluginData.DestinationsPluginInfo {
		pluginString := ""
		switch currentStep {
		case plugin.SETUP:
			pluginString = dppi.FSC.PluginStages.SetupDst
		case plugin.TEARDOWN:
			pluginString = dppi.FSC.PluginStages.TeardownDst
		}

		pathPlugin, pErr, err := getPluginFromString(log, transferID, currentStep, pluginString)
		if err != nil {
			pluginErrors.Errors = append(pluginErrors.Errors, &plugin.FTAPathError{
				LeasePath:  dppi.OriginalUserPath,
				PErr:       pErr,
				ErrMessage: err,
			})
		}

		dppi.Plugin = pathPlugin
	}

	// get source plugins
	for _, sppi := range pluginData.SourcePluginInfo {
		pluginString := ""
		switch currentStep {
		case plugin.SETUP:
			pluginString = sppi.FSC.PluginStages.SetupSrc
		case plugin.TEARDOWN:
			pluginString = sppi.FSC.PluginStages.TeardownSrc
		}

		pathPlugin, pErr, err := getPluginFromString(log, transferID, currentStep, pluginString)
		if err != nil {
			pluginErrors.Errors = append(pluginErrors.Errors, &plugin.FTAPathError{
				LeasePath:  sppi.OriginalUserPath,
				PErr:       pErr,
				ErrMessage: err,
			})
		}

		sppi.Plugin = pathPlugin
	}

	return pluginData, pluginErrors
}

// getPluginFromString gets the actual ConduitFTAPlugin from a provided plugin string
func getPluginFromString(log *logger.ConduitLogger, transferID uuid.UUID, currentStep plugin.PluginCapability, pluginString string) (plugin.ConduitFTAPlugin, proto.Error, error) {
	pathPlugin, ok := PluginMap[pluginString]
	if !ok {
		return nil, proto.Error_ERROR_INVALID_CONDUIT_CONFIG, fmt.Errorf("failed to find [%v] in plugin map. plugin [%v] is not supported in this version of conduit-fta", pluginString, pluginString)
	}

	pluginCaps := pathPlugin.Initialize(transferID, log)

	// verify plugins capabilities
	foundCap := false
	for _, c := range pluginCaps {
		if c == currentStep {
			foundCap = true
			break
		}
	}
	if !foundCap {
		return nil, proto.Error_ERROR_INVALID_CONDUIT_CONFIG, fmt.Errorf("this plugin[%v] does not have this capability", pluginString)
	}

	return pathPlugin, proto.Error_ERROR_NONE, nil
}

// getIntersection simply returns the intersection between two slices
func getIntersection[T comparable](a []T, b []T) []T {
	set := make([]T, 0)

	for _, v := range a {
		if containsGeneric(b, v) {
			set = append(set, v)
		}
	}

	return set
}

// used for getMapIntersection
func containsGeneric[T comparable](b []T, e T) bool {
	for _, v := range b {
		if v == e {
			return true
		}
	}
	return false
}

func removeDuplicates[T comparable](sliceList []T) []T {
	allKeys := make(map[T]bool)
	list := []T{}
	for _, item := range sliceList {
		if _, value := allKeys[item]; !value {
			allKeys[item] = true
			list = append(list, item)
		}
	}
	return list
}
