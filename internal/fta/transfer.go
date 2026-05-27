// Copyright 2026. Triad National Security, LLC. All rights reserved.

package fta

import (
	"fmt"

	"github.com/google/uuid"
	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/internal/etcd"
	"github.com/lanl/conduit/internal/fta/plugin"
	"github.com/lanl/conduit/internal/logger"
)

func StartPluginTransfer(log *logger.ConduitLogger, it proto.IncompleteTransfer, em *etcd.ETCDManager, action proto.Action, nodeList string) plugin.PluginErrors {
	transferID, err := uuid.Parse(it.GetTransferID())
	if err != nil {
		return plugin.PluginErrors{
			Errors: []*plugin.FTAPathError{
				{
					PErr:       proto.Error_ERROR_CONDUIT_INTERNAL,
					ErrMessage: fmt.Errorf("failed to parse transfer id[%v]: %v", it.GetTransferID(), err),
				},
			},
		}
	}

	// get sources and destination for transfer
	pluginData, pErr, err := getPluginDataFromETCD(it, em)
	if err != nil {
		return plugin.PluginErrors{
			Errors: []*plugin.FTAPathError{
				{
					PErr:       pErr,
					ErrMessage: fmt.Errorf("failed to get plugin data from etcd: %v", err),
				},
			},
		}
	}

	// get setup plugins for paths
	transferPlugin, pErr, err := getTransferPlugin(transferID, log, pluginData)
	if err != nil {
		return plugin.PluginErrors{
			Errors: []*plugin.FTAPathError{
				{
					PErr:       pErr,
					ErrMessage: err,
				},
			},
		}
	}

	destInfo, err := em.GetDestInfo(it)
	if err != nil {
		return plugin.PluginErrors{
			Errors: []*plugin.FTAPathError{
				{
					PErr:       proto.Error_ERROR_CONDUIT_INTERNAL,
					ErrMessage: fmt.Errorf("failed to get destination information from etcd: %v", err),
				},
			},
		}
	}

	updater := NewUpdater(log, em, it)

	// run the transfer
	errs := transferPlugin.Transfer(transferID, pluginData, destInfo, action, updater.updateTransferProgress, updater.updateAction)

	return errs
}

// getTransferPlugin will determine what plugin gets used for the transfer
func getTransferPlugin(transferID uuid.UUID, log *logger.ConduitLogger, pluginData *plugin.PluginData) (plugin.ConduitFTAPlugin, proto.Error, error) {
	// create a map of all available source transfer plugins
	sourceTransferPluginStrings := []string{}
	for _, sppi := range pluginData.SourcePluginInfo {
		sourceTransferPluginStrings = append(sourceTransferPluginStrings, sppi.FSC.PluginStages.TransferSrc...)
	}

	// create a map of all available destination transfer plugins
	destinationTransferPluginStrings := []string{}
	for _, dppi := range pluginData.DestinationsPluginInfo {
		destinationTransferPluginStrings = append(destinationTransferPluginStrings, dppi.FSC.PluginStages.TransferDst...)
	}

	finalPluginStrings := removeDuplicates(getIntersection(sourceTransferPluginStrings, destinationTransferPluginStrings))
	if len(finalPluginStrings) == 0 {
		return nil, proto.Error_ERROR_INVALID_INPUT, fmt.Errorf("failed to find a transfer plugin that is compatible with all sources and the destination")
	}

	var transferPlugin plugin.ConduitFTAPlugin

	for _, fps := range finalPluginStrings {
		pathPlugin, ok := PluginMap[fps]
		if !ok {
			continue
		}

		pluginCaps := pathPlugin.Initialize(transferID, log)

		// verify plugins capabilities
		foundCap := false
		for _, c := range pluginCaps {
			if c == plugin.TRANSFER {
				foundCap = true
				break
			}
		}
		if !foundCap {
			continue
		}

		transferPlugin = pathPlugin
		break
	}

	if transferPlugin == nil {
		return nil, proto.Error_ERROR_INVALID_INPUT, fmt.Errorf("none of the found transfer plugins are capable of transfer or they don't exist: %v", finalPluginStrings)
	}

	return transferPlugin, proto.Error_ERROR_NONE, nil
}
