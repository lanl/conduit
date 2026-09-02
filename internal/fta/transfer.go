// Copyright 2026. Triad National Security, LLC. All rights reserved.

package fta

import (
	"bytes"
	"fmt"

	"github.com/google/uuid"
	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/internal/fta/plugin"
	"github.com/lanl/conduit/internal/logger"
)

func StartPluginTransfer(log *logger.ConduitLogger, t *proto.TransferDetails, client *FTAClient, nodeList string) *proto.FTAPluginErrors {
	transferID, err := uuid.Parse(t.GetTransferID())
	if err != nil {
		return &proto.FTAPluginErrors{
			Errors: []*proto.FTAPathError{
				{
					PErr:       proto.Error_ERROR_CONDUIT_INTERNAL,
					ErrMessage: fmt.Sprintf("failed to parse transfer id[%v]: %v", t.GetTransferID(), err),
				},
			},
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

	// get setup plugins for paths
	transferPlugin, pErr, err := getTransferPlugin(transferID, log, pluginData)
	if err != nil {
		return &proto.FTAPluginErrors{
			Errors: []*proto.FTAPathError{
				{
					PErr:       pErr,
					ErrMessage: err.Error(),
				},
			},
		}
	}

	updater := NewUpdater(log, client, t)

	// run the transfer
	errs := transferPlugin.Transfer(transferID, pluginData, t.GetDestInfo(), t.GetAction(), t.GetOptions(), updater.updateTransferProgress, updater.updateAction)

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
