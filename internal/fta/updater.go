// Copyright 2026. Triad National Security, LLC. All rights reserved.

package fta

import (
	"context"
	"fmt"
	"sync"

	conduitproto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/internal/logger"
	"google.golang.org/protobuf/proto"
)

type Updater struct {
	log     *logger.ConduitLogger
	client  *FTAClient
	esd     *conduitproto.ETCDStatusDetails
	pdMutex sync.Mutex
}

func NewUpdater(log *logger.ConduitLogger, client *FTAClient, transfer *conduitproto.TransferDetails) *Updater {
	esd := transfer.ETCDStatusDetails()

	return &Updater{
		log:    log,
		client: client,
		esd:    esd,
	}
}

// updateProgress will send updates to etcd based off the pftool progress
func (u *Updater) updateTransferProgress(esd *conduitproto.ETCDStatusDetails) error {
	u.pdMutex.Lock()
	defer u.pdMutex.Unlock()

	// get record of current status details
	oldESD := proto.Clone(u.esd)

	if esd.Data != "" {
		u.esd.Data = esd.Data
	}
	if esd.Bandwidth != "" {
		u.esd.Bandwidth = esd.Bandwidth
	}
	if esd.FilesChunks >= u.esd.FilesChunks {
		u.esd.FilesChunks = esd.FilesChunks
	}
	if esd.Directories >= u.esd.Directories {
		u.esd.Directories = esd.Directories
	}
	if esd.Files >= u.esd.Files {
		u.esd.Files = esd.Files
	}
	if esd.PluginStatus != "" {
		u.esd.PluginStatus = esd.PluginStatus
	}

	// check if old and new bytes are the same
	if proto.Equal(oldESD, u.esd) {
		// don't update etcd if details are the same
		return nil
	}

	ctx := context.Context(context.Background())
	_, err := u.client.api.UpdateStatus(ctx, u.esd)
	if err != nil {
		return fmt.Errorf("failed to update transfer progress in etcd: %v", err)
	}

	return nil
}

// updateProgress will send updates to etcd based off the pftool progress
func (u *Updater) updateAction(currentAction string, newAction string) error {
	ctx := context.Context(context.Background())
	_, err := u.client.api.UpdateAction(ctx, &conduitproto.FTAActionUpdate{
		CurrentAction: currentAction,
		NewAction:     newAction,
	})
	if err != nil {
		return fmt.Errorf("failed to set new action in etcd: %v", err)
	}

	return nil
}
