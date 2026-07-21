// Copyright 2026. Triad National Security, LLC. All rights reserved.

package fta

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"

	conduitproto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/defaults"
	"github.com/lanl/conduit/internal/etcd"
	"github.com/lanl/conduit/internal/logger"
	"github.com/spf13/viper"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Updater struct {
	log     *logger.ConduitLogger
	em      *etcd.ETCDManager
	it      conduitproto.IncompleteTransfer
	esd     *conduitproto.ETCDStatusDetails
	pdMutex sync.Mutex
}

func NewUpdater(log *logger.ConduitLogger, em *etcd.ETCDManager, transfer conduitproto.IncompleteTransfer) *Updater {
	// get the transfers current status details
	esd, err := em.GetStatusDetails(transfer)
	if err != nil {
		log.Errorf("updater failed to get initial status details from etcd")
	}

	return &Updater{
		log: log,
		em:  em,
		it:  transfer,
		esd: esd,
	}
}

// updateProgress will send updates to etcd based off the pftool progress
func (u *Updater) updateTransferProgress(esd conduitproto.ETCDStatusDetails) error {
	u.pdMutex.Lock()
	defer u.pdMutex.Unlock()

	txnCompare := []clientv3.Cmp{
		clientv3.Compare(clientv3.Value(u.it.ETCDActiveKey()), "=", strconv.FormatBool(true)),
	}

	txnActions := []clientv3.Op{}

	// get record of current status details
	oldJson, err := json.Marshal(u.esd)
	if err != nil {
		return fmt.Errorf("failed to marshal current status details: %v", err)
	}

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

	newJson, err := json.Marshal(u.esd)
	if err != nil {
		return fmt.Errorf("failed to marshal status details: %v", err)
	}

	// check if old and new bytes are the same
	if bytes.Equal(oldJson, newJson) {
		// don't update etcd if details are the same
		return nil
	}

	txnActions = append(txnActions, clientv3.OpPut(u.it.ETCDStatusDetailsKey(), string(newJson)))
	retryCount := viper.GetInt(defaults.ConfigFTAVerifyRetryCountKey)
	sleepDur := viper.GetDuration(defaults.ConfigFTAVerifySleepDurationKey)

	resp, err := u.em.RetryTxn(&txnCompare, &txnActions, retryCount, sleepDur)
	if err != nil || !resp.Succeeded {
		return fmt.Errorf("failed to update transfer progress in etcd: %v %v", err, resp.Succeeded)
	}
	return nil
}

// updateProgress will send updates to etcd based off the pftool progress
func (u *Updater) updateAction(currentAction string, newAction string) error {
	txnCompare := []clientv3.Cmp{
		clientv3.Compare(clientv3.Value(u.it.ETCDActionKey()), "=", currentAction),
	}

	txnActions := []clientv3.Op{
		clientv3.OpPut(u.it.ETCDActionKey(), newAction),
	}

	retryCount := viper.GetInt(defaults.ConfigFTAVerifyRetryCountKey)
	sleepDur := viper.GetDuration(defaults.ConfigFTAVerifySleepDurationKey)

	resp, err := u.em.RetryTxn(&txnCompare, &txnActions, retryCount, sleepDur)
	if err != nil || !resp.Succeeded {
		return fmt.Errorf("failed to set new action in etcd: %v", err)
	}

	return nil
}
