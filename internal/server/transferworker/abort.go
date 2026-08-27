// Copyright 2026. Triad National Security, LLC. All rights reserved.

package transferworker

import (
	"strconv"
	"time"

	"github.com/google/uuid"
	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/defaults"
	"github.com/spf13/viper"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// handleTransferAbort sets all leases to error states to stop them from continuing
func (tw *TransferWorker) handleTransferAbort(it proto.IncompleteTransfer, eventID uuid.UUID) {
	defer tw.removeJob(eventID)

	// set transfer state to aborted
	comparisons := []clientv3.Cmp{}
	comparisons = append(comparisons, clientv3.Compare(clientv3.Value(it.ETCDStateKey()), "!=", proto.TransferState_TRANSFER_ERROR.String()))
	comparisons = append(comparisons, clientv3.Compare(clientv3.Value(it.ETCDStateKey()), "!=", proto.TransferState_TRANSFER_ABORTED.String()))
	actions := []clientv3.Op{}
	actions = append(actions, clientv3.OpPut(it.ETCDStateKey(), proto.TransferState_TRANSFER_ABORTED.String()))

	resp, err := tw.em.RetryTxn(&comparisons, &actions, defaults.MaxRetries, defaults.RetryDelay)
	if err != nil {
		tw.log.Errorf("error committing to etcd for transfer[%s]: %v", it.GetTransferID(), err)
		err := tw.em.CompleteTransfer(it)
		if err != nil {
			tw.log.Error(err)
		}
		return
	}
	if !resp.Succeeded {
		tw.log.Warnf("failed to add aborted state to transfer[%v], it was already set to error or aborted state", it.GetTransferID())
		err := tw.em.CompleteTransfer(it)
		if err != nil {
			tw.log.Error(err)
		}
		return
	}

	// get the full transfer details
	tid, err := uuid.Parse(it.GetTransferID())
	if err != nil {
		tw.log.Errorf("failed to parse uuid from [%s]: %v", it.GetTransferID(), err)
	}

	t, _, err := tw.em.GetTransfer(tid)
	if err != nil {
		tw.log.Errorf("failed to get transfer[%s] from etcd: %v", it.GetTransferID(), err)
		err := tw.em.CompleteTransfer(it)
		if err != nil {
			tw.log.Error(err)
		}
		return
	}

	// check if this happened before setup
	// go through each lease and see if any of them made it to the setup stage. If they didn't then it didn't get past validation
	foundPastValidation := false
	if t.GetSchedulerNodes().GetSetup() != "" {
		foundPastValidation = true
	}

	if !foundPastValidation {
		tw.log.Debugf("no leases for transfer[%v] made it to the setup stage", it.GetTransferID())

		comparisons := []clientv3.Cmp{}
		actions := []clientv3.Op{}
		comparisons = append(comparisons, clientv3.Compare(clientv3.Value(it.ETCDSchedulerNodesKey(proto.SchedulerCommand_SETUP)), "=", ""))

		newExpiry := timestamppb.New(time.Now().Add(viper.GetDuration(defaults.ConfigExpiryAdvanceKey)))

		actions = append(actions, clientv3.OpPut(it.ETCDActiveKey(), strconv.FormatBool(false)))
		actions = append(actions, clientv3.OpPut(it.ETCDArchiveStateKey(), proto.ArchiveState_ARCHIVE_READY.String()))
		actions = append(actions, clientv3.OpDelete(it.ETCDLeaseListKey(), clientv3.WithPrefix()))
		actions = append(actions, clientv3.OpPut(it.ETCDExpiryKey(), newExpiry.AsTime().Format(time.RFC3339)))

		resp, err := tw.em.RetryTxn(&comparisons, &actions, defaults.MaxRetries, defaults.RetryDelay)
		if err != nil || !resp.Succeeded {
			tw.log.Errorf("failed to set transfer[%s] to inactive because there was a scheduler setup job found for a lease: %v", it.GetTransferID(), err)
		} else {
			tw.log.Infof("successfully set transfer[%s] to inactive", it.GetTransferID())
			return
		}
	}
}
