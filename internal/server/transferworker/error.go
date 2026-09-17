// Copyright 2026. Triad National Security, LLC. All rights reserved.

package transferworker

import (
	"strconv"
	"time"

	"github.com/google/uuid"
	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/defaults"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// handleLeaseError sets the transfer status to error and cancels all other scheduler jobs for this transfer
func (tw *TransferWorker) handleTransferError(it proto.IncompleteTransfer, eventID uuid.UUID) {
	defer tw.removeJob(eventID)

	tid, err := uuid.Parse(it.GetTransferID())
	if err != nil {
		tw.log.Errorf("failed to parse transfer ID from [%s]: %v", it.GetTransferID(), err)
		return
	}

	comparisons := []clientv3.Cmp{
		clientv3.Compare(clientv3.Value(it.ETCDStateKey()), "!=", proto.TransferState_TRANSFER_ERROR.String()),
		clientv3.Compare(clientv3.Value(it.ETCDStateKey()), "!=", proto.TransferState_TRANSFER_ABORT.String()),
		clientv3.Compare(clientv3.Value(it.ETCDStateKey()), "!=", proto.TransferState_TRANSFER_ABORTED.String()),
		clientv3.Compare(clientv3.Value(it.ETCDActiveKey()), "=", strconv.FormatBool(true)),
	}

	actions := []clientv3.Op{
		clientv3.OpPut(it.ETCDStateKey(), proto.TransferState_TRANSFER_ERROR.String()),
		clientv3.OpDelete(it.ETCDJobsKey(), clientv3.WithPrefix()),
	}

	elses := []clientv3.Op{
		clientv3.OpGet(it.ETCDStateKey()),
		clientv3.OpGet(it.ETCDActiveKey()),
	}

	resp, err := tw.em.RetryTxn(&comparisons, &actions, &elses, defaults.MaxRetries, defaults.RetryDelay)
	if err != nil {
		tw.log.Errorf("failed to set transfer[%s] to error: %v", it.GetTransferID(), err)
		return
	}

	if !resp.Succeeded {
		if len(resp.Responses) < 2 {
			tw.log.Errorf("failed to inspect transfer[%s] after error state comparison failed", it.GetTransferID())
			return
		}

		stateResp := resp.Responses[0].GetResponseRange()
		activeResp := resp.Responses[1].GetResponseRange()

		if activeResp != nil && len(activeResp.Kvs) > 0 {
			active, err := strconv.ParseBool(string(activeResp.Kvs[0].Value))
			if err == nil && !active {
				return
			}
		}

		if stateResp == nil || len(stateResp.Kvs) == 0 {
			tw.log.Errorf("transfer[%s] state does not exist", it.GetTransferID())
			return
		}

		switch string(stateResp.Kvs[0].Value) {
		case proto.TransferState_TRANSFER_ERROR.String():
			// Another worker already changed the state.
			// Continue with cleanup.

		case proto.TransferState_TRANSFER_ABORT.String(), proto.TransferState_TRANSFER_ABORTED.String():
			// Abort path owns cleanup.
			return

		default:
			tw.log.Warnf("failed to set transfer[%s] to error; current state is [%s]", it.GetTransferID(), string(stateResp.Kvs[0].Value))
			return
		}
	}

	// remove from the schedulers
	tw.waitForTransferToStop(tid)

	if err := tw.em.CompleteTransfer(it); err != nil {
		tw.log.Errorf("failed to complete errored transfer[%s]: %v", it.GetTransferID(), err)
	}
}

func (tw *TransferWorker) waitForTransferToStop(tid uuid.UUID) bool {
	for _, s := range tw.schedulers {
		if err := s.RemoveTransfer(tid); err != nil {
			tw.log.Errorf("failed to remove transfer[%s] from scheduler[%s]: %v", tid, s.GetSchedulerID(), err)
		}
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		running := false

		for _, s := range tw.schedulers {
			if s.IsTransferRunning(tid) {
				running = true
				break
			}
		}

		if !running {
			// Clean up anything that may have been requeued while a
			// scheduler was finishing an in-flight dispatch.
			for _, s := range tw.schedulers {
				if err := s.RemoveTransfer(tid); err != nil {
					tw.log.Errorf("failed to remove transfer[%s] from scheduler[%s]: %v", tid, s.GetSchedulerID(), err)
				}
			}

			return true
		}

		tw.sMutex.Lock()
		state := tw.state
		tw.sMutex.Unlock()

		if state == proto.ServerState_SERVER_STOPPING || state == proto.ServerState_SERVER_STOPPED {
			tw.log.Debugf("stopped waiting for transfer[%s] during worker shutdown", tid)
			return false
		}

		<-ticker.C
	}
}
