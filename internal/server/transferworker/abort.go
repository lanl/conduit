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

// handleTransferAbort waits for all scheduler/runner work for an aborted
// transfer to stop, then marks the transfer aborted and releases its runtime resources
func (tw *TransferWorker) handleTransferAbort(it proto.IncompleteTransfer, eventID uuid.UUID) {
	defer tw.removeJob(eventID)

	tid, err := uuid.Parse(it.GetTransferID())
	if err != nil {
		tw.log.Errorf("failed to parse transfer ID from [%v]: %v", it.GetTransferID(), err)
		return
	}

	// remove from the schedulers
	tw.waitForTransferToStop(tid)

	newExpiry := timestamppb.New(
		time.Now().Add(viper.GetDuration(defaults.ConfigExpiryAdvanceKey)),
	)
	endTime := timestamppb.Now()

	comparisons := []clientv3.Cmp{
		// Only consume an actual abort request.
		clientv3.Compare(clientv3.Value(it.ETCDStateKey()), "=", proto.TransferState_TRANSFER_ABORT.String()),

		// AbortTransfer sets this atomically with TRANSFER_ABORT.
		clientv3.Compare(clientv3.Value(it.ETCDErrorKey()), "=", proto.Error_ERROR_ABORTED.String()),

		// There is nothing for this worker to do if completion has
		// already made the transfer inactive.
		clientv3.Compare(clientv3.Value(it.ETCDActiveKey()), "=", strconv.FormatBool(true)),
	}

	actions := []clientv3.Op{
		clientv3.OpPut(it.ETCDStateKey(), proto.TransferState_TRANSFER_ABORTED.String()),
		clientv3.OpPut(it.ETCDActiveKey(), strconv.FormatBool(false)),
		clientv3.OpDelete(it.ETCDLeaseListKey(), clientv3.WithPrefix()),
		clientv3.OpDelete(it.ETCDJobsKey(), clientv3.WithPrefix()),

		clientv3.OpTxn(
			[]clientv3.Cmp{
				clientv3.Compare(clientv3.Value(it.ETCDArchiveStateKey()), "=", proto.ArchiveState_ARCHIVE_NONE.String()),
			},
			[]clientv3.Op{
				clientv3.OpPut(it.ETCDArchiveStateKey(), proto.ArchiveState_ARCHIVE_READY.String()),
				clientv3.OpPut(it.ETCDExpiryKey(), newExpiry.AsTime().Format(time.RFC3339)),
			},
			nil,
		),

		clientv3.OpTxn(
			[]clientv3.Cmp{
				clientv3.Compare(clientv3.Value(it.ETCDEndTimeKey()), "=", time.Unix(0, 0).UTC().Format(time.RFC3339)),
			},
			[]clientv3.Op{
				clientv3.OpPut(it.ETCDEndTimeKey(), endTime.AsTime().Format(time.RFC3339)),
			},
			nil,
		),
	}

	// If the abort can no longer be processed, get enough state to
	// distinguish an idempotent/stale watch event from an unexpected
	// transition.
	elses := []clientv3.Op{
		clientv3.OpGet(it.ETCDStateKey()),
		clientv3.OpGet(it.ETCDActiveKey()),
		clientv3.OpGet(it.ETCDErrorKey()),
	}

	resp, err := tw.em.RetryTxn(&comparisons, &actions, &elses, defaults.MaxRetries, defaults.RetryDelay)
	if err != nil {
		tw.log.Errorf("failed to handle abort for transfer[%s]: %v", it.GetTransferID(), err)
		return
	}

	if resp.Succeeded {
		tw.log.Infof("successfully aborted transfer[%s]", it.GetTransferID())
		return
	}

	// The watch event may simply be stale because another worker already
	// processed it. Don't turn that into another state transition.
	if len(resp.Responses) < 3 {
		tw.log.Errorf("failed to handle abort for transfer[%s]: etcd did not return current state", it.GetTransferID())
		return
	}

	stateResp := resp.Responses[0].GetResponseRange()
	activeResp := resp.Responses[1].GetResponseRange()
	errorResp := resp.Responses[2].GetResponseRange()

	if stateResp == nil || len(stateResp.Kvs) == 0 {
		tw.log.Errorf("failed to handle abort for transfer[%s]: state key does not exist", it.GetTransferID())
		return
	}

	currentState := string(stateResp.Kvs[0].Value)

	if activeResp != nil && len(activeResp.Kvs) > 0 {
		active, err := strconv.ParseBool(string(activeResp.Kvs[0].Value))
		if err == nil && !active {
			tw.log.Debugf("ignoring abort event for inactive transfer[%s]", it.GetTransferID())
			return
		}
	}

	switch currentState {
	case proto.TransferState_TRANSFER_ABORTED.String():
		tw.log.Debugf("transfer[%s] was already aborted", it.GetTransferID())
		return

	case proto.TransferState_TRANSFER_ERROR.String():
		tw.log.Debugf("transfer[%s] entered error state while processing abort", it.GetTransferID())
		return
	}

	currentError := ""
	if errorResp != nil && len(errorResp.Kvs) > 0 {
		currentError = string(errorResp.Kvs[0].Value)
	}

	tw.log.Warnf("did not process abort for transfer[%s]: current state[%s] error[%s]", it.GetTransferID(), currentState, currentError)
}
