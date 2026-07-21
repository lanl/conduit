// Copyright 2026. Triad National Security, LLC. All rights reserved.

package transferworker

import (
	"fmt"
	"time"

	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/defaults"
	"github.com/spf13/viper"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// startSchedulerJob submits a job to scheduler
func (tm *TransferWorker) startSchedulerJob(t *proto.TransferDetails, user string, command proto.SchedulerCommand, successfulState proto.TransferState) (proto.Error, error) {
	// if this is a validation job, add the transfer as a user to etcd
	if command == proto.SchedulerCommand_VALIDATION {
		// create etcd user for transfer
		err := tm.em.AddTransferUser(t.GetTransferID())
		if err != nil {
			return proto.Error_ERROR_ETCD_INTERNAL, fmt.Errorf("worker[%s]: error adding user[%s] to etcd: %v", tm.id, t.GetTransferID(), err)
		}
	}

	// check if the jobs key exists
	comparisons := []clientv3.Cmp{}
	comparisons = append(comparisons, clientv3.Compare(clientv3.CreateRevision(t.ETCDJobsKey()), "=", int64(0)))
	actions := []clientv3.Op{}
	actions = append(actions, clientv3.OpPut(t.ETCDJobsKey(), command.String()))

	// check that we aren't in an error state
	comparisons = append(comparisons, clientv3.Compare(clientv3.Value(t.ETCDErrorKey()), "=", proto.Error_ERROR_NONE.String()))

	// add a start time if it's a setup job
	switch command {
	case proto.SchedulerCommand_SETUP:
		actions = append(actions, clientv3.OpPut(t.ETCDStartTimeKey(), timestamppb.Now().AsTime().Format(time.RFC3339)))
		fallthrough
	default:
		// set the etcd state key to the successful state (typically this is a "submitted" state)
		actions = append(actions, clientv3.OpPut(t.ETCDStateKey(), successfulState.String()))
	}

	// increment the expiry time in the transfer
	expiry := time.Now().Add(viper.GetDuration(defaults.ConfigExpiryAdvanceKey))
	actions = append(actions, clientv3.OpPut(t.ETCDExpiryKey(), expiry.Format(time.RFC3339)))

	// SEND IT
	resp, err := tm.em.RetryTxn(&comparisons, &actions, defaults.MaxRetries, defaults.RetryDelay)
	if err != nil {
		return proto.Error_ERROR_ETCD_CONNECTION, fmt.Errorf("failed to submit [%s] job for transfer[%v]: error while adding job to etcd: %v", command, t.GetTransferID(), err)
	}
	if !resp.Succeeded {
		return proto.Error_ERROR_CONDUIT_INTERNAL, fmt.Errorf("failed to submit [%s] job for transfer[%v]: failed to add job to etcd. Does the key already exist?", command, t.GetTransferID())
	}

	tm.log.Debugf("successfully added transfer[%s] to etcd as %s", t.GetTransferID(), successfulState.String())

	return proto.Error_ERROR_NONE, nil
}
