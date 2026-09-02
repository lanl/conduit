// Copyright 2026. Triad National Security, LLC. All rights reserved.

package transferworker

import (
	"fmt"
	"strconv"
	"time"

	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/defaults"
	"github.com/lanl/conduit/internal/etcd"
	"github.com/spf13/viper"
	clientv3 "go.etcd.io/etcd/client/v3"
	goproto "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// startSchedulerJob submits a job to scheduler
func (tw *TransferWorker) startSchedulerJob(t proto.IncompleteTransfer, command proto.SchedulerCommand, successfulState proto.TransferState) (proto.Error, error) {
	// get priority and createdTime
	createdTime, priority, err := tw.getCreatedTimeAndPriority(t)
	if err != nil {
		return proto.Error_ERROR_ETCD_INTERNAL, fmt.Errorf("failed to get transfer[%s] created time and priority from etcd: %v", t.GetTransferID(), err)
	}

	schedulerJob := &proto.SchedulerJob{
		Command:     command,
		Priority:    priority,
		CreatedTime: createdTime,
	}

	jobValue, err := goproto.MarshalOptions{Deterministic: true}.Marshal(schedulerJob)
	if err != nil {
		return proto.Error_ERROR_CONDUIT_INTERNAL, fmt.Errorf("failed to marshal scheduler job: %v", err)
	}

	// check if the jobs key exists
	comparisons := []clientv3.Cmp{}
	comparisons = append(comparisons, clientv3.Compare(clientv3.CreateRevision(t.ETCDJobsKey()), "=", int64(0)))
	actions := []clientv3.Op{}
	actions = append(actions, clientv3.OpPut(t.ETCDJobsKey(), string(jobValue)))

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
	resp, err := tw.em.RetryTxn(&comparisons, &actions, nil, defaults.MaxRetries, defaults.RetryDelay)
	if err != nil {
		return proto.Error_ERROR_ETCD_CONNECTION, fmt.Errorf("failed to submit [%s] job for transfer[%v]: error while adding job to etcd: %v", command, t.GetTransferID(), err)
	}
	if !resp.Succeeded {
		return proto.Error_ERROR_CONDUIT_INTERNAL, fmt.Errorf("failed to submit [%s] job for transfer[%v]: failed to add job to etcd. Does the key already exist?", command, t.GetTransferID())
	}

	tw.log.Debugf("successfully added transfer[%s] to etcd as %s", t.GetTransferID(), successfulState.String())

	return proto.Error_ERROR_NONE, nil
}

func (tw *TransferWorker) getCreatedTimeAndPriority(it proto.IncompleteTransfer) (createdTime *timestamppb.Timestamp, priority uint32, err error) {
	txnActions := []clientv3.Op{
		clientv3.OpGet(it.ETCDCreatedTimeKey()),
		clientv3.OpGet(it.ETCDPriorityKey()),
	}

	resp, err := tw.em.RetryTxn(nil, &txnActions, nil, defaults.MaxRetries, defaults.RetryDelay)
	if err != nil {
		return nil, 0, err
	}

	if len(resp.Responses) < 2 {
		return nil, 0, etcd.ErrNotFound
	}

	for i, r := range resp.Responses {
		rr := r.GetResponseRange()

		switch i {
		case 0:
			if len(rr.Kvs) < 1 {
				return nil, 0, fmt.Errorf("etcd didn't return a created time")
			}

			etcdCreatedTime, err := time.Parse(time.RFC3339, string(rr.Kvs[0].Value))
			if err != nil {
				return nil, 0, fmt.Errorf("failed to parse createdTime: %v", err)
			}
			createdTime = timestamppb.New(etcdCreatedTime)
		case 1:
			if len(rr.Kvs) < 1 {
				return nil, 0, fmt.Errorf("etcd didn't return a priority")
			}

			etcdPriority, err := strconv.Atoi(string(rr.Kvs[0].Value))
			if err != nil {
				return nil, 0, fmt.Errorf("failed to parse priority: %v", err)
			}
			priority = uint32(etcdPriority)
		}
	}

	return createdTime, priority, nil

}
