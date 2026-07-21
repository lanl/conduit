// Copyright 2026. Triad National Security, LLC. All rights reserved.

package transferworker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/defaults"
	"github.com/lanl/conduit/internal/etcd"
	"github.com/lanl/conduit/internal/logger"
	cert "github.com/lanl/conduit/internal/pki"
	"github.com/lanl/conduit/util"
	"github.com/spf13/viper"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type TransferWorker struct {
	id  uuid.UUID
	log *logger.ConduitLogger
	em  *etcd.ETCDManager
	cm  *cert.CertManager

	jobs   map[uuid.UUID]bool // the jobs map is only used for stopping and keeps track of the events that the transfer worker is actively handling
	jMutex sync.Mutex         // lock for jobs map

	leaseWait map[uuid.UUID]chan bool // a map of all transfers that are waiting for a conflicting lease to finish
	lwMutex   sync.Mutex

	stopWatchChan chan bool
	state         proto.ServerState
	sMutex        sync.Mutex // lock for transfer worker state
}

func NewTransferWorker(log *logger.ConduitLogger, cm *cert.CertManager, em *etcd.ETCDManager) *TransferWorker {
	id := uuid.New()

	// change prefix for logger
	l := logger.NewConduitLogger(log.GetLevel(), fmt.Sprintf("worker[%s]:", id))

	tw := &TransferWorker{
		id:        id,
		log:       l,
		cm:        cm,
		em:        em,
		jobs:      make(map[uuid.UUID]bool),
		leaseWait: make(map[uuid.UUID]chan bool),
		state:     proto.ServerState_SERVER_STARTING,
	}

	return tw
}

func (tw *TransferWorker) StartTransferWorker() error {
	// start watching for new transfers to appear
	successChan := make(chan bool)
	stopChan := make(chan bool)
	go tw.watchTransfers(successChan, stopChan)
	<-successChan
	tw.log.Infof("Started!")

	tw.stopWatchChan = stopChan

	tw.sMutex.Lock()
	tw.state = proto.ServerState_SERVER_RUNNING
	tw.sMutex.Unlock()

	return nil
}

func (tw *TransferWorker) StopTransferWorker() error {
	// check that the transfer worker is in a running state
	tw.sMutex.Lock()
	state := tw.state

	if state == proto.ServerState_SERVER_RUNNING || state == proto.ServerState_SERVER_DRAINING {
		tw.state = proto.ServerState_SERVER_STOPPING
	} else {
		tw.sMutex.Unlock()
		return fmt.Errorf("could not stop transfer worker[%v] because it is not in the running or drained state: %v", tw.id, state)
	}
	tw.sMutex.Unlock()

	tw.log.Info("stopping transfer worker")

	// stop watching transfers from etcd
	tw.stopWatchChan <- true

	// stop any lease waits
	tw.lwMutex.Lock()
	for lwid, lwChan := range tw.leaseWait {
		tw.log.Debugf("stopping lease wait[%s]", lwid)
		lwChan <- true
	}
	tw.lwMutex.Unlock()

	tw.log.Info("all lease waits have been stopped")

	// check to see if all the jobs are stopped
	jobsStopped := false
	jobCount := 0
	for !jobsStopped {
		tw.jMutex.Lock()
		numJobs := len(tw.jobs)
		tw.jMutex.Unlock()

		if numJobs == 0 {
			jobsStopped = true
		}

		if !jobsStopped && jobCount != numJobs {
			tw.log.Debugf("waiting for %v jobs to complete", numJobs)
			jobCount = numJobs
		}
		if !jobsStopped {
			time.Sleep(100 * time.Millisecond)
		}
	}

	tw.log.Info("all worker jobs are complete")

	tw.sMutex.Lock()
	tw.state = proto.ServerState_SERVER_STOPPED
	tw.sMutex.Unlock()

	return nil
}

// DrainTransferWorker will make the transfer worker continue transfers but not progress new ones
func (tw *TransferWorker) DrainTransferWorker() error {
	// check that the transfer worker is in a running state
	tw.sMutex.Lock()
	state := tw.state

	if state == proto.ServerState_SERVER_RUNNING {
		tw.state = proto.ServerState_SERVER_DRAINING
	} else {
		tw.sMutex.Unlock()
		return fmt.Errorf("could not drain transfer worker[%v] because it is not in the running state: %v", tw.id, state)
	}
	tw.sMutex.Unlock()

	tw.log.Info("transfer worker set to draining")

	// revert any transfers waiting for a lease back to init
	tw.lwMutex.Lock()
	for lwid, lwChan := range tw.leaseWait {
		tw.log.Debugf("stopping lease wait[%s]", lwid)
		lwChan <- true
	}
	tw.lwMutex.Unlock()

	return nil
}

func (tw *TransferWorker) watchTransfers(successChan chan bool, stopChan chan bool) {
	wc := tw.em.SubscribeToTransfers(tw.id)
	successChan <- true

	defer tw.em.UnsubscribeFromTransfers(tw.id)

	for {
		select {
		case wresp, ok := <-wc:
			if !ok {
				tw.log.Errorf("transfer watch channel closed unexpectedly")
				return
			}
			go tw.handleTransferEvents(wresp.Events)
			if wresp.Canceled {
				tw.log.Errorf("received cancel message from watch stream: %+v", wresp)
			}
		case <-stopChan:
			tw.log.Infof("stopped watching transfer events")
			return
		}
	}
}

// handleTransferEvents gets called whenever an event is passed to the transfer watch channel
func (tw *TransferWorker) handleTransferEvents(evs []*clientv3.Event) {
	for _, ev := range evs {
		eventID := uuid.New()
		tw.jMutex.Lock()
		tw.jobs[eventID] = true
		tw.jMutex.Unlock()

		// skip any delete events
		if ev.Type == mvccpb.DELETE {
			tw.removeJob(eventID)
			continue
		}
		// check if it's a lease event
		id, _, err := proto.ParseETCDTransfersKey(string(ev.Kv.Key))
		if err == nil {
			t := proto.IncompleteTransfer(&proto.TransferDetails{TransferID: id.String()})
			switch string(ev.Kv.Key) {
			case t.ETCDPausedStateKey():
				go func(ev *clientv3.Event, eID uuid.UUID) {
					defer tw.removeJob(eID)

					if ev.PrevKv != nil && viper.GetBool(defaults.ConfigTestKey) {
						tw.log.Debugf("pause state updated for transfer[%s] from [%s] to [%s]", t.GetTransferID(), string(ev.PrevKv.Value), string(ev.Kv.Value))
						// convert paused state in etcd to a proto.transferstate
						if pps, ok := proto.TransferState_value[string(ev.PrevKv.Value)]; !ok {
							tw.log.Error("failed to convert previous paused state to a transfer state: %v", string(ev.PrevKv.Value))
						} else {
							tw.progressPausedTransfer(t, proto.TransferState(pps))
						}
					}
				}(ev, eventID)
			case t.ETCDErrorKey():
				if string(ev.Kv.Value) != proto.Error_ERROR_NONE.String() {
					go tw.handleTransferError(t, eventID)
					continue
				}
			case t.ETCDStateKey():
				switch string(ev.Kv.Value) {
				case proto.TransferState_TRANSFER_ABORT.String():
					go tw.handleTransferAbort(t, eventID)
					continue
				case proto.TransferState_TRANSFER_INIT_COMPLETE.String():
					go tw.handleStateUpdate(t, proto.TransferState_TRANSFER_INIT_COMPLETE, proto.TransferState_TRANSFER_VALIDATION_READY, proto.SchedulerCommand_VALIDATION, proto.TransferState_TRANSFER_VALIDATION_SUBMITTED, eventID)
					continue
				case proto.TransferState_TRANSFER_VALIDATION_COMPLETE.String():
					go tw.verifyValidationComplete(t, eventID)
					continue
				case proto.TransferState_TRANSFER_LEASE_ACQUIRED.String():
					go tw.handleStateUpdate(t, proto.TransferState_TRANSFER_LEASE_ACQUIRED, proto.TransferState_TRANSFER_SETUP_READY, proto.SchedulerCommand_SETUP, proto.TransferState_TRANSFER_SETUP_SUBMITTED, eventID)
					continue
				case proto.TransferState_TRANSFER_SETUP_COMPLETE.String():
					go tw.handleStateUpdate(t, proto.TransferState_TRANSFER_SETUP_COMPLETE, proto.TransferState_TRANSFER_DATA_READY, proto.SchedulerCommand_TRANSFER, proto.TransferState_TRANSFER_DATA_SUBMITTED, eventID)
					continue
				case proto.TransferState_TRANSFER_DATA_COMPLETE.String():
					go tw.handleStateUpdate(t, proto.TransferState_TRANSFER_DATA_COMPLETE, proto.TransferState_TRANSFER_TEARDOWN_READY, proto.SchedulerCommand_TEARDOWN, proto.TransferState_TRANSFER_TEARDOWN_SUBMITTED, eventID)
					continue
				case proto.TransferState_TRANSFER_TEARDOWN_COMPLETE.String():
					// all setup leases are complete
					go tw.verifyFinalized(t, eventID)
					continue
				default:
					tw.removeJob(eventID)
				}
			default:
				tw.removeJob(eventID)
			}
		}
		tw.removeJob(eventID)
	}
}

func (tw *TransferWorker) handleStateUpdate(it proto.IncompleteTransfer, fromState proto.TransferState, toState proto.TransferState, command proto.SchedulerCommand, successfulState proto.TransferState, eventID uuid.UUID) {
	defer tw.removeJob(eventID)

	// check for pause
	if tw.checkForTransferPause(it, fromState) {
		return
	}

	// check for pause
	if tw.checkForDrain(it, fromState) {
		return
	}

	// set the transfer state to tostate
	succeeded, pErr, err := tw.em.SafelySetTransferState(it, fromState, toState)
	if err != nil {
		tErr := fmt.Errorf("error committing new transfer state to etcd for transfer[%s]: %v", it.GetTransferID(), err)
		tw.log.Error(tErr)
		_, _, err := tw.em.SafelyAddErr(it, pErr, tErr)
		if err != nil {
			tw.log.Error(err)
		}
		return
	} else if !succeeded {
		tw.log.Warnf("failed to set transfer[%s] state to %v. Another worker probably took care of it", it.GetTransferID(), toState.String())
		return
	}
	tw.log.Infof("successfully set transfer[%s] state to %v", it.GetTransferID(), toState.String())

	// get a full transfer details from etcd
	tid, err := uuid.Parse(it.GetTransferID())
	if err != nil {
		tErr := fmt.Errorf("failed to parse transfer id from[%s]: %v", it.GetTransferID(), err)
		_, _, err := tw.em.SafelyAddErr(it, proto.Error_ERROR_CONDUIT_INTERNAL, tErr)
		if err != nil {
			tw.log.Error(err)
		}
		return
	}
	t, pErr, err := tw.em.GetTransfer(tid)
	if err != nil {
		tErr := fmt.Errorf("failed to get transfer[%s] from etcd: %v", it.GetTransferID(), err)
		_, _, err := tw.em.SafelyAddErr(it, pErr, tErr)
		if err != nil {
			tw.log.Error(err)
		}
		return
	}

	// submit scheduler job
	pErr, err = tw.startSchedulerJob(t, t.GetUser(), command, successfulState)
	if err != nil {
		_, _, err := tw.em.SafelyAddErr(t, pErr, err)
		if err != nil {
			tw.log.Error(err)
		}
		tw.log.Debugf("successfully set transfer[%s] error to %v", t.GetTransferID(), pErr.String())
		return
	}

	tw.log.Infof("successfully proceeded to transfer[%s] to %s", t.GetTransferID(), successfulState.String())

}

func (tw *TransferWorker) acquireLeases(it proto.IncompleteTransfer, ctx context.Context) error {
	// check for pause
	if tw.checkForTransferPause(it, proto.TransferState_TRANSFER_VALIDATION_COMPLETE) {
		return nil
	}

	// set transfer to WAITING_FOR_LEASE to show that this worker has taken it
	succeeded, pErr, err := tw.em.SafelySetTransferState(it, proto.TransferState_TRANSFER_VALIDATION_COMPLETE, proto.TransferState_TRANSFER_WAITING_FOR_LEASE)
	if err != nil {
		tw.log.Errorf("error committing new transfer state to etcd for transfer[%s]: %v", it.GetTransferID(), err)
		_, _, err := tw.em.SafelyAddErr(it, pErr, err)
		if err != nil {
			tw.log.Error(err)
		}
		return nil
	} else if !succeeded {
		tw.log.Warnf("failed to set transfer[%s] state to %v. Another worker probably took care of it", it.GetTransferID(), proto.TransferState_TRANSFER_WAITING_FOR_LEASE.String())
		return nil
	}
	tw.log.Infof("successfully set transfer[%s] state to %v", it.GetTransferID(), proto.TransferState_TRANSFER_WAITING_FOR_LEASE.String())

	// parse transfer id
	tid, err := uuid.Parse(it.GetTransferID())
	if err != nil {
		tErr := fmt.Errorf("failed to parse transfer id from[%s]: %v", it.GetTransferID(), err)
		_, _, err := tw.em.SafelyAddErr(it, proto.Error_ERROR_CONDUIT_INTERNAL, tErr)
		if err != nil {
			tw.log.Error(err)
		}
		return nil
	}
	// get full transfer from etcd
	t, pErr, err := tw.em.GetTransfer(tid)
	if err != nil {
		tErr := fmt.Errorf("failed to get transfer[%s] from etcd: %v", it.GetTransferID(), err)
		_, _, err := tw.em.SafelyAddErr(it, pErr, tErr)
		if err != nil {
			tw.log.Error(err)
		}
		return nil
	}

	// submit this transfers leases to the lease space in etcd
	leases := t.GetLeases()

	// create op for path list in leases prefix
	jsonPathList, err := json.Marshal(leases)
	if err != nil {
		tErr := fmt.Errorf("transfer[%s]: failed to marshal path list into json for transfer: %v", t.GetTransferID(), err)
		tw.log.Error(tErr)
		_, _, err := tw.em.SafelyAddErr(it, proto.Error_ERROR_CONDUIT_INTERNAL, fmt.Errorf("transfer[%s]: failed to marshal path list into json for transfer: %v", t.GetTransferID(), err))
		if err != nil {
			tw.log.Errorf("%s failed to error: %v", it.GetTransferID(), err)
		}
		return nil
	}

	// check to see if this transfer is already in the etcd lease list
	var rev int64
	getResp, err := tw.em.RetryGet(it.ETCDLeaseListKey(), defaults.MaxRetries, defaults.RetryDelay)
	if err != nil {
		tErr := fmt.Errorf("failed to get lease list from etcd for transfer[%s]: %v", it.GetTransferID(), err)
		tw.log.Error(tErr)
		_, _, err := tw.em.SafelyAddErr(it, proto.Error_ERROR_ETCD_CONNECTION, tErr)
		if err != nil {
			tw.log.Errorf("%s failed to error: %v", it.GetTransferID(), err)
		}
		return nil
	}

	if len(getResp.Kvs) > 0 {
		// use the revision of the lease that's already there
		rev = getResp.Kvs[0].CreateRevision
		tw.log.Debugf("transfer[%s] was already in the lease list. Using revision: %v ", tid, rev)
	} else {
		// add transfer to the lease list in etcd
		putResp, err := tw.em.RetryPut(it.ETCDLeaseListKey(), string(jsonPathList), defaults.MaxRetries, defaults.RetryDelay)
		if err != nil {
			tErr := fmt.Errorf("failed to put lease list into etcd for transfer[%s]: %v", it.GetTransferID(), err)
			tw.log.Error(tErr)
			_, _, err := tw.em.SafelyAddErr(it, proto.Error_ERROR_ETCD_CONNECTION, tErr)
			if err != nil {
				tw.log.Errorf("%s failed to error: %v", it.GetTransferID(), err)
			}
			return nil
		}

		tw.log.Debugf("transfer[%s] successfully added leases to etcd(%v): %v:%v", tid, putResp.Header.GetRevision(), it.ETCDLeaseListKey(), string(jsonPathList))

		// use the revision from when we added this transfers leases
		rev = putResp.Header.GetRevision()
	}

	// get all the other leases listed in the lease prefix area
	gresp, err := tw.em.RetryGetPrefixRev(proto.LeasePrefix, rev, defaults.MaxRetries, defaults.RetryDelay)
	if err != nil {
		tw.log.Errorf("failed to get all leases at rev[%v] to aquire lease for transfer[%s]: %v", rev, it.GetTransferID(), err)
		_, _, err := tw.em.SafelyAddErr(it, proto.Error_ERROR_ETCD_CONNECTION, fmt.Errorf("failed to get all leases at rev[%v] to aquire lease for transfer[%s]: %v", rev, it.GetTransferID(), err))
		if err != nil {
			tw.log.Errorf("%s failed to error: %v", it.GetTransferID(), err)
		}
		return nil
	}

	parsedLeases := tw.parseETCDLeases(gresp.Kvs)

	// this will blow up your logs if you have a lot of transfers at once
	// tw.log.Debugf("transfer[%s] found other leases: %+v", tid, parsedLeases)
	combinedLeases := []string{}
	combinedLeases = append(combinedLeases, leases.GetSource()...)
	combinedLeases = append(combinedLeases, leases.GetDestination()...)

	conflictingTransfers := make(map[uuid.UUID]bool)
	// check this transfer's sources and destinations don't conflict with the running destinations
	for _, lease := range combinedLeases {
		parents := findLeaseParents(lease, tid, parsedLeases, proto.LeaseType_DESTINATION)
		children := findLeaseChildren(lease, tid, parsedLeases, proto.LeaseType_DESTINATION)
		exacts := findLeaseExacts(lease, tid, parsedLeases, proto.LeaseType_DESTINATION)

		if len(parents) > 0 {
			tw.log.Debugf("transfer[%s] lease[%s]: parents: %v", tid, lease, parents)
		}
		if len(children) > 0 {
			tw.log.Debugf("transfer[%s] lease[%s]: children: %v", tid, lease, children)
		}
		if len(exacts) > 0 {
			tw.log.Debugf("transfer[%s] lease[%s]: exact: %v", tid, lease, exacts)
		}

		if len(parents) > 0 || len(children) > 0 || len(exacts) > 0 {
			for id := range parents {
				conflictingTransfers[id] = true
			}
			for id := range children {
				conflictingTransfers[id] = true
			}
			for id := range exacts {
				conflictingTransfers[id] = true
			}
		}
	}

	// check this transfer's destinations don't conflict with the running sources
	for _, lease := range leases.GetDestination() {
		parents := findLeaseParents(lease, tid, parsedLeases, proto.LeaseType_SOURCE)
		children := findLeaseChildren(lease, tid, parsedLeases, proto.LeaseType_SOURCE)
		exacts := findLeaseExacts(lease, tid, parsedLeases, proto.LeaseType_SOURCE)

		if len(parents) > 0 {
			tw.log.Debugf("transfer[%s] lease[%s]: parents: %v", tid, lease, parents)
		}
		if len(children) > 0 {
			tw.log.Debugf("transfer[%s] lease[%s]: children: %v", tid, lease, children)
		}
		if len(exacts) > 0 {
			tw.log.Debugf("transfer[%s] lease[%s]: exact: %v", tid, lease, exacts)
		}

		if len(parents) > 0 || len(children) > 0 || len(exacts) > 0 {
			for id := range parents {
				conflictingTransfers[id] = true
			}
			for id := range children {
				conflictingTransfers[id] = true
			}
			for id := range exacts {
				conflictingTransfers[id] = true
			}
		}
	}

	ct := []uuid.UUID{}
	for ctid, _ := range conflictingTransfers {
		ct = append(ct, ctid)
	}

	tw.log.Debugf("transfer[%s] waiting for conflicting transfers", tid)

	// start updating the expiry for the transfer
	updateExpiryStopChan := make(chan bool, 1)
	go tw.em.UpdateExpiryConstantly(t, updateExpiryStopChan)

	err = tw.em.WaitTransfersActive(ct, ctx)
	if err != nil {
		if err.Error() == context.Canceled.Error() {
			updateExpiryStopChan <- true
			return err
		}

		tErr := fmt.Errorf("failed to wait for conflicting transfers for transfer[%s]: %v", it.GetTransferID(), err)
		tw.log.Error(tErr)
		_, _, err := tw.em.SafelyAddErr(it, proto.Error_ERROR_CONDUIT_INTERNAL, tErr)
		if err != nil {
			tw.log.Errorf("%s failed to error: %v", it.GetTransferID(), err)
		}
		updateExpiryStopChan <- true
		return nil
	}

	// stop updating the expiry for the transfer
	updateExpiryStopChan <- true

	tw.log.Debugf("transfer[%s] done waiting for conflicting trasnfers", tid)

	// double check that all conflicting transfers are complete
	for _, ctid := range ct {
		ctit := proto.IncompleteTransfer(&proto.TransferDetails{TransferID: ctid.String()})
		active, err := tw.em.GetActive(ctit)
		if err != nil {
			if errors.Is(err, etcd.ErrNotFound) {
				continue
			}
			tErr := fmt.Errorf("error while double checking watched transfers for lease aquisition for transfer[%s] lease[%s]: %v ", it.GetTransferID(), ctid, err)
			tw.log.Error(tErr)
			_, _, err := tw.em.SafelyAddErr(it, proto.Error_ERROR_ETCD_CONNECTION, tErr)
			if err != nil {
				tw.log.Error(err)
			}
			return nil
		}
		if active {
			tErr := fmt.Errorf("etcd found an active transfer that we thought was complete, this shouldn't happen. Transfer[%s] conflicting Transfer[%s] watchedTransfers:[%v]", it.GetTransferID(), ctid, ct)
			tw.log.Error(tErr)
			_, _, err := tw.em.SafelyAddErr(it, proto.Error_ERROR_CONDUIT_INTERNAL, tErr)
			if err != nil {
				tw.log.Error(err)
			}
			return nil
		}
	}

	// all leases are done, set it to lease acquired, unless there was an error
	succeeded, pErr, err = tw.em.SafelySetTransferState(it, proto.TransferState_TRANSFER_WAITING_FOR_LEASE, proto.TransferState_TRANSFER_LEASE_ACQUIRED)
	if err != nil {
		tErr := fmt.Errorf("error while to set transfer[%s] to %s: %v", it.GetTransferID(), proto.TransferState_TRANSFER_LEASE_ACQUIRED.String(), err)
		tw.log.Error(tErr)
		_, _, err := tw.em.SafelyAddErr(it, pErr, tErr)
		if err != nil {
			tw.log.Error(err)
		}
		return nil
	}
	if !succeeded {
		tw.log.Warnf("failed to set transfer[%s] to %s. This might be because it was already in an error state or another worker took care of it", it.GetTransferID(), proto.TransferState_TRANSFER_LEASE_ACQUIRED.String())
		return nil
	}

	return nil
}

func (tw *TransferWorker) verifyFinalized(it proto.IncompleteTransfer, eventID uuid.UUID) {
	defer tw.removeJob(eventID)

	tw.log.Infof("transfer[%v] is ready to be finalized", it.GetTransferID())

	comparisons := []clientv3.Cmp{}
	actions := []clientv3.Op{}

	comparisons = append(comparisons, clientv3.Compare(clientv3.Value(it.ETCDStateKey()), "=", proto.TransferState_TRANSFER_TEARDOWN_COMPLETE.String()))
	comparisons = append(comparisons, clientv3.Compare(clientv3.Value(it.ETCDErrorKey()), "=", proto.Error_ERROR_NONE.String()))

	actions = append(actions, clientv3.OpPut(it.ETCDStateKey(), proto.TransferState_TRANSFER_FINALIZED.String()))
	actions = append(actions, clientv3.OpPut(it.ETCDActiveKey(), strconv.FormatBool(false)))
	actions = append(actions, clientv3.OpPut(it.ETCDEndTimeKey(), timestamppb.Now().AsTime().Format(time.RFC3339)))
	actions = append(actions, clientv3.OpDelete(it.ETCDLeaseListKey()))
	actions = append(actions, clientv3.OpPut(it.ETCDArchiveStateKey(), proto.ArchiveState_ARCHIVE_READY.String()))

	resp, err := tw.em.RetryTxn(&comparisons, &actions, defaults.MaxRetries, defaults.RetryDelay)
	if err != nil {
		tErr := fmt.Errorf("failed to set transfer[%s] to %v: %v", it.GetTransferID(), proto.TransferState_TRANSFER_FINALIZED.String(), err)
		// tErr := fmt.Errorf("error while to set transfer[%s] to %s: %v", it.GetTransferID(), proto.TransferState_TRANSFER_LEASE_ACQUIRED.String(), err)
		tw.log.Error(tErr)
		_, _, err := tw.em.SafelyAddErr(it, proto.Error_ERROR_ETCD_CONNECTION, tErr)
		if err != nil {
			tw.log.Error(err)
		}
		return
	}
	if !resp.Succeeded {
		tw.log.Warnf("failed to set transfer[%s] to %v. Another worker probably took care of it", it.GetTransferID(), proto.TransferState_TRANSFER_FINALIZED.String())
		return
	}

	tw.log.Infof("successfully set transfer[%s] to %v", it.GetTransferID(), proto.TransferState_TRANSFER_FINALIZED.String())
}

func (tw *TransferWorker) verifyValidationComplete(it proto.IncompleteTransfer, eventID uuid.UUID) {
	defer tw.removeJob(eventID)

	tw.lwMutex.Lock()
	stopChan := make(chan bool, 1)
	tw.leaseWait[eventID] = stopChan
	tw.lwMutex.Unlock()

	defer func() {
		tw.lwMutex.Lock()
		delete(tw.leaseWait, eventID)
		tw.lwMutex.Unlock()
	}()

	tw.log.Infof("transfer[%v] is done with validation. Checking for how to proceed", it.GetTransferID())

	// get a full transfer details from etcd
	tid, err := uuid.Parse(it.GetTransferID())
	if err != nil {
		tErr := fmt.Errorf("failed to parse transfer id from[%s]: %v", it.GetTransferID(), err)
		_, _, err := tw.em.SafelyAddErr(it, proto.Error_ERROR_CONDUIT_INTERNAL, tErr)
		if err != nil {
			tw.log.Error(err)
		}
		return
	}
	t, pErr, err := tw.em.GetTransfer(tid)
	if err != nil {
		tErr := fmt.Errorf("failed to get transfer[%s] from etcd: %v", it.GetTransferID(), err)
		_, _, err := tw.em.SafelyAddErr(it, pErr, tErr)
		if err != nil {
			tw.log.Error(err)
		}
		return
	}

	// check if this is stopping after validation. If it isn't, continue to acquire leases
	if !t.GetValidationOnly() {
		acquireLeaseDoneChan := make(chan error, 1)
		ctx, ctxCancel := context.WithCancel(context.Background())

		go func() {
			err := tw.acquireLeases(t, ctx)
			acquireLeaseDoneChan <- err
		}()

		// if we get a stop request while we're waiting for a lease, we want to rollback to validation complete
		select {
		case <-acquireLeaseDoneChan:
			ctxCancel()
		case <-stopChan:
			tw.log.Debugf("stopping acquire lease for transfer[%s] event[%s]", it.GetTransferID(), eventID)
			ctxCancel()
			err := tw.em.RollbackState(t, proto.TransferState_TRANSFER_WAITING_FOR_LEASE, proto.TransferState_TRANSFER_VALIDATION_COMPLETE, etcd.Transfer, nil)
			if err != nil {
				tw.log.Errorf("failed to rollback transfer from [%s] to [%s]: %v", proto.TransferState_TRANSFER_WAITING_FOR_LEASE, proto.TransferState_TRANSFER_VALIDATION_COMPLETE, err)
			}
		}

		return
	}

	// this is a validation only transfer
	// lets set active to false and be done with it
	comparisons := []clientv3.Cmp{}
	actions := []clientv3.Op{}

	comparisons = append(comparisons, clientv3.Compare(clientv3.Value(it.ETCDStateKey()), "=", proto.TransferState_TRANSFER_VALIDATION_COMPLETE.String()))
	comparisons = append(comparisons, clientv3.Compare(clientv3.Value(it.ETCDErrorKey()), "=", proto.Error_ERROR_NONE.String()))
	comparisons = append(comparisons, clientv3.Compare(clientv3.Value(it.ETCDValidationOnlyKey()), "=", strconv.FormatBool(true)))

	// actions = append(actions, clientv3.OpPut(it.ETCDStateKey(), proto.TransferState_TRANSFER_FINALIZED.String()))
	actions = append(actions, clientv3.OpPut(it.ETCDActiveKey(), strconv.FormatBool(false)))
	actions = append(actions, clientv3.OpPut(it.ETCDEndTimeKey(), timestamppb.Now().AsTime().Format(time.RFC3339)))
	actions = append(actions, clientv3.OpDelete(it.ETCDLeaseListKey()))
	actions = append(actions, clientv3.OpPut(it.ETCDArchiveStateKey(), proto.ArchiveState_ARCHIVE_READY.String()))

	resp, err := tw.em.RetryTxn(&comparisons, &actions, defaults.MaxRetries, defaults.RetryDelay)
	if err != nil {
		tErr := fmt.Errorf("failed to set transfer[%s] to inactive: %v", it.GetTransferID(), err)
		// tErr := fmt.Errorf("error while to set transfer[%s] to %s: %v", it.GetTransferID(), proto.TransferState_TRANSFER_LEASE_ACQUIRED.String(), err)
		tw.log.Error(tErr)
		_, _, err := tw.em.SafelyAddErr(it, proto.Error_ERROR_ETCD_CONNECTION, tErr)
		if err != nil {
			tw.log.Error(err)
		}
		return
	}
	if !resp.Succeeded {
		tw.log.Warnf("failed to set transfer[%s] to %v. Another worker probably took care of it", it.GetTransferID(), proto.TransferState_TRANSFER_FINALIZED.String())
		return
	}

	tw.log.Infof("successfully set transfer[%s] to %v", it.GetTransferID(), proto.TransferState_TRANSFER_FINALIZED.String())
}

// checkForPause checks if transfer needs to pause at the desired fromState
// returns variables:
// bool: if true, the transfer needs to pause now. If false, it can continue
func (tw *TransferWorker) checkForTransferPause(it proto.IncompleteTransfer, fromState proto.TransferState) bool {
	if viper.GetBool(defaults.ConfigTestKey) {
		resp, err := tw.em.RetryGet(it.ETCDPausedStateKey(), defaults.MaxRetries, defaults.RetryDelay)
		if err != nil || len(resp.Kvs) < 1 {
			tw.log.Errorf("failed to get pause state from etcd for transfer[%s]: %v", it.GetTransferID(), err)
			return false
		}

		if ps, ok := proto.TransferState_value[string(resp.Kvs[0].Value)]; !ok {
			tw.log.Errorf("failed to covert pause state from etcd to transferstate for transfer[%s]: %v", it.GetTransferID(), string(resp.Kvs[0].Value))
			return false
		} else {
			if proto.TransferState(ps) == fromState {
				tw.log.Warnf("transfer[%s] has pause state at [%v]. Transfer worker will not proceed to next state until pause state changes", it.GetTransferID(), proto.TransferState(ps).String())
				return true
			}
		}
	}

	return false
}

// checkForDrain checks if transfer worker is trying to drain and whether this transfer should continue or not.
// returns variables:
// bool: if true, the transfer should NOT progress. If false, it can continue
func (tw *TransferWorker) checkForDrain(it proto.IncompleteTransfer, fromState proto.TransferState) bool {
	if tw.state == proto.ServerState_SERVER_DRAINING && fromState == proto.TransferState_TRANSFER_INIT_COMPLETE {
		tw.log.Warnf("transfer worker is in a draining state and will not progress transfer[%s]", it.GetTransferID())
		return true
	}

	return false
}

// progressPausedTransfer will check if the old pausedstate is the same as any of the current lease states.
// If a lease state is the same, it will re-put into the etcd database to kick off the transfer worker process
func (tw *TransferWorker) progressPausedTransfer(it proto.IncompleteTransfer, oldPausedState proto.TransferState) {
	comparisons := []clientv3.Cmp{}
	actions := []clientv3.Op{}
	comparisons = append(comparisons, clientv3.Compare(clientv3.Value(it.ETCDStateKey()), "=", oldPausedState.String()))
	actions = append(actions, clientv3.OpPut(it.ETCDStateKey(), oldPausedState.String()))

	resp, err := tw.em.RetryTxn(&comparisons, &actions, defaults.MaxRetries, defaults.RetryDelay)
	if err != nil || !resp.Succeeded {
		tw.log.Errorf("failed to set transfer[%s] leases to progress paused state: %v", it.GetTransferID(), err)
		if err == nil {
			tw.log.Errorf("transfer[%s] compare failed %v != %v", it.GetTransferID(), it.ETCDStateKey(), oldPausedState.String())
		}
	} else {
		tw.log.Infof("successfully set transfer[%s] leases to progress paused state", it.GetTransferID())
	}
}

// parseETCDLeases returns a map of all transfers that have active leases in the "leases" space of ETCD
//
// returns map[<Transfer ID>] value: slice[<Lease Path>]
func (tw *TransferWorker) parseETCDLeases(kvs []*mvccpb.KeyValue) map[uuid.UUID]*proto.Leases {
	leases := make(map[uuid.UUID]*proto.Leases)

	for _, kv := range kvs {
		id, err := uuid.Parse(strings.TrimPrefix(string(kv.Key), proto.LeasePrefix))
		if err != nil {
			tw.log.Errorf("error parsing transfer id from etcd: %v", err)
		}
		pathList := &proto.Leases{}
		err = json.Unmarshal(kv.Value, pathList)
		if err != nil {
			tw.log.Errorf("error unmarshalling pathlist from etcd: %v", err)
		}
		leases[id] = pathList
	}
	return leases
}

// findLeaseParents returns all matching parents of a lease from a list of paths
func findLeaseParents(lease string, tid uuid.UUID, leases map[uuid.UUID]*proto.Leases, leaseType proto.LeaseType) map[uuid.UUID][]string {
	parents := make(map[uuid.UUID][]string)

	possibleParents := util.PathParents(lease)
	// em.log.Debugf("possible parents: %v", possibleParents)
	for id, l := range leases {
		var paths []string
		if leaseType == proto.LeaseType_SOURCE {
			paths = l.GetSource()
		} else {
			paths = l.GetDestination()
		}

		// make sure to exclude the lease we are matching against
		if id != tid {
			fp := util.FindParents(possibleParents, paths)
			if len(fp) > 0 {
				parents[id] = append(parents[id], fp...)
			}
		}
	}
	return parents
}

// findLeaseChildren returns all matching children from a list of paths
func findLeaseChildren(lease string, tid uuid.UUID, leases map[uuid.UUID]*proto.Leases, leaseType proto.LeaseType) map[uuid.UUID][]string {
	children := make(map[uuid.UUID][]string)

	for id, l := range leases {
		var paths []string
		if leaseType == proto.LeaseType_SOURCE {
			paths = l.GetSource()
		} else {
			paths = l.GetDestination()
		}

		// make sure to exclude the lease we are matching against
		if id != tid {
			fc := util.FindChildren(lease, paths)
			if len(fc) > 0 {
				children[id] = append(children[id], fc...)
			}
		}
	}

	return children
}

// findLeaseExacts returns all exact matches from a list of paths
func findLeaseExacts(lease string, tid uuid.UUID, leases map[uuid.UUID]*proto.Leases, leaseType proto.LeaseType) map[uuid.UUID][]string {
	exacts := make(map[uuid.UUID][]string)

	for id, l := range leases {
		var paths []string
		if leaseType == proto.LeaseType_SOURCE {
			paths = l.GetSource()
		} else {
			paths = l.GetDestination()
		}

		// make sure to exclude the lease we are matching against
		if id != tid {
			fe := util.FindExacts(lease, paths)
			if len(fe) > 0 {
				exacts[id] = append(exacts[id], fe...)
			}
		}
	}

	return exacts
}

func (tw *TransferWorker) removeJob(eventID uuid.UUID) {
	tw.jMutex.Lock()
	delete(tw.jobs, eventID)
	tw.jMutex.Unlock()
}
