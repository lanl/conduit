// Copyright 2026. Triad National Security, LLC. All rights reserved.

package watchdog

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/defaults"
	"github.com/lanl/conduit/internal/etcd"
	"github.com/lanl/conduit/internal/logger"
	cert "github.com/lanl/conduit/internal/pki"
	"github.com/lanl/conduit/internal/server/scheduler"
	"github.com/spf13/viper"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Watchdog struct {
	id  uuid.UUID
	log *logger.ConduitLogger
	em  *etcd.ETCDManager
	cm  *cert.CertManager
	sch []*scheduler.Scheduler

	transfers map[uuid.UUID]*transferMonitor // map of transfers that are currently being watched. key: transfer id value: cancelFunc for context
	tMutex    sync.RWMutex

	stopWatchChan chan bool
	state         proto.ServerState
	sMutex        sync.Mutex // lock for watchdog state

	cleanerCancel context.CancelCauseFunc
}

type transferMonitor struct {
	id     uuid.UUID
	cancel context.CancelFunc
}

func NewWatchdog(cl *logger.ConduitLogger, cm *cert.CertManager, em *etcd.ETCDManager, sch []*scheduler.Scheduler) *Watchdog {
	id := uuid.New()

	// change prefix for logger
	l := logger.NewConduitLogger(cl.GetLevel(), fmt.Sprintf("watchdog[%s]:", id))

	w := &Watchdog{
		id:        id,
		log:       l,
		cm:        cm,
		em:        em,
		sch:       sch,
		transfers: make(map[uuid.UUID]*transferMonitor),
		state:     proto.ServerState_SERVER_STARTING,
	}

	return w
}

func (w *Watchdog) StartWatchdog() error {
	// start watching for new leases to appear
	successChan := make(chan bool)
	waitChan := make(chan bool)
	stopChan := make(chan bool)
	go w.watchTransfers(successChan, waitChan, stopChan)
	<-successChan
	w.stopWatchChan = stopChan

	// check for any transfers that should've already been watched
	err := w.checkCurrentTransfers()
	if err != nil {
		w.sMutex.Lock()
		w.state = proto.ServerState_SERVER_STOPPED
		w.sMutex.Unlock()
		return fmt.Errorf("failed to start watchdog: %v", err)
	}
	waitChan <- true

	_, cancel := context.WithCancelCause(context.Background())
	// go w.watchdogCleaner(ctx)
	w.cleanerCancel = cancel

	w.sMutex.Lock()
	w.state = proto.ServerState_SERVER_RUNNING
	w.sMutex.Unlock()

	w.log.Infof("Started!")

	return nil
}

// checkCurrentTransfers, checks to see if any transfers already in etcd need to be watched
func (w *Watchdog) checkCurrentTransfers() error {
	// get all transfers in etcd to see if they need to be watched
	resp, err := w.em.GetPrefix(proto.TransferPrefix)
	if err != nil {
		return fmt.Errorf("failed to get all transfers from etcd: %v", err)
	}

	// convert kvs to events
	evs := []*clientv3.Event{}

	for _, kv := range resp.Kvs {
		ev := &clientv3.Event{
			Type: mvccpb.PUT,
			Kv:   kv,
		}
		evs = append(evs, ev)
	}
	w.handleWatchEvents(evs)
	return nil
}

func (w *Watchdog) watchTransfers(successChan chan bool, waitChan chan bool, stopChan chan bool) {
	wc := w.em.SubscribeToTransfers(w.id)
	successChan <- true

	// wait for initial startup to complete
	<-waitChan

	defer w.em.UnsubscribeFromTransfers(w.id)

	for {
		select {
		case wresp, ok := <-wc:
			if !ok {
				w.log.Errorf("transfer watch channel closed unexpectedly")
				return
			}
			w.handleWatchEvents(wresp.Events)
			if wresp.Canceled {
				w.log.Errorf("received cancel message from watch stream: %+v", wresp)
			}
		case <-stopChan:
			w.log.Infof("stopped watching transfer events")
			return
		}
	}
}

func (w *Watchdog) StopWatchdog() error {
	// check that the watchdog is in a running state
	w.sMutex.Lock()
	state := w.state

	if state == proto.ServerState_SERVER_RUNNING {
		w.state = proto.ServerState_SERVER_STOPPING
	} else {
		w.sMutex.Unlock()
		return fmt.Errorf("could not stop watchdog[%v] because it is not in the running state: %v", w.id, state)
	}
	w.sMutex.Unlock()

	w.cleanerCancel(fmt.Errorf("stopping watchdog"))

	w.log.Info("stopping watchdog")

	// stop watching transfers from etcd
	w.stopWatchChan <- true

	w.log.Info("stopped watching from etcd")

	// stop monitoring all transfers
	tids := []uuid.UUID{}
	w.tMutex.RLock()
	for tid := range w.transfers {
		tids = append(tids, tid)
	}
	w.tMutex.RUnlock()

	for _, tid := range tids {
		it := proto.IncompleteTransfer(&proto.TransferDetails{TransferID: tid.String()})

		w.stopWatchingTransfer(it, "", "")
	}

	w.log.Info("stopped watching all transfers")

	return nil
}

func (w *Watchdog) watchdogCleaner(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.log.Infof("stopping watchdog cleaner")
			return

		case <-ticker.C:
			err := w.CleanupETCD()
			if err != nil {
				w.log.Errorf("failed to cleanup etcd: %v", err)
			}
		}
	}
}

// handleWatchEvents gets called anytime an event gets sent to the watch channel
func (w *Watchdog) handleWatchEvents(evs []*clientv3.Event) {
	for _, ev := range evs {
		if ev == nil || ev.Kv == nil {
			continue
		}

		id, _, err := proto.ParseETCDTransfersKey(string(ev.Kv.Key))
		if err != nil {
			continue
		}

		it := proto.IncompleteTransfer(&proto.TransferDetails{TransferID: id.String()})

		key := string(ev.Kv.Key)
		value := string(ev.Kv.Value)

		switch ev.Type {
		case mvccpb.DELETE:
			// A finalized transfer will eventually be deleted by the archiver. Stop any stale monitor if the transfer's state or expiry disappears.
			switch key {
			case it.ETCDStateKey(), it.ETCDExpiryKey():
				w.stopWatchingTransfer(it, key, value)
			}

		case mvccpb.PUT:
			switch key {
			case it.ETCDPausedStateKey():
				fallthrough
			case it.ETCDStateKey():
				switch value {
				case proto.TransferState_TRANSFER_ERROR.String():
					fallthrough
				case proto.TransferState_TRANSFER_FINALIZED.String():
					// Terminal transfers are no longer the watchdog's
					// responsibility. Archival is handled independently.
					w.stopWatchingTransfer(it, key, value)

				case proto.TransferState_TRANSFER_NONE.String():
					fallthrough
				case proto.TransferState_TRANSFER_INIT.String():
					fallthrough
				case proto.TransferState_TRANSFER_INIT_COMPLETE.String():
					fallthrough
				case proto.TransferState_TRANSFER_WAITING_FOR_LEASE.String():
					fallthrough
				case proto.TransferState_TRANSFER_LEASE_ACQUIRED.String():
					fallthrough
				case proto.TransferState_TRANSFER_VALIDATION_READY.String():
					fallthrough
				case proto.TransferState_TRANSFER_VALIDATION_COMPLETE.String():
					fallthrough
				case proto.TransferState_TRANSFER_SETUP_READY.String():
					fallthrough
				case proto.TransferState_TRANSFER_SETUP_COMPLETE.String():
					fallthrough
				case proto.TransferState_TRANSFER_DATA_READY.String():
					fallthrough
				case proto.TransferState_TRANSFER_DATA_COMPLETE.String():
					fallthrough
				case proto.TransferState_TRANSFER_TEARDOWN_READY.String():
					fallthrough
				case proto.TransferState_TRANSFER_TEARDOWN_COMPLETE.String():
					fallthrough
				case proto.TransferState_TRANSFER_SETUP_SUBMITTED.String():
					fallthrough
				case proto.TransferState_TRANSFER_SETUP.String():
					fallthrough
				case proto.TransferState_TRANSFER_TEARDOWN_SUBMITTED.String():
					fallthrough
				case proto.TransferState_TRANSFER_TEARDOWN.String():
					fallthrough
				case proto.TransferState_TRANSFER_VALIDATION_SUBMITTED.String():
					fallthrough
				case proto.TransferState_TRANSFER_VALIDATING.String():
					fallthrough
				case proto.TransferState_TRANSFER_DATA_SUBMITTED.String():
					fallthrough
				case proto.TransferState_TRANSFER_DATA_TRANSFERRING.String():
					w.startWatchingTransfer(it, key, value)

				default:
					w.log.Errorf("received unknown transfer[%s] state: %s=%s", it.GetTransferID(), key, value)
				}
			}
		}
	}
}

// startWatchingLease gets called when a state matches when we should start watching the expiry of that lease
func (w *Watchdog) startWatchingTransfer(it proto.IncompleteTransfer, key string, value string) {
	id, err := uuid.Parse(it.GetTransferID())
	if err != nil {
		w.log.Errorf("failed to parse transfer id from [%v]: %v", it.GetTransferID(), err)
		return
	}

	w.tMutex.Lock()

	if _, ok := w.transfers[id]; ok {
		w.log.Debugf("transfer[%s] is already being monitored", it.GetTransferID())
		w.tMutex.Unlock()
		return
	}

	ctx, cancel := context.WithCancel(context.Background())

	monitor := &transferMonitor{
		id:     uuid.New(),
		cancel: cancel,
	}

	w.transfers[id] = monitor

	w.tMutex.Unlock()

	w.log.Debugf("starting to monitor transfer[%s] %s = %s", it.GetTransferID(), key, value)

	go w.monitorTransferExpiry(it, ctx, monitor.id)
}

// stopWatchingLease gets called when a state matches when we should stop watching the expiry of that lease
func (w *Watchdog) stopWatchingTransfer(it proto.IncompleteTransfer, key string, value string) {
	// get transfer id
	id, err := uuid.Parse(it.GetTransferID())
	if err != nil {
		w.log.Errorf("failed to parse transfer id from [%v]: %v", it.GetTransferID(), err)
		return
	}

	w.tMutex.Lock()
	// check if we are even monitoring this transfer
	if tm, ok := w.transfers[id]; ok {
		tm.cancel()
		delete(w.transfers, id)
		w.log.Debugf("stopping monitoring transfer[%s] %s = %s", it.GetTransferID(), key, value)
	}
	w.tMutex.Unlock()
}

// monitorLeaseExpiry will monitor a lease's expiry by sleeping until the expiration time and then checking if it's changed
func (w *Watchdog) monitorTransferExpiry(it proto.IncompleteTransfer, ctx context.Context, monitorID uuid.UUID) {
	id, err := uuid.Parse(it.GetTransferID())
	if err != nil {
		w.log.Errorf("failed to parse transfer id from [%v]: %v", it.GetTransferID(), err)
		return
	}

	defer func() {
		w.tMutex.Lock()

		// Only remove the entry if it still belongs to us.
		if m, ok := w.transfers[id]; ok && m.id == monitorID {
			delete(w.transfers, id)
		}

		w.tMutex.Unlock()

		w.log.Debugf("stopping monitor loop for transfer[%v]", it.GetTransferID())
	}()

monitorLoop:
	for {
		// check if lease is still in w.transfers
		w.tMutex.RLock()
		monitor, found := w.transfers[id]
		currentMonitor := found && monitor.id == monitorID
		w.tMutex.RUnlock()

		if !currentMonitor {
			w.log.Debugf("transfer[%s] monitor[%s] is no longer current", it.GetTransferID(), monitorID)
			break monitorLoop
		}

		if found {
			// w.log.Debugf("transfer[%s] still in \"transfers\"", it.GetTransferID())

			expiryTime, err := w.em.GetExpiry(it)
			if err != nil {
				w.log.Errorf("error getting transfer expiry from etcd for transfer[%s]: %v", it.GetTransferID(), err)
				_, _, err := w.em.SafelyAddErr(it, proto.Error_ERROR_CONDUIT_INTERNAL, fmt.Errorf("failed to read expiry from etcd"))
				if err != nil {
					w.log.Errorf("failed to add error to transfer[%v]", it.GetTransferID())
				}
				break monitorLoop
			} else {
				// check if the transfer is paused at a certain state. We don't want to kill it if it's supposed to be paused there
				if viper.GetBool(defaults.ConfigTestKey) {
					tState, tErr := w.em.GetTransferState(it)
					tpState, tpErr := w.em.GetTransferPausedState(it)

					if tErr != nil || tpErr != nil {
						if tErr != nil {
							w.log.Errorf("failed to get transfer[%s] state from etcd: %v", it.GetTransferID(), tErr)
						}
						if tpErr != nil {
							w.log.Errorf("failed to get transfer[%s] paused state from etcd: %v", it.GetTransferID(), tpErr)
						}
					} else {
						if tpState == tState {
							// this lease is paused at the state it's supposed to be in. Don't kill
							w.log.Debugf("transfer[%s] is in a paused state. removing from watchdog while paused.", it.GetTransferID())
							w.stopWatchingTransfer(it, "PAUSE", tState.String())
							break monitorLoop
						}
					}
				}

				now := time.Now()
				if expiryTime.After(now) {
					// the lease is still valid. Lets sleep until it expires
					w.log.Debugf("transfer[%s] still valid. sleeping for %v", it.GetTransferID(), time.Until(expiryTime.Add(10*time.Second)))
					select {
					case <-ctx.Done():
						w.log.Debugf("transfer[%s] context was cancelled, monitor stopped", it.GetTransferID())
						break monitorLoop
					case <-time.After(time.Until(expiryTime.Add(10 * time.Second))):
						continue monitorLoop
					}
				} else {
					// the transfer expired
					t, _, err := w.em.GetTransfer(id)
					if err != nil {
						if ctx.Err() != nil {
							break monitorLoop
						}

						w.log.Errorf("failed to get transfer[%s] from etcd: %v", it.GetTransferID(), err)
					} else {
						w.log.Debugf("transfer[%s] state: %s", it.GetTransferID(), t.GetState())
						w.log.Debugf("transfer[%s] expiry: %s", it.GetTransferID(), t.GetExpiry().AsTime().Format(time.RFC3339))

						// The state watch normally handles this, but this protects against
						// races where the monitor has already woken up when finalization occurs.
						if t.GetState() == proto.TransferState_TRANSFER_FINALIZED || t.GetState() == proto.TransferState_TRANSFER_ERROR {
							break monitorLoop
						}

						// check if the transfer expired in a state that we can recover from
						if t.GetState() == proto.TransferState_TRANSFER_WAITING_FOR_LEASE {
							// the transfer expired while waiting for lease. Push the state back to validation complete
							rollbackErr := w.em.RollbackState(t, proto.TransferState_TRANSFER_WAITING_FOR_LEASE, proto.TransferState_TRANSFER_VALIDATION_COMPLETE, etcd.Transfer, &expiryTime)
							if rollbackErr != nil {
								w.log.Error(rollbackErr)
							} else {
								continue monitorLoop
							}
						}
					}

					// expire the lease
					w.log.Infof("transfer[%s] no longer valid [%s] vs [%s]. expiring...", it.GetTransferID(), expiryTime, now)
					successful, currentExpiry, jobPending, err := w.expireTransfer(it, expiryTime)
					if err != nil {
						w.log.Errorf("failed to expire transfer[%s]: %v", it.GetTransferID(), err)
						break monitorLoop
					}
					if successful {
						break monitorLoop
					}

					if jobPending {
						w.log.Debugf("transfer[%s] is expired but still has a pending scheduler job; continuing monitoring", it.GetTransferID())

						// // Don't spin on an already-expired timestamp.
						// select {
						// case <-ctx.Done():
						// 	break monitorLoop
						// case <-time.After(30 * time.Second):
						// 	continue monitorLoop
						// }
					}

					// The transaction failed. Check whether the expiry changed
					// underneath us.
					if !currentExpiry.Equal(expiryTime) {
						w.log.Debugf("transfer[%s] expiry changed from %s to %s while attempting expiry; continuing monitoring", it.GetTransferID(), expiryTime, currentExpiry)
						continue monitorLoop
					}

					// The expiry did not change, so some other transaction
					// predicate failed. The transfer no longer needs this
					// expiry attempt.
					break monitorLoop
				}
			}
		} else {
			w.log.Debugf("transfer[%s] no longer in \"transfers\"", it.GetTransferID())
			break monitorLoop
		}
	}
}

// expireTransfer gets called when a transfer's expiry did not change and has expired.
func (w *Watchdog) expireTransfer(it proto.IncompleteTransfer, expiry time.Time) (successful bool, currentExpiryTime time.Time, jobPending bool, err error) {
	expiryKey := it.ETCDExpiryKey()
	stateKey := it.ETCDStateKey()
	errorKey := it.ETCDErrorKey()
	jobKey := it.ETCDJobsKey()

	txn, _ := w.em.Txn()
	txn.If(
		clientv3.Compare(clientv3.Value(stateKey), "!=", proto.TransferState_TRANSFER_FINALIZED.String()),
		clientv3.Compare(clientv3.Value(stateKey), "!=", proto.TransferState_TRANSFER_ERROR.String()),
		clientv3.Compare(clientv3.Value(errorKey), "=", proto.Error_ERROR_NONE.String()),
		clientv3.Compare(clientv3.Value(expiryKey), "=", expiry.Format(time.RFC3339)),
		clientv3.Compare(clientv3.CreateRevision(jobKey), "=", 0),
	)

	txn.Then(
		clientv3.OpPut(errorKey, proto.Error_ERROR_LEASE_EXPIRED.String()),
	)

	// If the transaction fails, retrieve the values that caused it
	// to fail so we can determine whether the transfer should still run.
	txn.Else(
		clientv3.OpGet(stateKey),
		clientv3.OpGet(errorKey),
		clientv3.OpGet(expiryKey),
		clientv3.OpGet(jobKey),
	)

	resp, err := txn.Commit()
	if err != nil {
		return false, time.Unix(0, 0), false, fmt.Errorf("error committing transaction to etcd for transfer[%s]: %v", it.GetTransferID(), err)
	}

	if resp.Succeeded {
		w.log.Infof("successfully expired transfer[%v]", it.GetTransferID())

		// We successfully marked the transfer as lease-expired, so it
		// must no longer be scheduled.
		if err := w.removeTransferFromSchedulers(it); err != nil {
			w.log.Errorf("failed to remove transfer[%v] from schedulers: %v", it.GetTransferID(), err)
		}

		return true, time.Unix(0, 0), false, nil
	}

	// The archiver may have deleted the transfer while this monitor was trying to expire it. If all of the required transfer keys are gone,
	// there is nothing left for the watchdog to do.
	if len(resp.Responses) == 4 &&
		len(resp.Responses[0].GetResponseRange().Kvs) == 0 &&
		len(resp.Responses[1].GetResponseRange().Kvs) == 0 &&
		len(resp.Responses[2].GetResponseRange().Kvs) == 0 {

		w.log.Debugf("transfer[%s] no longer exists in etcd", it.GetTransferID())

		return false, time.Unix(0, 0), false, nil
	}

	if len(resp.Responses) != 4 ||
		len(resp.Responses[0].GetResponseRange().Kvs) == 0 ||
		len(resp.Responses[1].GetResponseRange().Kvs) == 0 ||
		len(resp.Responses[2].GetResponseRange().Kvs) == 0 {

		return false, time.Unix(0, 0), false, fmt.Errorf("failed to get state, error, expiry, and job for transfer[%s]: %+v", it.GetTransferID(), resp.Responses)
	}

	state := string(resp.Responses[0].GetResponseRange().Kvs[0].Value)
	transferErr := string(resp.Responses[1].GetResponseRange().Kvs[0].Value)
	currentExpiry := string(resp.Responses[2].GetResponseRange().Kvs[0].Value)

	jobPending = len(resp.Responses[3].GetResponseRange().Kvs) > 0

	currentExpiryTime, err = time.Parse(time.RFC3339, currentExpiry)
	if err != nil {
		return false, time.Unix(0, 0), false, fmt.Errorf("error parsing expiry from etcd[%s]: %v", string(resp.Responses[2].GetResponseRange().Kvs[0].Value), err)
	}

	// Somebody else completed or errored the transfer while we were
	// attempting to expire it.
	if transferErr != proto.Error_ERROR_NONE.String() ||
		state == proto.TransferState_TRANSFER_FINALIZED.String() ||
		state == proto.TransferState_TRANSFER_ERROR.String() {

		if err := w.removeTransferFromSchedulers(it); err != nil {
			w.log.Errorf("failed to remove transfer[%v] from schedulers: %v", it.GetTransferID(), err)
		}

		return false, currentExpiryTime, false, nil
	}

	if !expiry.Equal(currentExpiryTime) {
		// somebody refreshed the lease while we were trying to expire it.
		// The transfer is still active, so DO NOT remove it from the scheduler.
		w.log.Debugf("transfer[%v] expiry changed from [%s] to [%s] while attempting to expire it", it.GetTransferID(), expiry.Format(time.RFC3339), currentExpiry)

		return false, currentExpiryTime, false, nil
	}

	if jobPending {
		w.log.Debugf("transfer[%v] expiry elapsed but scheduler job [%s] is still pending", it.GetTransferID(), jobKey)
		return false, currentExpiryTime, true, nil
	}

	w.log.Debugf("did not expire transfer[%v]: state=%s error=%s expiry=%s", it.GetTransferID(), state, transferErr, currentExpiry)

	return false, currentExpiryTime, false, nil
}

func (w *Watchdog) removeTransferFromSchedulers(it proto.IncompleteTransfer) error {
	id, err := uuid.Parse(it.GetTransferID())
	if err != nil {
		return fmt.Errorf("failed to parse transferID[%v]: %v", it.GetTransferID(), err)
	}
	for _, s := range w.sch {
		s.RemoveTransfer(id)
	}

	return nil
}

func (w *Watchdog) CleanupETCD() error {
	// get the oldest compact revision key
	oldestKV, currRev, err := w.em.GetOldestTransfersRev()
	if err != nil {
		return fmt.Errorf("failed to get oldest revision from etcd: %v", err)
	}

	if oldestKV.CreateRevision == currRev {
		// we're already compacted to the latest revision so no need for cleanup
		return nil
	}

	// get what events happened at this revision
	evs, err := w.em.GetModifiedKeysAtRev(oldestKV.CreateRevision)
	if err != nil {
		return fmt.Errorf("failed to get modified keys at rev[%v]: %v", oldestKV.CreateRevision, err)
	}

	// check if each key is part of a transfer
	// if it is, check if that transfer is still active and its archive status
	// if it is a lone key, log it and delete it
	for _, ev := range evs {
		id, _, err := proto.ParseETCDTransfersKey(string(ev.Kv.Key))
		if err != nil {
			return fmt.Errorf("failed to parse transfer id from transfers key during cleanup: %v", err)
		}

		t, pErr, err := w.em.GetTransfer(id)
		if err != nil {
			switch pErr {
			case proto.Error_ERROR_CONDUIT_INTERNAL:
				// if there is a failure to parse the transfer, lets log and delete this key
				w.log.Warnf("error while parsing transfer from key. deleting key from etcd: %v", string(ev.Kv.Key))
				_, err := w.em.Delete(string(ev.Kv.Key))
				if err != nil {
					return fmt.Errorf("failed to delete key[%v] from etcd: %v", string(ev.Kv.Key), err)
				}

				continue

			case proto.Error_ERROR_ETCD_CONNECTION:
				return fmt.Errorf("failed to get transfer from etcd: %v", err)
			}
		}

		// there is a transfer in etcd
		if !t.GetActive() {
			delete := false
			switch {
			case t.GetState() == proto.TransferState_TRANSFER_NONE:
				// the transfer state is none which could mean this is a lone key in etcd. log and delete it
				delete = true
				w.log.Warnf("Transfer state in etcd is NONE. deleting key from etcd: %v = %v", string(ev.Kv.Key), string(ev.Kv.Value))
			case t.GetArchiveState() == proto.ArchiveState_ARCHIVE_NONE:
				// the archive state is none which could mean this is a lone key in etcd. log and delete it
				delete = true
				w.log.Warnf("Archive state in etcd is NONE. deleting key from etcd: %v", string(ev.Kv.Key))
			}

			if delete {
				_, err := w.em.DeleteTransfer(id)
				if err != nil {
					return fmt.Errorf("failed to delete key[%v] from etcd: %v", string(ev.Kv.Key), err)
				}

				continue
			}
		}
	}

	return nil
}
