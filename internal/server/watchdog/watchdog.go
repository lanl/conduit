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
	"github.com/lanl/conduit/internal/server/rqlite"
	"github.com/lanl/conduit/internal/server/scheduler"
	"github.com/spf13/viper"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Watchdog struct {
	id  uuid.UUID
	log *logger.ConduitLogger
	em  *etcd.ETCDManager
	rm  *rqlite.RqliteManager
	cm  *cert.CertManager
	sch []*scheduler.Scheduler

	transfers map[uuid.UUID]context.CancelFunc // map of transfers that are currently being watched. key: transfer id value: cancelFunc for context
	tMutex    sync.RWMutex

	jobs   map[uuid.UUID]bool // the jobs map is only used for stopping and keeps track of the events that the watchdog is actively handling
	jMutex sync.RWMutex

	stopWatchChan chan bool
	state         proto.ServerState
	sMutex        sync.Mutex // lock for watchdog state
}

func NewWatchdog(cl *logger.ConduitLogger, cm *cert.CertManager, em *etcd.ETCDManager, rm *rqlite.RqliteManager, sch []*scheduler.Scheduler) *Watchdog {
	id := uuid.New()

	// change prefix for logger
	l := logger.NewConduitLogger(cl.GetLevel(), fmt.Sprintf("watchdog[%s]:", id))

	w := &Watchdog{
		id:        id,
		log:       l,
		cm:        cm,
		em:        em,
		rm:        rm,
		sch:       sch,
		transfers: make(map[uuid.UUID]context.CancelFunc),
		jobs:      make(map[uuid.UUID]bool),
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
			go w.handleWatchEvents(wresp.Events)
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

	// check to see if all the jobs are stopped
	jobsStopped := false
	jobCount := 0
	for !jobsStopped {
		w.jMutex.Lock()
		numJobs := len(w.jobs)
		w.jMutex.Unlock()

		if numJobs == 0 {
			jobsStopped = true
		}

		if !jobsStopped && jobCount != numJobs {
			w.log.Debugf("waiting for %v jobs to complete", numJobs)
			jobCount = numJobs
		}
		if !jobsStopped {
			time.Sleep(100 * time.Millisecond)
		}
	}

	w.log.Info("all watchdog jobs are complete")

	return nil
}

// handleWatchEvents gets called anytime an event gets sent to the watch channel
func (w *Watchdog) handleWatchEvents(evs []*clientv3.Event) {
	for _, ev := range evs {
		// w.log.Debugf("new event in transfers: %+v", ev)
		id, _, err := proto.ParseETCDTransfersKey(string(ev.Kv.Key))
		if err != nil {
			// this prints a lot of messages
			// w.log.Debugf("Got non lease event: %v",  err)
		}

		it := proto.IncompleteTransfer(&proto.TransferDetails{TransferID: id.String()})

		if ev.Type == mvccpb.DELETE {
			switch string(ev.Kv.Key) {
			case it.ETCDArchiveStateKey():
				w.stopWatchingTransfer(it, string(ev.Kv.Key), string(ev.Kv.Value))
			}
		} else if ev.Type == mvccpb.PUT {
			switch string(ev.Kv.Key) {
			case it.ETCDArchiveStateKey():
				switch string(ev.Kv.Value) {
				case proto.ArchiveState_ARCHIVE_READY.String():
					eventID := uuid.New()
					w.jMutex.Lock()
					w.jobs[eventID] = true
					w.jMutex.Unlock()

					go w.archiveTransfer(it, eventID)
					fallthrough
				case proto.ArchiveState_ARCHIVE_SUBMIT.String():
					w.startWatchingTransfer(it, string(ev.Kv.Key), string(ev.Kv.Value))
				case proto.ArchiveState_ARCHIVE_COMPLETE.String():
					fallthrough
				case proto.ArchiveState_ARCHIVE_ERROR.String():
					w.stopWatchingTransfer(it, string(ev.Kv.Key), string(ev.Kv.Value))
				}
			case it.ETCDPausedStateKey():
				fallthrough
			case it.ETCDStateKey():
				switch string(ev.Kv.Value) {
				case proto.TransferState_TRANSFER_ERROR.String():
					w.stopWatchingTransfer(it, string(ev.Kv.Key), string(ev.Kv.Value))
				case proto.TransferState_TRANSFER_FINALIZED.String():
					// TRANSFER_FINALIZED does not necessarily mean the watchdog is done.
					// Archiving may still be in progress and may require lease recovery.
					t, _, err := w.em.GetTransfer(id)
					if err != nil {
						w.log.Errorf("failed to get transfer[%s] while handling finalized state: %v", it.GetTransferID(), err)

						// Important: don't stop an existing watcher when we don't know
						// whether archive recovery is still required.
						break
					}

					switch t.GetArchiveState() {
					case proto.ArchiveState_ARCHIVE_READY, proto.ArchiveState_ARCHIVE_SUBMIT:
						w.log.Debugf("transfer[%s] is finalized but archive is still active [%s]; continuing watchdog", it.GetTransferID(), t.GetArchiveState())
						w.startWatchingTransfer(it, string(ev.Kv.Key), string(ev.Kv.Value))
					default:
						w.stopWatchingTransfer(it, string(ev.Kv.Key), string(ev.Kv.Value))
					}
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
					// start watching
					w.startWatchingTransfer(it, string(ev.Kv.Key), string(ev.Kv.Value))
				default:
					w.log.Errorf("received unknown transfer[%s] state: %v=%v", it.GetTransferID(), string(ev.Kv.Key), string(ev.Kv.Value))
				}
			}
		}
	}
}

// startWatchingLease gets called when a state matches when we should start watching the expiry of that lease
func (w *Watchdog) startWatchingTransfer(it proto.IncompleteTransfer, key string, value string) {
	// get transfer id
	id, err := uuid.Parse(it.GetTransferID())
	if err != nil {
		w.log.Errorf("failed to parse transfer id from [%v]: %v", it.GetTransferID(), err)
		return
	}

	w.tMutex.Lock()
	// check if lease is already being monitored
	if _, ok := w.transfers[id]; !ok {
		ctx, cancel := context.WithCancel(context.Background())
		w.transfers[id] = cancel
		w.tMutex.Unlock()
		// start watching this lease
		w.log.Debugf("starting to monitor transfer[%s] %s = %s", it.GetTransferID(), key, value)
		go w.monitorTransferExpiry(it, ctx)
	} else {
		w.log.Debugf("transfer[%s] is already being monitored", it.GetTransferID())
		w.tMutex.Unlock()
	}
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
	if cancel, ok := w.transfers[id]; ok {
		cancel()
		delete(w.transfers, id)
		w.log.Debugf("stopping monitoring transfer[%s] %s = %s", it.GetTransferID(), key, value)
	}
	w.tMutex.Unlock()
}

// monitorLeaseExpiry will monitor a lease's expiry by sleeping until the expiration time and then checking if it's changed
func (w *Watchdog) monitorTransferExpiry(it proto.IncompleteTransfer, ctx context.Context) {
	// get transfer id
	id, err := uuid.Parse(it.GetTransferID())
	if err != nil {
		w.log.Errorf("failed to parse transfer id from [%v]: %v", it.GetTransferID(), err)
		return
	}

monitorLoop:
	for {
		// check if lease is still in w.transfers
		w.tMutex.RLock()
		_, found := w.transfers[id]
		w.tMutex.RUnlock()
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

				if expiryTime.After(time.Now()) {
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
						w.log.Errorf("failed to get transfer[%s] from etcd: %v", it.GetTransferID(), err)
					} else {
						w.log.Debugf("transfer[%s] state: %s", it.GetTransferID(), t.GetState())
						w.log.Debugf("transfer[%s] archive: %s", it.GetTransferID(), t.GetArchiveState())
						w.log.Debugf("transfer[%s] expiry: %s", it.GetTransferID(), t.GetExpiry().AsTime().Format(time.RFC3339))

						// check if the transfer expired in a state that we can recover from
						var rollbackErr error
						switch {
						// transfer state
						case t.GetState() == proto.TransferState_TRANSFER_WAITING_FOR_LEASE:
							// the transfer expired while waiting for lease. Push the state back to validation complete
							rollbackErr = w.em.RollbackState(t, proto.TransferState_TRANSFER_WAITING_FOR_LEASE, proto.TransferState_TRANSFER_VALIDATION_COMPLETE, etcd.Transfer, &expiryTime)
							if rollbackErr != nil {
								w.log.Error(rollbackErr)
							} else {
								continue monitorLoop
							}
						case t.GetArchiveState() == proto.ArchiveState_ARCHIVE_SUBMIT:
							// the transfer expired while submitting to rqlite. Push the state back to archive ready
							rollbackErr = w.em.RollbackState(t, proto.ArchiveState_ARCHIVE_SUBMIT, proto.ArchiveState_ARCHIVE_READY, etcd.Archive, &expiryTime)
							if rollbackErr != nil {
								w.log.Error(rollbackErr)
							} else {
								continue monitorLoop
							}
						}
					}

					// expire the lease
					w.log.Debugf("transfer[%s] no longer valid [%s] vs [%s]. expiring...", it.GetTransferID(), expiryTime, time.Now())
					err = w.expireTransfer(t, it, expiryTime)
					if err != nil {
						w.log.Errorf("failed to expire transfer[%s]: %v", it.GetTransferID(), err)
					}
					break monitorLoop
				}
			}
		} else {
			w.log.Debugf("transfer[%s] no longer in \"transfers\"", it.GetTransferID())
			break monitorLoop
		}
	}
}

// expireTransfer gets called when a transfers's expiry did not change and has expired
func (w *Watchdog) expireTransfer(t *proto.TransferDetails, it proto.IncompleteTransfer, expiry time.Time) error {
	// remove transfer from schedulers after this function
	defer func() {
		err := w.removeTransferFromSchedulers(it)
		if err != nil {
			w.log.Errorf("failed to remove transfer[%v] from schedulers: %v", it.GetTransferID(), err)
		}
	}()

	expiryKey := it.ETCDExpiryKey()
	stateKey := it.ETCDStateKey()
	errorKey := it.ETCDErrorKey()

	txn, _ := w.em.Txn()
	txn.If(
		clientv3.Compare(clientv3.Value(stateKey), "!=", proto.TransferState_TRANSFER_TEARDOWN_COMPLETE.String()),
		clientv3.Compare(clientv3.Value(errorKey), "!=", proto.Error_ERROR_LEASE_EXPIRED.String()),
		clientv3.Compare(clientv3.Value(errorKey), "=", proto.Error_ERROR_NONE.String()),
		clientv3.Compare(clientv3.Value(expiryKey), "=", expiry.Format(time.RFC3339)),
	)
	txn.Then(clientv3.OpPut(errorKey, proto.Error_ERROR_LEASE_EXPIRED.String()))

	resp, err := txn.Commit()
	if err != nil {
		return fmt.Errorf("error committing transaction to etcd for transfer[%s]: %v", it.GetTransferID(), err)
	}
	if !resp.Succeeded {
		w.log.Warnf("failed to expire transfer[%v], it was already marked as expired or expiry updated", it.GetTransferID())
		return nil
	} else {
		w.log.Infof("successfully expired transfer[%v]", it.GetTransferID())
	}

	return nil
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

func (w *Watchdog) compactETCD() error {
	oRev, err := w.em.GetOldestTransfersRev()
	if err != nil {
		return fmt.Errorf("failed to get oldest revision from etcd: %v", err)
	}

	curRev, err := w.em.CompactRevision(oRev)
	if err != nil {
		return fmt.Errorf("failed to compact to oldest safe rev[%v] cur[%v]: %v", oRev, curRev, err)
	}

	w.log.Infof("successfully compacted etcd to revision: %v. current revision: %v", oRev, curRev)
	return nil
}

func (w *Watchdog) removeJob(eventID uuid.UUID) {
	w.jMutex.Lock()
	delete(w.jobs, eventID)
	w.jMutex.Unlock()
}
