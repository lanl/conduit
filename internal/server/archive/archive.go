// Copyright 2026. Triad National Security, LLC. All rights reserved.

package archive

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/internal/etcd"
	"github.com/lanl/conduit/internal/logger"
	cert "github.com/lanl/conduit/internal/pki"
	"github.com/lanl/conduit/internal/server/rqlite"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	archiveClaimPrefix = "archive/claims/"
	archiveClaimTTL    = 30 // seconds
)

func archiveClaimKey(tid uuid.UUID) string {
	return archiveClaimPrefix + tid.String()
}

type archiveClaim struct {
	key     string
	token   string
	leaseID clientv3.LeaseID

	cancelKeepAlive context.CancelFunc
}

type Archiver struct {
	id  uuid.UUID
	log *logger.ConduitLogger
	em  *etcd.ETCDManager
	rm  *rqlite.RqliteManager
	cm  *cert.CertManager

	jobs   map[uuid.UUID]bool // the jobs map is only used for stopping and keeps track of the events that the watchdog is actively handling
	jMutex sync.RWMutex

	stopWatch context.CancelFunc
	state     proto.ServerState
	sMutex    sync.Mutex // lock for archiver state

	compactWake   chan bool
	compactCancel context.CancelFunc
}

func NewArchiver(cl *logger.ConduitLogger, cm *cert.CertManager, em *etcd.ETCDManager, rm *rqlite.RqliteManager) *Archiver {
	id := uuid.New()

	// change prefix for logger
	l := logger.NewConduitLogger(cl.GetLevel(), fmt.Sprintf("archiver[%s]:", id))

	w := &Archiver{
		id:          id,
		log:         l,
		cm:          cm,
		em:          em,
		rm:          rm,
		jobs:        make(map[uuid.UUID]bool),
		state:       proto.ServerState_SERVER_STARTING,
		compactWake: make(chan bool, 1),
	}

	return w
}

func (a *Archiver) StartArchiver() error {
	// start watching for finished transfers to appear
	successChan := make(chan bool)
	waitChan := make(chan bool)
	ctx, stopWatch := context.WithCancel(context.Background())

	go a.watchTransfers(ctx, successChan, waitChan)
	go a.watchArchiveClaims(ctx)

	a.stopWatch = stopWatch

	<-successChan

	// check for any transfers that should've already been watched
	err := a.checkCurrentTransfers()
	if err != nil {
		a.sMutex.Lock()
		a.state = proto.ServerState_SERVER_STOPPED
		a.sMutex.Unlock()
		return fmt.Errorf("failed to start archiver: %v", err)
	}
	waitChan <- true

	ctx, cancel := context.WithCancel(context.Background())
	go a.compactLoop(ctx)
	a.compactCancel = cancel

	a.sMutex.Lock()
	a.state = proto.ServerState_SERVER_RUNNING
	a.sMutex.Unlock()

	a.log.Infof("Started!")

	return nil
}

// checkCurrentTransfers, checks to see if any transfers already in etcd need to be watched
func (a *Archiver) checkCurrentTransfers() error {
	// get all transfers in etcd to see if they need to be watched
	resp, err := a.em.GetPrefix(proto.TransferPrefix)
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
	a.handleWatchEvents(evs)
	return nil
}

func (a *Archiver) watchTransfers(ctx context.Context, successChan chan bool, waitChan chan bool) {
	wc := a.em.SubscribeToTransfers(a.id)
	successChan <- true

	// wait for initial startup to complete
	<-waitChan

	defer a.em.UnsubscribeFromTransfers(a.id)

	for {
		select {
		case wresp, ok := <-wc:
			if !ok {
				a.log.Errorf("transfer watch channel closed unexpectedly")
				return
			}
			a.handleWatchEvents(wresp.Events)
			if wresp.Canceled {
				a.log.Errorf("received cancel message from watch stream: %+v", wresp)
			}
		case <-ctx.Done():
			a.log.Infof("stopped watching transfer events")
			return
		}
	}
}

func (a *Archiver) StopArchiver() error {
	// check that the archiver is in a running state
	a.sMutex.Lock()
	state := a.state

	if state == proto.ServerState_SERVER_RUNNING {
		a.state = proto.ServerState_SERVER_STOPPING
	} else {
		a.sMutex.Unlock()
		return fmt.Errorf("could not stop archiver[%v] because it is not in the running state: %v", a.id, state)
	}
	a.sMutex.Unlock()

	a.log.Info("stopping archiver")

	// stop watching transfers from etcd
	a.stopWatch()
	a.log.Info("stopped watching all transfers")

	// stop the compact loop
	a.compactCancel()

	// check to see if all the jobs are stopped
	jobsStopped := false
	jobCount := 0
	for !jobsStopped {
		a.jMutex.Lock()
		numJobs := len(a.jobs)
		a.jMutex.Unlock()

		if numJobs == 0 {
			jobsStopped = true
		}

		if !jobsStopped && jobCount != numJobs {
			a.log.Debugf("waiting for %v jobs to complete", numJobs)
			jobCount = numJobs
		}
		if !jobsStopped {
			time.Sleep(100 * time.Millisecond)
		}
	}

	a.log.Info("all archiver jobs are complete")

	return nil
}

// handleWatchEvents gets called anytime an event gets sent to the watch channel
func (a *Archiver) handleWatchEvents(evs []*clientv3.Event) {
	for _, ev := range evs {
		// We only care about PUTs. Deletes are expected when a successfully
		// archived transfer is removed from etcd.
		if ev == nil || ev.Kv == nil || ev.Type != mvccpb.PUT {
			continue
		}

		// w.log.Debugf("new event in transfers: %+v", ev)
		tid, _, err := proto.ParseETCDTransfersKey(string(ev.Kv.Key))
		if err != nil {
			// this prints a lot of messages
			// w.log.Debugf("Got non lease event: %v",  err)
			continue
		}

		it := proto.IncompleteTransfer(&proto.TransferDetails{TransferID: tid.String()})

		// The archiver only cares about archive-state changes.
		if string(ev.Kv.Key) != it.ETCDArchiveStateKey() {
			continue
		}

		switch string(ev.Kv.Value) {
		case proto.ArchiveState_ARCHIVE_READY.String():
			a.queueArchive(tid)
		case proto.ArchiveState_ARCHIVE_NONE.String(),
			proto.ArchiveState_ARCHIVE_COMPLETE.String(),
			proto.ArchiveState_ARCHIVE_ERROR.String(),
			proto.ArchiveState_ARCHIVE_SUBMIT.String():
			// Nothing to do.
			//
			// ARCHIVE_COMPLETE is normally only stored in rqlite now.
			// ARCHIVE_ERROR and ARCHIVE_SUBMIT are legacy states and
			// should no longer be written by the new archiver.
		default:
			a.log.Warnf("received unknown archive state for transfer[%s]: %s=%s", tid, string(ev.Kv.Key), string(ev.Kv.Value))
		}
	}
}

func (a *Archiver) removeJob(id uuid.UUID) {
	a.jMutex.Lock()
	delete(a.jobs, id)
	a.jMutex.Unlock()
}

func (a *Archiver) Compact() {
	select {
	case a.compactWake <- true:
	default:
		// A wakeup is already pending.
	}
}

func (a *Archiver) compactLoop(ctx context.Context) {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			a.log.Infof("stopping archive loop")
			return

		case <-ticker.C:
			select {
			case <-a.compactWake:
				// do compaction
				err := a.compactETCD()
				if err != nil {
					if err.Error() == etcd.ErrRevCompacted {
						a.log.Warnf("failed to compact etcd: %v", err)
					} else {
						a.log.Errorf("failed to compact etcd: %v", err)
					}
				}
			default:
				// no compaction requested, wait till the next tick
			}
		}
	}
}

func (a *Archiver) compactETCD() error {
	oldestKV, currRev, err := a.em.GetOldestTransfersRev()
	if err != nil {
		return fmt.Errorf("failed to get oldest revision from etcd: %v", err)
	}

	newCurrRev, err := a.em.CompactRevision(oldestKV.CreateRevision)
	if newCurrRev != -1 {
		currRev = newCurrRev
	}
	if err != nil {
		return fmt.Errorf("failed to compact to oldest safe revision[%v] current[%v] key[%v]: %v", oldestKV.CreateRevision, currRev, string(oldestKV.Key), err)
	}

	a.log.Infof("successfully compacted etcd to revision: %v. current revision: %v", oldestKV.CreateRevision, currRev)
	return nil
}

func (a *Archiver) acquireArchiveClaim(it proto.IncompleteTransfer) (*archiveClaim, bool, error) {
	tid, err := uuid.Parse(it.GetTransferID())
	if err != nil {
		return nil, false, fmt.Errorf("failed to parse transfer id[%s]: %v", it.GetTransferID(), err)
	}

	leaseID, err := a.em.GrantLease(archiveClaimTTL)
	if err != nil {
		return nil, false, err
	}

	claimKey := archiveClaimKey(tid)

	token := fmt.Sprintf("%s/%s", a.id.String(), uuid.New().String())

	txn, cancel := a.em.Txn()

	txn.If(
		// Nobody else owns this transfer.
		clientv3.Compare(clientv3.CreateRevision(claimKey), "=", 0),

		// The transfer still actually needs archival.
		clientv3.Compare(clientv3.Value(it.ETCDArchiveStateKey()), "=", proto.ArchiveState_ARCHIVE_READY.String()),
	)

	txn.Then(
		clientv3.OpPut(claimKey, token, clientv3.WithLease(leaseID)),
	)

	resp, err := txn.Commit()

	cancel()

	if err != nil {
		// We don't know whether the txn made it to etcd. Revoking is safe either way and makes sure we didn't leave a claim behind.
		if rerr := a.em.RevokeLease(leaseID); rerr != nil {
			a.log.Warnf("failed to revoke unused archive lease[%v]: %v", leaseID, rerr)
		}

		return nil, false, fmt.Errorf("failed to claim transfer[%s] for archival: %v", it.GetTransferID(), err)
	}

	if !resp.Succeeded {
		// Another archiver owns it, or it is no longer ARCHIVE_READY.
		if err := a.em.RevokeLease(leaseID); err != nil {
			a.log.Warnf("failed to revoke unused archive lease[%v]: %v", leaseID, err)
		}

		return nil, false, nil
	}

	keepAliveCtx, cancelKeepAlive := context.WithCancel(context.Background())

	keepAlive, err := a.em.KeepLeaseAlive(keepAliveCtx, leaseID)
	if err != nil {
		cancelKeepAlive()

		// IMPORTANT:
		// Don't revoke here. Let the lease expire naturally so this becomes the retry delay.
		return nil, false, fmt.Errorf("failed to start keepalive for archive claim transfer[%s]: %v", it.GetTransferID(), err)
	}

	// KeepAlive responses should be consumed.
	go func() {
		for range keepAlive {
		}
	}()

	return &archiveClaim{
		key:             claimKey,
		token:           token,
		leaseID:         leaseID,
		cancelKeepAlive: cancelKeepAlive,
	}, true, nil
}

func (a *Archiver) deleteTransferIfClaimed(it proto.IncompleteTransfer, claim *archiveClaim) (bool, error) {
	tid, err := uuid.Parse(it.GetTransferID())
	if err != nil {
		return false, err
	}

	txn, cancel := a.em.Txn()
	defer cancel()

	txn.If(
		clientv3.Compare(clientv3.Value(claim.key), "=", claim.token),
	)

	txn.Then(
		clientv3.OpDelete(proto.TransferPrefix+tid.String(), clientv3.WithPrefix()),

		// Probably already gone because CompleteTransfer deletes these, but harmless and makes cleanup explicit.
		clientv3.OpDelete(it.ETCDLeaseListKey(), clientv3.WithPrefix()),
	)

	resp, err := txn.Commit()

	if err != nil {
		return false, fmt.Errorf("failed deleting archived transfer[%s]: %v", it.GetTransferID(), err)
	}

	return resp.Succeeded, nil
}

func (a *Archiver) archiveTransfer(it proto.IncompleteTransfer, eventID uuid.UUID) {
	defer a.removeJob(eventID)

	claim, claimed, err := a.acquireArchiveClaim(it)
	if err != nil {
		a.log.Errorf("failed to acquire archive claim for transfer[%s]: %v", it.GetTransferID(), err)
		return
	}

	if !claimed {
		// Another archiver got it.
		return
	}

	// Every path out of this worker stops refreshing the claim.
	//
	// On failure we intentionally DO NOT revoke the lease. It will
	// expire naturally and trigger retry.
	defer claim.cancelKeepAlive()

	tid, err := uuid.Parse(it.GetTransferID())
	if err != nil {
		a.log.Errorf("archive transfer failed to parse transferid[%v]: %v", it.GetTransferID(), err)
		return
	}

	t, _, err := a.em.GetTransfer(tid)
	if err != nil {
		a.log.Errorf("archive transfer failed to get transfer[%s] from etcd: %v", it.GetTransferID(), err)
		return
	}

	// ARCHIVE_COMPLETE describes the version stored in rqlite.
	// We don't need to persist COMPLETE to etcd because the etcd
	// representation is about to be removed.
	t.ArchiveState = proto.ArchiveState_ARCHIVE_COMPLETE

	if err := a.rm.AddTransfer(t); err != nil {
		a.log.Errorf("failed to add transfer[%s] to rqlite: %v", it.GetTransferID(), err)

		// Do NOT revoke.
		//
		// cancelKeepAlive() runs as we return. The claim then expires
		// after archiveClaimTTL and another archiver retries it.
		return
	}

	// Only delete if this exact archive attempt still owns the claim.
	deleted, err := a.deleteTransferIfClaimed(it, claim)
	if err != nil {
		a.log.Errorf("failed to delete archived transfer[%s]: %v", it.GetTransferID(), err)
		return
	}

	if !deleted {
		a.log.Warnf("transfer[%s] was archived to rqlite but archive claim was lost before etcd cleanup", it.GetTransferID())

		// Leave the transfer behind. The new owner will retry AddTransfer,
		// see that the record already exists, and finish the cleanup.
		return
	}

	// The transfer is now durably in rqlite and gone from etcd.

	if err := a.em.RemoveTransferUser(tid.String()); err != nil {
		a.log.Errorf("failed to remove etcd user for transfer[%s]: %v", it.GetTransferID(), err)
	}

	// Success: remove the claim immediately rather than waiting for expiry.
	if err := a.em.RevokeLease(claim.leaseID); err != nil {
		a.log.Warnf("failed to revoke archive claim lease[%v] for transfer[%s]: %v", claim.leaseID, it.GetTransferID(), err)
	}

	a.Compact()

	a.log.Infof("successfully archived transfer[%s]", it.GetTransferID())
}

func (a *Archiver) watchArchiveClaims(ctx context.Context) {
	wc, cancel := a.em.GetWatchChannelPrefix(archiveClaimPrefix, 0)
	defer cancel()

	for {
		select {
		case resp, ok := <-wc:
			if !ok {
				a.log.Errorf("archive claim watch channel closed unexpectedly")
				return
			}

			if resp.Canceled {
				a.log.Errorf("archive claim watch was canceled: %+v", resp)
				return
			}

			for _, ev := range resp.Events {
				if ev.Type != mvccpb.DELETE {
					continue
				}

				key := string(ev.Kv.Key)

				if !strings.HasPrefix(key, archiveClaimPrefix) {
					continue
				}

				idString := strings.TrimPrefix(key, archiveClaimPrefix)

				tid, err := uuid.Parse(idString)
				if err != nil {
					a.log.Errorf("invalid transfer id in archive claim key[%s]: %v", key, err)
					continue
				}

				a.queueArchive(tid)
			}

		case <-ctx.Done():
			a.log.Infof("stopped watching archive claims")
			return
		}
	}
}

func (a *Archiver) queueArchive(tid uuid.UUID) {
	a.jMutex.Lock()

	if a.jobs[tid] {
		a.jMutex.Unlock()
		return
	}

	a.jobs[tid] = true
	a.jMutex.Unlock()

	it := proto.IncompleteTransfer(&proto.TransferDetails{
		TransferID: tid.String(),
	})

	go a.archiveTransfer(it, tid)
}
