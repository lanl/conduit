// Copyright 2026. Triad National Security, LLC. All rights reserved.

package etcd

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/andybalholm/brotli"
	"github.com/google/uuid"
	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/defaults"
	"github.com/lanl/conduit/internal/fta/plugin"
	"github.com/lanl/conduit/internal/logger"
	"github.com/spf13/viper"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	ClientCreationRetry = 5
	OpChunkSize         = 100
)

type StateType int

const (
	Transfer StateType = iota
	Archive
)

var (
	ErrNotFound     = errors.New("not found in ETCD")
	ErrRevCompacted = "etcdserver: mvcc: required revision has been compacted"
)

type ETCDManager struct {
	log *logger.ConduitLogger

	client *clientv3.Client
	// The client needs to refresh auth after any policy changes. This mutex will be used to write lock the client during policy changes
	cmutex sync.RWMutex

	tclients map[uuid.UUID]chan clientv3.WatchResponse
	tmutex   sync.RWMutex

	lclients map[uuid.UUID]chan clientv3.WatchResponse
	lmutex   sync.RWMutex

	eclients map[uuid.UUID]chan clientv3.WatchResponse
	emutex   sync.RWMutex
}

// NewETCDManager creates a new ETCDManager instance that can be used to interact with ETCD.
//
// NOTE: requires a CertManager to generate TLS client certs with the CertManager's CA.
func NewETCDManager(log *logger.ConduitLogger, tlsCert *tls.Certificate, certPool *x509.CertPool, etcdEndpoints []string) *ETCDManager {
	// change prefix for logger
	l := logger.NewConduitLogger(log.GetLevel(), fmt.Sprintf("%sETCD manager:", log.GetPrefix()))
	if log.GetPrefix() == "" {
		l = logger.NewConduitLogger(log.GetLevel(), "ETCD manager:")
	}

	em := &ETCDManager{
		log:      l,
		tclients: make(map[uuid.UUID]chan clientv3.WatchResponse),
		lclients: make(map[uuid.UUID]chan clientv3.WatchResponse),
		eclients: make(map[uuid.UUID]chan clientv3.WatchResponse),
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{*tlsCert},
		RootCAs:      certPool,
		MinVersion:   tls.VersionTLS12, // for some reason etcd uses tls 1.2 instead of 1.3
		MaxVersion:   tls.VersionTLS13,
	}

	// sometimes the auth policy changes during our request for an etcd client. We'll retry this before giving up
	// this auth issue goes deeper than I was expecting. I'm hoping this PR will fix this issue:
	// https://github.com/etcd-io/etcd/pull/13262
	// issue described here: https://github.com/etcd-io/etcd/issues/13300
	em.log.Debug("creating etcd client")
	eConfig := clientv3.Config{
		Endpoints:   etcdEndpoints,
		DialTimeout: defaults.DefaultETCDTimeout,
		TLS:         tlsConfig,
		DialOptions: []grpc.DialOption{
			grpc.WithReturnConnectionError(),
			grpc.WithBlock(),
			grpc.FailOnNonTempDialError(true),
		},
		// Logger: zLogger,
	}
	c, err := clientv3.New(eConfig)
	if err != nil {
		em.log.Fatalf("failed to create etcd client: %v etcd client config: %+v", err, eConfig)
	}

	em.client = c

	return em
}

func (em *ETCDManager) CloseClient() {
	em.client.Close()
}

// GetStatus returns the status of the provided etcd endpoint as well as the compact revision number
func (em *ETCDManager) GetStatus(etcdEndpoint string) (*clientv3.StatusResponse, int64, error) {
	maint := clientv3.NewMaintenance(em.client)

	ctx, cancel := context.WithTimeout(context.Background(), defaults.DefaultETCDTimeout)

	em.cmutex.RLock()
	defer em.cmutex.RUnlock()

	sResp, err := maint.Status(ctx, etcdEndpoint)
	cancel()
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get etcd status: %v", err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), defaults.DefaultETCDTimeout)
	hResp, err := maint.HashKV(ctx, etcdEndpoint, 0)
	cancel()
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get etcd HashKV: %v", err)
	}

	// sometimes etcd can return a compact revision of -1?
	cr := int64(0)
	if hResp.CompactRevision > 0 {
		cr = hResp.CompactRevision
	}

	return sResp, cr, nil
}

func (em *ETCDManager) GetWatchChannel(key string) (clientv3.WatchChan, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	em.cmutex.RLock()
	lch := em.client.Watch(ctx, key, clientv3.WithPrevKV())
	em.cmutex.RUnlock()
	return lch, cancel
}

func (em *ETCDManager) GetWatchChannelPrefix(key string, rev int64) (clientv3.WatchChan, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	em.cmutex.RLock()
	lch := em.client.Watch(ctx, key, clientv3.WithPrefix(), clientv3.WithPrevKV(), clientv3.WithRev(rev))
	em.cmutex.RUnlock()
	return lch, cancel
}

func (em *ETCDManager) GetWatchChannelRev(key string, rev int64) (clientv3.WatchChan, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	em.cmutex.RLock()
	lch := em.client.Watch(ctx, key, clientv3.WithRev(rev))
	em.cmutex.RUnlock()
	return lch, cancel
}

func (em *ETCDManager) Put(key, value string) (*clientv3.PutResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaults.DefaultETCDTimeout)
	em.cmutex.RLock()
	resp, err := em.client.Put(ctx, key, value)
	em.cmutex.RUnlock()
	cancel()
	return resp, err
}

func (em *ETCDManager) Txn() (clientv3.Txn, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), defaults.DefaultETCDTimeout)
	em.cmutex.RLock()
	txn := em.client.Txn(ctx)
	em.cmutex.RUnlock()
	return txn, cancel
}

func (em *ETCDManager) Get(key string) (*clientv3.GetResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaults.DefaultETCDTimeout)
	em.cmutex.RLock()
	resp, err := em.client.Get(ctx, key, clientv3.WithPrevKV())
	em.cmutex.RUnlock()
	cancel()
	return resp, err
}

func (em *ETCDManager) GetPrefix(prefix string) (*clientv3.GetResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaults.DefaultETCDTimeout)
	// resp, err := em.client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByCreateRevision, clientv3.SortAscend))
	em.cmutex.RLock()
	resp, err := em.client.Get(ctx, prefix, clientv3.WithPrefix())
	em.cmutex.RUnlock()
	cancel()
	return resp, err
}

func (em *ETCDManager) GetPrefixRev(key string, rev int64) (*clientv3.GetResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaults.DefaultETCDTimeout)
	em.cmutex.RLock()
	resp, err := em.client.Get(ctx, key, clientv3.WithPrefix(), clientv3.WithRev(rev))
	em.cmutex.RUnlock()
	cancel()
	return resp, err
}

func (em *ETCDManager) DeletePrefix(prefix string) (*clientv3.DeleteResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaults.DefaultETCDTimeout)
	em.cmutex.RLock()
	resp, err := em.client.Delete(ctx, prefix, clientv3.WithPrefix())
	em.cmutex.RUnlock()
	cancel()
	return resp, err
}

func (em *ETCDManager) Delete(key string) (*clientv3.DeleteResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaults.DefaultETCDTimeout)
	em.cmutex.RLock()
	resp, err := em.client.Delete(ctx, key)
	em.cmutex.RUnlock()
	cancel()
	return resp, err
}

// DeleteTransfer will delete all values for a transfer in etcd
func (em *ETCDManager) DeleteTransfer(id uuid.UUID) (int64, error) {
	prefix := proto.TransferPrefix + id.String()
	resp, err := em.RetryDeletePrefix(prefix, defaults.MaxRetries, defaults.RetryDelay)

	return resp.Deleted, err
}

// DeleteTransferLease will delete all lease values for a transfer in etcd
func (em *ETCDManager) DeleteTransferLease(it proto.IncompleteTransfer) (int64, error) {
	resp, err := em.RetryDeletePrefix(it.ETCDLeaseListKey(), defaults.MaxRetries, defaults.RetryDelay)

	return resp.Deleted, err
}

// GetTransfer will get all values for a transfer
func (em *ETCDManager) GetTransfer(id uuid.UUID) (*proto.TransferDetails, proto.Error, error) {
	prefix := proto.TransferPrefix + id.String()
	resp, err := em.RetryGetPrefix(prefix, defaults.MaxRetries, defaults.RetryDelay)
	if err != nil {
		return nil, proto.Error_ERROR_ETCD_CONNECTION, err
	}

	t, err := ParseETCDTransfer(id, resp.Kvs, nil)
	if err != nil {
		return nil, proto.Error_ERROR_CONDUIT_INTERNAL, fmt.Errorf("error parsing etcd transfer: %v", err)
	}

	return t, proto.Error_ERROR_NONE, nil
}

// GetTransferUser will get the transfer's user from etcd
func (em *ETCDManager) GetTransferUser(it proto.IncompleteTransfer) (string, error) {
	resp, err := em.RetryGet(it.ETCDUserKey(), defaults.MaxRetries, defaults.RetryDelay)
	if err != nil {
		return "", err
	}

	if len(resp.Kvs) < 1 {
		return "", ErrNotFound
	}

	if string(resp.Kvs[0].Value) == "" {
		return "", fmt.Errorf("etcd returned empty user")
	}

	return string(resp.Kvs[0].Value), nil
}

// GetTransferUser will get the transfer's user from etcd
func (em *ETCDManager) GetTransferWarnings(it proto.IncompleteTransfer) ([]string, error) {
	resp, err := em.RetryGet(it.ETCDWarningsKey(), defaults.MaxRetries, defaults.RetryDelay)
	if err != nil {
		return nil, err
	}

	if len(resp.Kvs) < 1 {
		return nil, fmt.Errorf("etcd returned 0 KVs")
	}

	if string(resp.Kvs[0].Value) == "" {
		return nil, fmt.Errorf("etcd returned empty warnings")
	}

	warnList := []string{}
	err = json.Unmarshal(resp.Kvs[0].Value, &warnList)
	if err != nil {
		return nil, err
	}

	return warnList, nil
}

// GetTransferState will get the transfer state from etcd
func (em *ETCDManager) GetTransferState(t proto.IncompleteTransfer) (proto.TransferState, error) {
	return em.getTState(t.ETCDStateKey())
}

// GetTransferPausedState will get the transfer paused state from etcd
func (em *ETCDManager) GetTransferPausedState(t proto.IncompleteTransfer) (proto.TransferState, error) {
	return em.getTState(t.ETCDPausedStateKey())
}

func (em *ETCDManager) getTState(etcdKey string) (proto.TransferState, error) {
	resp, err := em.Get(etcdKey)
	if err != nil {
		return proto.TransferState_TRANSFER_NONE, err
	}

	if len(resp.Kvs) < 1 {
		return proto.TransferState_TRANSFER_NONE, ErrNotFound
	}

	if state, ok := proto.TransferState_value[string(resp.Kvs[0].Value)]; !ok {
		return proto.TransferState_TRANSFER_NONE, fmt.Errorf("could not convert [%v] to transfer state", string(resp.Kvs[0].Value))
	} else {
		return proto.TransferState(state), nil
	}
}

// GetExpiry will get the transfer expiry from etcd
func (em *ETCDManager) GetExpiry(it proto.IncompleteTransfer) (time.Time, error) {
	resp, err := em.RetryGet(it.ETCDExpiryKey(), defaults.MaxRetries, defaults.RetryDelay)
	if err != nil {
		return time.Time{}, err
	}

	if len(resp.Kvs) < 1 {
		return time.Time{}, ErrNotFound
	}

	expiryTime, err := time.Parse(time.RFC3339, string(resp.Kvs[0].Value))
	if err != nil {
		return time.Time{}, fmt.Errorf("error parsing expiry from etcd[%s]: %v", string(resp.Kvs[0].Value), err)
	}

	return expiryTime, nil
}

// GetActive will get the transfer active state from etcd
func (em *ETCDManager) GetActive(it proto.IncompleteTransfer) (bool, error) {
	resp, err := em.RetryGet(it.ETCDActiveKey(), defaults.MaxRetries, defaults.RetryDelay)
	if err != nil {
		return false, err
	}

	if len(resp.Kvs) < 1 {
		return false, ErrNotFound
	}

	active, err := strconv.ParseBool(string(resp.Kvs[0].Value))
	if err != nil {
		return false, fmt.Errorf("failed to parse bool from string[%v]: %v", string(resp.Kvs[0].Value), err)
	}

	return active, nil
}

// GetPriority will get the transfer priority from etcd
func (em *ETCDManager) GetPriority(it proto.IncompleteTransfer) (uint32, error) {
	resp, err := em.RetryGet(it.ETCDPriorityKey(), defaults.MaxRetries, defaults.RetryDelay)
	if err != nil {
		return 0, err
	}

	if len(resp.Kvs) < 1 {
		return 0, ErrNotFound
	}

	priority, err := strconv.Atoi(string(resp.Kvs[0].Value))
	if err != nil {
		return 0, fmt.Errorf("failed to convert string from etcd to int[%v]: %v", string(resp.Kvs[0].Value), err)
	}

	return uint32(priority), nil
}

// GetCreatedTime will get the transfer's created time from etcd
func (em *ETCDManager) GetCreatedTime(it proto.IncompleteTransfer) (*timestamppb.Timestamp, error) {
	resp, err := em.RetryGet(it.ETCDCreatedTimeKey(), defaults.MaxRetries, defaults.RetryDelay)
	if err != nil {
		return nil, err
	}

	if len(resp.Kvs) < 1 {
		return nil, ErrNotFound
	}

	createdTime, err := time.Parse(time.RFC3339, string(resp.Kvs[0].Value))
	if err != nil {
		return nil, fmt.Errorf("failed to parse createdTime: %v", err)
	}

	return timestamppb.New(createdTime), nil
}

// GetAllTransfers will get all transfers in etcd
func (em *ETCDManager) GetAllTransfers(cRev int64) (map[string]*proto.TransferDetails, error) {
	em.log.Debugf("Retrieving all existing transfers in etcd...")
	if cRev == 0 {
		em.log.Debugf("cannot use 0 as a prefix revision, using 1 instead")
		cRev = 1
	}
	wc, cancel := em.GetWatchChannelPrefix(proto.TransferPrefix, cRev)
	events := []*clientv3.Event{}
watchChannelLoop:
	for {
		select {
		case wresp := <-wc:
			for _, e := range wresp.Events {
				events = append(events, e)
				// em.log.Debugf("%v %v", e.Kv.CreateRevision, e.Kv.ModRevision)
				if e.Kv.ModRevision == wresp.Header.Revision {
					em.log.Debugf("found the last key! %v %v %v", e.Kv.CreateRevision, e.Kv.ModRevision, string(e.Kv.Key))
					break watchChannelLoop
				}
				if e.Kv.ModRevision > wresp.Header.Revision {
					em.log.Debugf("found newer key! %v %v %v %v", e.Kv.CreateRevision, e.Kv.ModRevision, wresp.Header.Revision, string(e.Kv.Key))
					break watchChannelLoop
				}
			}
		case <-time.After(5 * time.Second):
			em.log.Debugf("didn't get any transfers after 5 seconds, continuing...")
			break watchChannelLoop
		}
	}
	cancel()

	em.log.Debugf("found %v events in etcd", len(events))
	em.log.Debugf("converting all etcd events into transfers...")

	transfers, err := ParseETCDTransfers(events)
	if err != nil {
		return nil, fmt.Errorf("failed to parse etcd transfers: %v", err)
	}

	return transfers, nil
}

func (em *ETCDManager) CompactRevision(rev int64) (curRev int64, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaults.DefaultETCDTimeout)
	em.cmutex.RLock()
	resp, err := em.client.Compact(ctx, rev)
	em.cmutex.RUnlock()
	cancel()
	if resp != nil && resp.Header != nil {
		curRev = resp.Header.GetRevision()
	} else {
		curRev = -1
	}
	if err != nil {
		if err.Error() == ErrRevCompacted {
			return curRev, err
		}
		return curRev, fmt.Errorf("failed to compact etcd at rev[%v]: %v", rev, err)
	}

	return curRev, nil
}

// CompleteTransfer DOES NOT require a full transferDetails object including all leases
func (em *ETCDManager) CompleteTransfer(t proto.IncompleteTransfer) error {

	var comparisons *[]clientv3.Cmp

	actions := []clientv3.Op{}
	actions = append(actions, clientv3.OpPut(t.ETCDActiveKey(), strconv.FormatBool(false)))
	actions = append(actions, clientv3.OpPut(t.ETCDArchiveStateKey(), proto.ArchiveState_ARCHIVE_READY.String()))
	actions = append(actions, clientv3.OpDelete(t.ETCDLeaseListKey(), clientv3.WithPrefix()))
	actions = append(actions, clientv3.OpPut(t.ETCDEndTimeKey(), timestamppb.Now().AsTime().Format(time.RFC3339)))
	actions = append(actions, clientv3.OpDelete(t.ETCDJobsKey(), clientv3.WithPrefix()))

	resp, err := em.RetryTxn(comparisons, &actions, defaults.MaxRetries, defaults.RetryDelay)
	if err != nil {
		return fmt.Errorf("error while setting active key to false for transfer[%s]: %v", t.GetTransferID(), err)
	}
	if !resp.Succeeded {
		return fmt.Errorf("failed to set active key to false for transfer[%s]: %v", t.GetTransferID(), resp.Responses)
	}

	return nil
}

// SafelyAddErr will attempt to set the error state safely by checking if there is already an error
func (em *ETCDManager) SafelyAddErr(id proto.IncompleteTransfer, errorState proto.Error, errorMessage error) (successful bool, pErr proto.Error, err error) {
	return em.safelyAddErr(id, errorState, errorMessage, Transfer)
}

// safelyAddErr will attempt to set the error state safely by checking for existing errors
func (em *ETCDManager) safelyAddErr(it proto.IncompleteTransfer, errorState proto.Error, errorMessage error, errorType StateType) (bool, proto.Error, error) {
	errKey := ""
	errMessageKey := ""
	switch errorType {
	case Transfer:
		errKey = it.ETCDErrorKey()
		errMessageKey = it.ETCDErrorMessageKey()
	default:
		return false, proto.Error_ERROR_CONDUIT_INTERNAL, fmt.Errorf("error failed to find correct error to change for transfer[%s]: %v", it.GetTransferID(), errorType)
	}

	for i := 1; i <= defaults.MaxRetries; i++ {
		txn, cancel := em.Txn()
		txn.If(clientv3.Compare(clientv3.Value(errKey), "=", proto.Error_ERROR_NONE.String()))
		actions := []clientv3.Op{}
		actions = append(actions, clientv3.OpPut(errKey, errorState.String()))
		actions = append(actions, clientv3.OpPut(errMessageKey, errorMessage.Error()))
		txn.Then(actions...)

		resp, err := txn.Commit()
		cancel()

		if err != nil {
			if i == defaults.MaxRetries {
				return false, proto.Error_ERROR_ETCD_CONNECTION, fmt.Errorf("error committing to etcd for transfer[%s]: %v", it.GetTransferID(), err)
			} else {
				time.Sleep(defaults.RetryDelay)
				continue
			}
		}
		if resp == nil {
			if i == defaults.MaxRetries {
				em.log.Errorf("response from etcd was nil, that's not supposed to happen")
				return false, proto.Error_ERROR_ETCD_INTERNAL, fmt.Errorf("response from etcd was nil, that's not supposed to happen")
			} else {
				time.Sleep(defaults.RetryDelay)
				continue
			}
		}
		if !resp.Succeeded {
			return false, proto.Error_ERROR_NONE, nil
		}

		return true, proto.Error_ERROR_NONE, nil
	}

	return false, proto.Error_ERROR_CONDUIT_INTERNAL, fmt.Errorf("etcd was never contacted to safely set err state for transfer[%s]", it.GetTransferID())
}

// ForceAddErr will set the transfer error state and message without checking the current state
func (em *ETCDManager) ForceAddErr(id proto.IncompleteTransfer, errorState proto.Error, errorMessage error) (bool, proto.Error, error) {
	return em.forceAddErr(id, errorState, errorMessage, Transfer)
}

// forceAddErr will set the error and error message without checking the current state
func (em *ETCDManager) forceAddErr(it proto.IncompleteTransfer, errorState proto.Error, errorMessage error, errorType StateType) (bool, proto.Error, error) {
	errKey := ""
	errMessageKey := ""
	switch errorType {
	case Transfer:
		errKey = it.ETCDErrorKey()
		errMessageKey = it.ETCDErrorMessageKey()
	default:
		return false, proto.Error_ERROR_CONDUIT_INTERNAL, fmt.Errorf("error failed to find correct error to change for transfer[%s]: %v", it.GetTransferID(), errorType)
	}

	for i := 1; i <= defaults.MaxRetries; i++ {
		txn, cancel := em.Txn()

		actions := []clientv3.Op{}
		actions = append(actions, clientv3.OpPut(errKey, errorState.String()))
		actions = append(actions, clientv3.OpPut(errMessageKey, errorMessage.Error()))
		txn.Then(actions...)

		resp, err := txn.Commit()
		cancel()

		if err != nil {
			if i == defaults.MaxRetries {
				return false, proto.Error_ERROR_ETCD_CONNECTION, fmt.Errorf("error committing to etcd for transfer[%s]: %v", it.GetTransferID(), err)
			} else {
				time.Sleep(defaults.RetryDelay)
				continue
			}
		}
		if resp == nil {
			if i == defaults.MaxRetries {
				em.log.Errorf("response from etcd was nil, that's not supposed to happen")
				return false, proto.Error_ERROR_ETCD_INTERNAL, fmt.Errorf("response from etcd was nil, that's not supposed to happen")
			} else {
				time.Sleep(defaults.RetryDelay)
				continue
			}
		}
		if !resp.Succeeded {
			return false, proto.Error_ERROR_NONE, nil
		}

		return true, proto.Error_ERROR_NONE, nil
	}

	return false, proto.Error_ERROR_CONDUIT_INTERNAL, fmt.Errorf("etcd was never contacted to safely set err state for transfer[%s]", it.GetTransferID())
}

// FailTransfer DOES NOT require a full transferDetails object including all leases
func (em *ETCDManager) AbortTransfer(t *proto.TransferDetails, errorMessage error) error {
	comparisons := []clientv3.Cmp{}
	comparisons = append(comparisons, clientv3.Compare(clientv3.Value(t.ETCDStateKey()), "!=", proto.TransferState_TRANSFER_ERROR.String()))
	comparisons = append(comparisons, clientv3.Compare(clientv3.Value(t.ETCDStateKey()), "!=", proto.TransferState_TRANSFER_ABORTED.String()))
	comparisons = append(comparisons, clientv3.Compare(clientv3.Value(t.ETCDStateKey()), "!=", proto.TransferState_TRANSFER_ABORT.String()))

	actions := []clientv3.Op{}
	actions = append(actions, clientv3.OpPut(t.ETCDStateKey(), proto.TransferState_TRANSFER_ABORT.String()))
	actions = append(actions, clientv3.OpPut(t.ETCDErrorKey(), proto.Error_ERROR_ABORTED.String()))
	actions = append(actions, clientv3.OpPut(t.ETCDErrorMessageKey(), errorMessage.Error()))

	resp, err := em.RetryTxn(&comparisons, &actions, defaults.MaxRetries, defaults.RetryDelay)
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		comparisons := []clientv3.Cmp{}

		actions := []clientv3.Op{}
		actions = append(actions, clientv3.OpPut(t.ETCDActiveKey(), strconv.FormatBool(false)))
		actions = append(actions, clientv3.OpDelete(t.ETCDLeaseListKey(), clientv3.WithPrefix()))

		resp, err := em.RetryTxn(&comparisons, &actions, defaults.MaxRetries, defaults.RetryDelay)
		if err != nil {
			em.log.Errorf("failed to remove lease from etcd for transfer[%s]: %v", t.GetTransferID(), err)
		}
		if !resp.Succeeded {
			em.log.Errorf("failed to remove lease from etcd for transfer[%s]: it has recoverable paths", t.GetTransferID())
		}

		return fmt.Errorf("failed to set abort state for transfer[%s], has it already completed?: %v", t.GetTransferID(), resp.Responses)
	}
	return nil
}

// SubmitTransfer is the initial submission into ETCD
func (em *ETCDManager) SubmitTransfer(t *proto.TransferDetails) {
	ops, err := ConvertETCDTransfer(t)
	if err != nil {
		tErr := fmt.Errorf("transfer[%s]: failed to get ops for etcd: %v", t.GetTransferID(), err)
		em.log.Error(tErr)
		fErr := em.CompleteTransfer(t)
		if fErr != nil {
			em.log.Errorf("failed to fail transfer[%s]: %v", t.GetTransferID(), fErr)
		}
		return
	}

	// split ops into chunks. ETCD has a limit of how many operations you can do per transfer. ETCD's default is 128
	opsChunks := [][]clientv3.Op{}
	for i := 0; i < len(ops); i += OpChunkSize {
		end := i + OpChunkSize
		if end > len(ops) {
			end = len(ops)
		}
		opsChunks = append(opsChunks, ops[i:end])
	}

	// set transfer state INIT_COMPLETE to trigger the transfer worker
	// these need to be the last operations because anything after it might not get picked up by the transfer workers
	fOp := clientv3.OpPut(t.ETCDStateKey(), proto.TransferState_TRANSFER_INIT_COMPLETE.String())

	// add the final ops chunks to the end of the opschunks
	opsChunks = append(opsChunks, []clientv3.Op{fOp})

	// send the chunks to etcd
	for ci, c := range opsChunks {
		em.log.Debugf("transfer[%s]: sending chunk %v of %v", t.GetTransferID(), ci, len(opsChunks))

		resp, err := em.RetryTxn(nil, &c, defaults.MaxRetries, defaults.RetryDelay)
		if err != nil {
			tErr := fmt.Errorf("transfer[%s]: error while submitting transfer: %v", t.GetTransferID(), err)
			em.log.Error(tErr)
			fErr := em.CompleteTransfer(t)
			if fErr != nil {
				em.log.Errorf("failed to fail transfer[%s]: %v", t.GetTransferID(), fErr)
			}
			return
		}
		if !resp.Succeeded {
			tErr := fmt.Errorf("transfer[%s]: if condition failed to etcd transaction while submitting transfer to etcd", t.GetTransferID())
			em.log.Error(tErr)
			fErr := em.CompleteTransfer(t)
			if fErr != nil {
				em.log.Errorf("failed to fail transfer[%s]: %v", t.GetTransferID(), fErr)
			}
		}
	}
}

// PauseTransfer sets the paused state value for a transfer in ETCD
func (em *ETCDManager) PauseTransfer(t *proto.TransferDetails, ps proto.TransferState) (bool, error) {
	txn, cancel := em.Txn()
	actions := []clientv3.Op{}
	actions = append(actions, clientv3.OpPut(t.ETCDPausedStateKey(), ps.String()))

	txn.Then(actions...)

	resp, err := txn.Commit()
	cancel()

	if err != nil {
		return false, fmt.Errorf("error committing pause to etcd for transfer[%s]: %v", t.GetTransferID(), err)
	}
	if resp == nil {
		em.log.Fatalf("response from etcd was nil, that's not supposed to happen")
		return false, fmt.Errorf("response from etcd was nil, that's not supposed to happen")
	}
	if !resp.Succeeded {
		return false, nil
	}

	return true, nil
}

// SafelySetTransferState will attempt to set the transfer state safely by comparing the transfer state it should be coming from
func (em *ETCDManager) SafelySetTransferState(t proto.IncompleteTransfer, fromState proto.TransferState, toState proto.TransferState) (bool, proto.Error, error) {
	return em.safelySetState(t, fromState, toState, Transfer)
}

// SafelySetTransferArchiveState will attempt to set the transfer archive state safely by comparing the transfer archive state it should be coming from
func (em *ETCDManager) SafelySetTransferArchiveState(t proto.IncompleteTransfer, fromState proto.ArchiveState, toState proto.ArchiveState) (bool, proto.Error, error) {
	return em.safelySetState(t, fromState, toState, Archive)
}

// safelySetState will attempt to set a state safely by comparing the previous state it should be coming from
func (em *ETCDManager) safelySetState(t proto.IncompleteTransfer, fromState proto.StringableState, toState proto.StringableState, stateType StateType) (bool, proto.Error, error) {
	key := ""
	switch stateType {
	case Transfer:
		key = t.ETCDStateKey()
	case Archive:
		key = t.ETCDArchiveStateKey()
	default:
		return false, proto.Error_ERROR_CONDUIT_INTERNAL, fmt.Errorf("error failed to find correct state to change for transfer[%s]: %v", t.GetTransferID(), stateType)
	}

	for i := 1; i <= defaults.MaxRetries; i++ {
		newExpiry := timestamppb.New(time.Now().Add(viper.GetDuration(defaults.ConfigExpiryAdvanceKey)))

		txn, cancel := em.Txn()
		txn.If(clientv3.Compare(clientv3.Value(key), "=", fromState.String()))
		txn.Then(
			clientv3.OpPut(key, toState.String()),
			clientv3.OpPut(t.ETCDExpiryKey(), newExpiry.AsTime().Format(time.RFC3339)),
		)

		resp, err := txn.Commit()
		cancel()

		if err != nil {
			if i == defaults.MaxRetries {
				return false, proto.Error_ERROR_ETCD_CONNECTION, fmt.Errorf("error committing to etcd for transfer[%s]: %v", t.GetTransferID(), err)
			} else {
				time.Sleep(defaults.RetryDelay)
				continue
			}
		}
		if resp == nil {
			if i == defaults.MaxRetries {
				em.log.Fatalf("response from etcd was nil, that's not supposed to happen")
				return false, proto.Error_ERROR_ETCD_CONNECTION, fmt.Errorf("response from etcd was nil, that's not supposed to happen")
			} else {
				time.Sleep(defaults.RetryDelay)
				continue
			}
		}
		if !resp.Succeeded {
			return false, proto.Error_ERROR_NONE, nil
		}

		return true, proto.Error_ERROR_NONE, nil
	}

	return false, proto.Error_ERROR_CONDUIT_INTERNAL, fmt.Errorf("etcd was never contacted to safely set state for transfer[%s]", t.GetTransferID())
}

// ForceSetTransferState will set the transfer state without checking the current state
func (em *ETCDManager) ForceSetTransferState(t proto.IncompleteTransfer, state proto.TransferState) error {
	return em.forceSetState(t, state, Transfer)
}

// ForceSetTransferArchiveState will set the transfer archive state without checking the current state
func (em *ETCDManager) ForceSetTransferArchiveState(t proto.IncompleteTransfer, state proto.TransferArchiveState) error {
	return em.forceSetState(t, state, Archive)
}

// safelySetState will attempt to set a state safely by comparing the previous state it should be coming from
func (em *ETCDManager) forceSetState(t proto.IncompleteTransfer, state proto.StringableState, stateType StateType) error {
	key := ""
	switch stateType {
	case Transfer:
		key = t.ETCDStateKey()
	case Archive:
		key = t.ETCDArchiveStateKey()
	default:
		return fmt.Errorf("error failed to find correct state to change for transfer[%s]: %v", t.GetTransferID(), stateType)
	}

	var fErr error
	for i := 1; i <= defaults.MaxRetries; i++ {
		_, err := em.Put(key, state.String())
		if err == nil {
			return nil
		}
		fErr = err
	}

	return fmt.Errorf("error committing to etcd for transfer[%s]: %v", t.GetTransferID(), fErr)
}

// ParseCertFromBytes will parse a tls certificate from an array of bytes. This is
// primarily used in the FTA code
func ParseCertFromBytes(pemBytes []byte) (*tls.Certificate, uuid.UUID, error) {
	tlsCert, err := tls.X509KeyPair(pemBytes, pemBytes)
	if err != nil {
		return nil, uuid.Nil, fmt.Errorf("failed to create tls cert: %v", err)
	}

	cert, err := x509.ParseCertificate(tlsCert.Certificate[0])
	if err != nil {
		return nil, uuid.Nil, fmt.Errorf("failed to parse x509 cert from tlscert: %v", err)
	}

	id, err := uuid.Parse(cert.Subject.CommonName)
	if err != nil {
		return nil, uuid.Nil, fmt.Errorf("failed to parse uuid from subject common name: %v", err)
	}

	return &tlsCert, id, nil
}

// RetryTxn will create a transaction and commit it a set number of times
func (em *ETCDManager) RetryTxn(compare *[]clientv3.Cmp, actions *[]clientv3.Op, retryCount int, sleepDur time.Duration) (*clientv3.TxnResponse, error) {
	var resp *clientv3.TxnResponse
	var err error

	for i := 1; i <= retryCount; i++ {
		txn, cancel := em.Txn()
		if compare != nil {
			txn.If(*compare...)
		}
		if actions != nil {
			txn.Then(*actions...)
		}

		resp, err = txn.Commit()
		cancel()
		if err == nil {
			// txn was successful
			return resp, nil
		}
		if i != retryCount {
			time.Sleep(sleepDur)
		}
	}

	return nil, fmt.Errorf("retried txn %v times. Giving up: %v", retryCount, err)
}

// RetryGet will try to get a set number of times
func (em *ETCDManager) RetryGet(key string, retryCount int, sleepDur time.Duration) (*clientv3.GetResponse, error) {
	var resp *clientv3.GetResponse
	var err error

	for i := 1; i <= retryCount; i++ {
		resp, err = em.Get(key)
		if err == nil {
			return resp, nil
		}
		if i != retryCount {
			time.Sleep(sleepDur)
		}
	}

	em.log.Errorf("error getting[%v] from etcd: %v", key, err)
	return nil, fmt.Errorf("retried get %v times. Giving up: %v", retryCount, err)
}

// RetryGetPrefix will try to get a prefix a set number of times
func (em *ETCDManager) RetryGetPrefix(prefix string, retryCount int, sleepDur time.Duration) (*clientv3.GetResponse, error) {
	var resp *clientv3.GetResponse
	var err error

	for i := 1; i <= retryCount; i++ {
		resp, err = em.GetPrefix(prefix)
		if err == nil {
			return resp, nil
		}
		if i != retryCount {
			time.Sleep(sleepDur)
		}
	}

	em.log.Errorf("error getting prefix[%v] from etcd: %v", prefix, err)
	return nil, fmt.Errorf("retried get prefix %v times. Giving up: %v", retryCount, err)
}

// RetryGetPrefixRev will try to get a prefix a set number of times at a certain revision
func (em *ETCDManager) RetryGetPrefixRev(prefix string, rev int64, retryCount int, sleepDur time.Duration) (*clientv3.GetResponse, error) {
	var resp *clientv3.GetResponse
	var err error

	for i := 1; i <= retryCount; i++ {
		resp, err = em.GetPrefixRev(prefix, rev)
		if err == nil {
			return resp, nil
		}
		if i != retryCount {
			time.Sleep(sleepDur)
		}
	}

	em.log.Errorf("error getting prefix[%v] from etcd: %v", prefix, err)
	return nil, fmt.Errorf("retried get prefix %v times. Giving up: %v", retryCount, err)
}

// RetryPut will try to put a set number of times
func (em *ETCDManager) RetryPut(key, value string, retryCount int, sleepDur time.Duration) (*clientv3.PutResponse, error) {
	var resp *clientv3.PutResponse
	var err error

	for i := 1; i <= retryCount; i++ {
		resp, err = em.Put(key, value)
		if err == nil {
			return resp, nil
		}
		if i != retryCount {
			time.Sleep(sleepDur)
		}
	}

	em.log.Errorf("error putting[%v](%s) into etcd: %v", key, value, err)
	return nil, fmt.Errorf("retried put %v times. Giving up: %v", retryCount, err)
}

func (em *ETCDManager) AddWarnings(it proto.IncompleteTransfer, newWarnings []string) error {
	succeed := false
	var err error

	for i := 1; i <= defaults.MaxRetries; i++ {
		oldWarnings, err := em.GetTransferWarnings(it)
		if err == nil {
			oldWarningsJson, err := json.Marshal(oldWarnings)
			if err != nil {
				return fmt.Errorf("transfer[%s]: failed to marshal old transfer warnings[%v] for etcd: %v", it.GetTransferID(), oldWarnings, err)
			}

			newWarnings := append(oldWarnings, newWarnings...)
			newWarningsJson, err := json.Marshal(newWarnings)
			if err != nil {
				return fmt.Errorf("transfer[%s]: failed to marshal new transfer warnings[%v] for etcd: %v", it.GetTransferID(), newWarnings, err)
			}

			comparisons := &[]clientv3.Cmp{clientv3.Compare(clientv3.Value(it.ETCDWarningsKey()), "=", string(oldWarningsJson))}
			actions := &[]clientv3.Op{clientv3.OpPut(it.ETCDWarningsKey(), string(newWarningsJson))}

			resp, err := em.RetryTxn(comparisons, actions, defaults.MaxRetries, defaults.RetryDelay)
			if resp != nil {
				succeed = resp.Succeeded
			}
			if err == nil && resp.Succeeded {
				return nil
			}
		}
		if i != defaults.MaxRetries {
			time.Sleep(defaults.RetryDelay)
		}
	}

	fErr := fmt.Errorf("error adding warning to transfer[%s] (succeeded: %v) in etcd: %v", it.GetTransferID(), succeed, err)
	em.log.Error(fErr)
	return fErr
}

// RetryDeletePrefix will try to delete a prefix a set number of times
func (em *ETCDManager) RetryDeletePrefix(prefix string, retryCount int, sleepDur time.Duration) (*clientv3.DeleteResponse, error) {
	var resp *clientv3.DeleteResponse
	var err error

	for i := 1; i <= retryCount; i++ {
		resp, err = em.DeletePrefix(prefix)
		if err == nil {
			return resp, nil
		}
		if i != retryCount {
			time.Sleep(sleepDur)
		}
	}

	em.log.Errorf("error deleting prefix[%v] from etcd: %v", prefix, err)
	return nil, fmt.Errorf("retried delete prefix %v times. Giving up: %v", retryCount, err)
}

// GetOldestRev returns the oldest revision of a provided prefix. If no prefix is provided (""), it will return the oldest revision for all keys in the database
func (em *ETCDManager) GetOldestRev(prefix string) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaults.DefaultETCDTimeout)
	var resp *clientv3.GetResponse
	var err error
	em.cmutex.RLock()
	if prefix == "" {
		// \x00 will match every key in etcd
		resp, err = em.client.Get(ctx, "\x00", clientv3.WithFromKey(), clientv3.WithLimit(1), clientv3.WithSort(clientv3.SortByCreateRevision, clientv3.SortAscend))
	} else {
		resp, err = em.client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithLimit(1), clientv3.WithSort(clientv3.SortByCreateRevision, clientv3.SortAscend))
	}
	em.cmutex.RUnlock()
	cancel()
	if err != nil {
		return 0, err
	}
	if len(resp.Kvs) < 1 {
		// etcd is empty
		// compact to the most recent revision
		return resp.Header.GetRevision(), nil
	}
	em.log.Debugf("got oldest prefix[%v] rev of %v for key: [%v] value: [%v]", prefix, resp.Kvs[0].CreateRevision, string(resp.Kvs[0].Key), string(resp.Kvs[0].Value))
	return resp.Kvs[0].CreateRevision, err
}

func (em *ETCDManager) GetOldestTransfersRev() (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaults.DefaultETCDTimeout)
	em.cmutex.RLock()
	// when keys are sorted Lexicographically, the jobs prefix will come after the errors prefix. We're trying to get the oldest revision that's not in the errors prefix
	resp, err := em.client.Get(ctx, proto.JobsPrefix, clientv3.WithFromKey(), clientv3.WithLimit(1), clientv3.WithSort(clientv3.SortByCreateRevision, clientv3.SortAscend))
	em.cmutex.RUnlock()
	cancel()
	if err != nil {
		return 0, err
	}
	if len(resp.Kvs) < 1 {
		// etcd is empty
		// compact to the most recent revision
		return resp.Header.GetRevision(), nil
	}
	em.log.Debugf("got oldest transfer rev of %v for key: [%v] value: [%v]", resp.Kvs[0].CreateRevision, string(resp.Kvs[0].Key), string(resp.Kvs[0].Value))
	return resp.Kvs[0].CreateRevision, err
}

// GetDestInfo will get the transfer destination info from etcd
func (em *ETCDManager) GetDestInfo(it proto.IncompleteTransfer) (proto.DestInfo, error) {
	resp, err := em.RetryGet(it.ETCDDestInfoKey(), defaults.MaxRetries, defaults.RetryDelay)
	if err != nil {
		return proto.DestInfo_DEST_NONE, err
	}

	if len(resp.Kvs) < 1 {
		return proto.DestInfo_DEST_NONE, ErrNotFound
	}

	di, ok := proto.DestInfo_value[string(resp.Kvs[0].Value)]
	if !ok {
		return proto.DestInfo_DEST_NONE, fmt.Errorf("failed to get destinfo value from: %v", string(resp.Kvs[0].Value))
	}

	destInfo := proto.DestInfo(di)

	return destInfo, nil
}

// GetLeases will get the transfer's leases from etcd
func (em *ETCDManager) GetLeases(it proto.IncompleteTransfer) ([]string, error) {
	resp, err := em.RetryGet(it.ETCDLeasesKey(), defaults.MaxRetries, defaults.RetryDelay)
	if err != nil {
		return []string{}, err
	}

	if len(resp.Kvs) < 1 {
		return []string{}, ErrNotFound
	}

	leases := []string{}
	err = json.Unmarshal(resp.Kvs[0].Value, &leases)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal json: %v", err)
	}

	return leases, nil
}

// GetSources will get the transfer's sources from etcd
func (em *ETCDManager) GetSources(it proto.IncompleteTransfer) ([]string, error) {
	resp, err := em.RetryGet(it.ETCDSourceKey(), defaults.MaxRetries, defaults.RetryDelay)
	if err != nil {
		return []string{}, err
	}

	if len(resp.Kvs) < 1 {
		return []string{}, ErrNotFound
	}

	sources := []string{}
	err = json.Unmarshal(resp.Kvs[0].Value, &sources)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal json: %v", err)
	}

	return sources, nil
}

// rollbackState will safely rollback the state of a transfer if it gets stuck somewhere it shouldn't
func (em *ETCDManager) RollbackState(it proto.IncompleteTransfer, fromState, toState proto.StringableState, stateType StateType, oldExpiry *time.Time) error {
	stateKey := ""

	switch stateType {
	case Transfer:
		stateKey = it.ETCDStateKey()
	case Archive:
		stateKey = it.ETCDArchiveStateKey()
	}

	// the transfer expired while waiting for lease. Push the state back to validation complete
	comparisons := []clientv3.Cmp{
		clientv3.Compare(clientv3.Value(stateKey), "=", fromState.String()),
	}

	if oldExpiry != nil {
		comparisons = append(comparisons, clientv3.Compare(clientv3.Value(it.ETCDExpiryKey()), "=", oldExpiry.Format(time.RFC3339)))
	}

	if stateType == Transfer {
		comparisons = append(comparisons, clientv3.Compare(clientv3.Value(it.ETCDErrorKey()), "=", proto.Error_ERROR_NONE.String()))
	}

	actions := &[]clientv3.Op{
		clientv3.OpPut(stateKey, toState.String()),
		clientv3.OpPut(it.ETCDExpiryKey(), time.Now().Add(viper.GetDuration(defaults.ConfigExpiryAdvanceKey)).Format(time.RFC3339)),
	}

	resp, err := em.RetryTxn(&comparisons, actions, defaults.MaxRetries, defaults.RetryDelay)
	if err != nil || !resp.Succeeded {
		return fmt.Errorf("failed to rollback state to %s for transfer[%s]: %v", toState, it.GetTransferID(), err)
	}

	em.log.Debugf("transfer[%s] successfully reverted state back to %s", it.GetTransferID(), toState)
	return nil
}

// GetStatusDetails will get the transfer's status details from etcd. Used in conduit-fta
func (em *ETCDManager) GetStatusDetails(it proto.IncompleteTransfer) (*proto.ETCDStatusDetails, error) {
	resp, err := em.RetryGet(it.ETCDStatusDetailsKey(), defaults.MaxRetries, defaults.RetryDelay)
	if err != nil {
		return nil, err
	}

	if len(resp.Kvs) < 1 {
		return nil, ErrNotFound
	}

	esd := &proto.ETCDStatusDetails{}
	err = json.Unmarshal(resp.Kvs[0].Value, &esd)
	if err != nil {
		return nil, fmt.Errorf("transfer[%s]: failed to unmarshal status details from etcd into json object: %v [%v]", it.GetTransferID(), err, string(resp.Kvs[0].Value))
	}

	return esd, nil
}

// getPluginData will retrieve and format a transfers pluginData from etcd
func (em *ETCDManager) GetPluginData(it proto.IncompleteTransfer) (*plugin.PluginData, error) {
	resp, err := em.RetryGet(it.ETCDPluginDataKey(), defaults.MaxRetries, defaults.RetryDelay)
	if err != nil {
		return nil, err
	}

	if len(resp.Kvs) < 1 {
		return nil, ErrNotFound
	}

	r := brotli.NewReader(bytes.NewReader([]byte(string(resp.Kvs[0].Value))))
	var decodedOutput bytes.Buffer
	_, err = io.Copy(&decodedOutput, r)
	if err != nil {
		return nil, fmt.Errorf("failed to decode brotli pluginData for transfer[%v]: %v", it.GetTransferID(), err)
	}

	pluginData := &plugin.PluginData{}
	err = json.Unmarshal(decodedOutput.Bytes(), pluginData)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal pluginData for transfer[%v]: %v", it.GetTransferID(), err)
	}

	return pluginData, nil
}

// GetActionAndOptions will retrieve a transfers action and options from etcd
func (em *ETCDManager) GetActionAndOptions(it proto.IncompleteTransfer) (action string, options map[string]*anypb.Any, _ error) {
	options = make(map[string]*anypb.Any)

	txnActions := []clientv3.Op{
		clientv3.OpGet(it.ETCDActionKey()),
		clientv3.OpGet(it.ETCDOptionsKey()),
	}

	resp, err := em.RetryTxn(nil, &txnActions, defaults.MaxRetries, defaults.RetryDelay)
	if err != nil {
		return "", nil, err
	}

	if len(resp.Responses) < 2 {
		return "", nil, ErrNotFound
	}

	for i, r := range resp.Responses {
		rr := r.GetResponseRange()

		switch i {
		case 0:
			if len(rr.Kvs) < 1 {
				return "", nil, fmt.Errorf("etcd didn't return an action")
			}

			action = string(rr.Kvs[0].Value)
		case 1:
			if len(rr.Kvs) < 1 {
				return "", nil, fmt.Errorf("etcd didn't return any options")
			}

			err = json.Unmarshal(rr.Kvs[0].Value, &options)
			if err != nil {
				return "", nil, fmt.Errorf("failed to unmarshal options from string[%v]: %v", rr.Kvs[0].Value, err)
			}
		}
	}

	return action, options, nil
}

// GetSourcesAndDestination will retrieve a transfers sources and destination from etcd
func (em *ETCDManager) GetSourcesAndDestination(it proto.IncompleteTransfer) (sources []string, destination string, _ error) {
	sources = []string{}

	txnActions := []clientv3.Op{
		clientv3.OpGet(it.ETCDSourceKey()),
		clientv3.OpGet(it.ETCDDestinationKey()),
	}

	resp, err := em.RetryTxn(nil, &txnActions, defaults.MaxRetries, defaults.RetryDelay)
	if err != nil {
		return nil, "", err
	}

	if len(resp.Responses) < 2 {
		return nil, "", ErrNotFound
	}

	for i, r := range resp.Responses {
		rr := r.GetResponseRange()

		switch i {
		case 0:
			if len(rr.Kvs) < 1 {
				return nil, "", fmt.Errorf("etcd didn't return any sources")
			}

			err = json.Unmarshal(rr.Kvs[0].Value, &sources)
			if err != nil {
				return nil, "", fmt.Errorf("failed to unmarshal sources from string[%v]: %v", rr.Kvs[0].Value, err)
			}
		case 1:
			if len(rr.Kvs) < 1 {
				return nil, "", fmt.Errorf("etcd didn't return a destination")
			}

			destination = string(rr.Kvs[0].Value)

		}
	}

	return sources, destination, nil
}
