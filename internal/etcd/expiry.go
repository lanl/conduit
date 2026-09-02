// Copyright 2026. Triad National Security, LLC. All rights reserved.

package etcd

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/defaults"
	"github.com/spf13/viper"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// UpdateExpiryConstantly will update a transfers expiry every 10 seconds. The new expiry will be the configured ExpiryAdvance duration from the current time
func (em *ETCDManager) UpdateExpiryConstantly(it proto.IncompleteTransfer, ctx context.Context, status string) {
	em.log.Debugf("constantly updating expiry for transfer[%v] every %v seconds", it.GetTransferID(), 10)

	_, err, _, _ := em.UpdateExpiryOnce(it, status)
	if err != nil {
		em.log.Error(err)
	}

	id := uuid.New()

	em.exmutex.Lock()
	if _, exists := em.expiries[it.GetTransferID()]; !exists {
		em.expiries[it.GetTransferID()] = make(map[uuid.UUID]proto.IncompleteTransfer)
	}

	em.expiries[it.GetTransferID()][id] = it

	em.exmutex.Unlock()

	<-ctx.Done()

	em.log.Debugf("finished constantly updating expiry for transfer[%v]", it.GetTransferID())

	em.exmutex.Lock()

	delete(em.expiries[it.GetTransferID()], id)
	if len(em.expiries[it.GetTransferID()]) == 0 {
		delete(em.expiries, it.GetTransferID())
	}

	em.exmutex.Unlock()

}

// UpdateExpiryOnce will update a transfers expiry one time. The new expiry will be the configured ExpiryAdvance duration from the current time
func (em *ETCDManager) updateExpiriesOnce(its map[string]map[uuid.UUID]proto.IncompleteTransfer) (succeeded bool, err error, newExpiry *timestamppb.Timestamp) {
	newExpiry = timestamppb.New(time.Now().Add(viper.GetDuration(defaults.ConfigExpiryAdvanceKey)))

	newExpiryTime := newExpiry.AsTime().Format(time.RFC3339)
	stringTrue := strconv.FormatBool(true)

	actions := []clientv3.Op{}

	// check if the transfer has an error before updating the expiry key
	for _, m := range its {
		tCompare := []clientv3.Cmp{}
		tActions := []clientv3.Op{}

		for _, it := range m {
			tCompare = append(tCompare,
				clientv3.Compare(clientv3.Value(it.ETCDErrorKey()), "=", proto.Error_ERROR_NONE.String()),
				clientv3.Compare(clientv3.Value(it.ETCDActiveKey()), "=", stringTrue),
			)

			tActions = append(tActions,
				clientv3.OpPut(it.ETCDExpiryKey(), newExpiryTime),
			)

			// we only need one item from this map
			break
		}

		actions = append(actions, clientv3.OpTxn(tCompare, tActions, nil))
	}

	// split ops into chunks. ETCD has a limit of how many operations you can do per transfer. ETCD's default is 128
	opsChunks := [][]clientv3.Op{}
	for i := 0; i < len(actions); i += OpChunkSize {
		end := i + OpChunkSize
		if end > len(actions) {
			end = len(actions)
		}
		opsChunks = append(opsChunks, actions[i:end])
	}

	// send the chunks to etcd
	for ci := range opsChunks {
		em.log.Infof("sending chunk %v of %v", ci, len(opsChunks))

		txn, cancel := em.Txn()
		txn.Then(opsChunks[ci]...)

		resp, err := txn.Commit()
		cancel()
		if err != nil {
			return false, fmt.Errorf("failed to update expiries in etcd for transfers[%v]: %s ", len(its), err), newExpiry
		}

		if !resp.Succeeded {
			return resp.Succeeded, err, newExpiry
		}
	}

	return true, nil, newExpiry
}

func (em *ETCDManager) StartUpdatingExpiries(ctx context.Context) {
	expiryTicker := time.NewTicker(10 * time.Second)

	for {
		select {
		case <-ctx.Done():
			return
		case <-expiryTicker.C:
			for {
				em.exmutex.RLock()

				if len(em.expiries) == 0 {
					em.exmutex.RUnlock()
					break
				}

				expiries := cloneExpiries(em.expiries)

				em.exmutex.RUnlock()

				succeeded, err, _ := em.updateExpiriesOnce(expiries)
				if err == nil && succeeded {
					break
				}
				if err != nil {
					em.log.Error(err)
				}
				if !succeeded {
					em.log.Warnf("failed to updated expiries in etcd")
				}

				time.Sleep(100 * time.Millisecond)
			}
		}

	}
}

func cloneExpiries(src map[string]map[uuid.UUID]proto.IncompleteTransfer) map[string]map[uuid.UUID]proto.IncompleteTransfer {
	dst := make(map[string]map[uuid.UUID]proto.IncompleteTransfer, len(src))

	for transferID, registrations := range src {
		registrationsClone := make(map[uuid.UUID]proto.IncompleteTransfer, len(registrations))

		for id, it := range registrations {
			registrationsClone[id] = it
		}

		dst[transferID] = registrationsClone
	}

	return dst
}
