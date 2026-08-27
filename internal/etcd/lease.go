// Copyright 2026. Triad National Security, LLC. All rights reserved.

package etcd

import (
	"context"
	"fmt"

	"github.com/lanl/conduit/defaults"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func (em *ETCDManager) GrantLease(ttl int64) (clientv3.LeaseID, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaults.DefaultETCDTimeout)
	defer cancel()

	em.cmutex.RLock()
	resp, err := em.client.Grant(ctx, ttl)
	em.cmutex.RUnlock()

	if err != nil {
		return 0, fmt.Errorf("failed to grant etcd lease: %v", err)
	}

	return resp.ID, nil
}

func (em *ETCDManager) KeepLeaseAlive(ctx context.Context, id clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	em.cmutex.RLock()
	ch, err := em.client.KeepAlive(ctx, id)
	em.cmutex.RUnlock()

	if err != nil {
		return nil, fmt.Errorf("failed to keep etcd lease[%v] alive: %v", id, err)
	}

	return ch, nil
}

func (em *ETCDManager) RevokeLease(id clientv3.LeaseID) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaults.DefaultETCDTimeout)
	defer cancel()

	em.cmutex.RLock()
	_, err := em.client.Revoke(ctx, id)
	em.cmutex.RUnlock()

	if err != nil {
		return fmt.Errorf("failed to revoke etcd lease[%v]: %v", id, err)
	}

	return nil
}
