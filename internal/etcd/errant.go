// Copyright 2026. Triad National Security, LLC. All rights reserved.

package etcd

import (
	"fmt"
	"time"

	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/defaults"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// RemoveErrant will set the errant key in etcd to PURGE. this will tell the purger to remove any files there
func (em *ETCDManager) RemoveErrant(user string, errantPath string) (succeeded bool, err error) {
	errantPathKey := proto.ETCDErrantPathKey(user, errantPath)

	compares := []clientv3.Cmp{
		clientv3.Compare(clientv3.CreateRevision(errantPathKey), ">", 0),
		clientv3.Compare(clientv3.Value(errantPathKey), "!=", proto.PurgeValue.AsTime().Format(time.RFC3339)),
	}

	actions := []clientv3.Op{
		clientv3.OpPut(errantPathKey, proto.PurgeValue.AsTime().Format(time.RFC3339)),
	}

	resp, err := em.RetryTxn(&compares, &actions, nil, defaults.MaxRetries, defaults.RetryDelay)

	if err != nil {
		return false, fmt.Errorf("failed to set Purge value in etcd for [%v]: %v", errantPathKey, err)
	}

	if resp == nil {
		return false, fmt.Errorf("response from etcd was nil, that's not supposed to happen")
	}

	return resp.Succeeded, nil
}

// AddErrant will set the errant key in etcd to the transferID
func (em *ETCDManager) AddErrant(user string, errantPath string) (succeeded bool, err error) {
	errantPathKey := proto.ETCDErrantPathKey(user, errantPath)

	compares := []clientv3.Cmp{
		clientv3.Compare(clientv3.CreateRevision(errantPathKey), "=", 0),
	}

	actions := []clientv3.Op{
		clientv3.OpPut(errantPathKey, timestamppb.Now().AsTime().Format(time.RFC3339)),
	}

	resp, err := em.RetryTxn(&compares, &actions, nil, defaults.MaxRetries, defaults.RetryDelay)

	if err != nil {
		return false, fmt.Errorf("failed to set errant value in etcd for [%v]: %v", errantPathKey, err)
	}

	if resp == nil {
		return false, fmt.Errorf("response from etcd was nil, that's not supposed to happen")
	}

	return resp.Succeeded, nil
}
