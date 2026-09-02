// Copyright 2026. Triad National Security, LLC. All rights reserved.

package etcd

import (
	"context"
	"errors"
	"fmt"

	"github.com/lanl/conduit/defaults"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func (em *ETCDManager) AddRoot() {
	em.cmutex.Lock()
	defer em.cmutex.Unlock()

	err := em.addUser("root")
	if err != nil {
		em.log.Fatalf("failed to add user root: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaults.DefaultETCDTimeout)
	_, err = em.client.UserGrantRole(ctx, "root", "root")
	cancel()
	if err != nil {
		em.log.Fatalf("failed to add root user to root role: %v", err)
	}

	// check that auth is enabled on etcd
	ctx, cancel = context.WithTimeout(context.Background(), defaults.DefaultETCDTimeout)
	authStatus, err := em.client.AuthStatus(ctx)
	cancel()
	if err != nil {
		em.log.Errorf("failed to get etcd auth status: %v", err)
	}
	if !authStatus.Enabled {
		ctx, cancel := context.WithTimeout(context.Background(), defaults.DefaultETCDTimeout)
		_, err := em.client.AuthEnable(ctx)
		cancel()
		if err != nil {
			em.log.Fatalf("failed to enable auth on etcd: %v", err)
		}
	}
}

// AddUser will add a user into etcd
//
// REQUIRES A LOCK BEFOREHAND
func (em *ETCDManager) addUser(username string) error {
	em.log.Debugf("sending request to add user: %v", username)
	ctx, cancel := context.WithTimeout(context.Background(), defaults.DefaultETCDTimeout)

	_, err := em.client.UserAddWithOptions(ctx, username, "", &clientv3.UserAddOptions{
		NoPassword: true,
	})

	cancel()

	if err != nil {
		err = rpctypes.Error(err)
		if errors.Is(err, rpctypes.ErrUserAlreadyExist) {
			return nil
		}

		return fmt.Errorf("failed to add user %v to etcd: %v", username, err)
	}

	return nil
}
