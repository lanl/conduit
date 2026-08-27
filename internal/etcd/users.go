// Copyright 2026. Triad National Security, LLC. All rights reserved.

package etcd

import (
	"context"
	"errors"
	"fmt"

	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/defaults"
	"go.etcd.io/etcd/api/v3/authpb"
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

// // DoesUserExist will check if a user already exists in etcd
// //
// // REQUIRES A LOCK BEFOREHAND
// func (em *ETCDManager) DoesUserExist(username string) (bool, error) {
// 	// check if user is already in the list of etcd users
// 	userExists := false
// 	ctx, cancel := context.WithTimeout(context.Background(), defaults.DefaultETCDTimeout)
// 	userList, err := em.client.UserList(ctx)
// 	cancel()
// 	if err != nil {
// 		return false, fmt.Errorf("failed to get user list: %v", err)
// 	}
// 	for _, user := range userList.Users {
// 		if user == username {
// 			userExists = true
// 			break
// 		}
// 	}

// 	return userExists, nil
// }

// // DoesRoleExist will check if a role already exists in etcd
// //
// // REQUIRES A LOCK BEFOREHAND
// func (em *ETCDManager) DoesRoleExist(roleName string) (bool, error) {
// 	// check if role is already in the list of etcd roles
// 	roleExists := false
// 	ctx, cancel := context.WithTimeout(context.Background(), defaults.DefaultETCDTimeout)
// 	roleList, err := em.client.RoleList(ctx)
// 	cancel()
// 	if err != nil {
// 		return false, fmt.Errorf("failed to get role list: %v", err)
// 	}
// 	for _, role := range roleList.Roles {
// 		if role == roleName {
// 			roleExists = true
// 			break
// 		}
// 	}

// 	return roleExists, nil
// }

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

// AddRole will add a role into etcd
//
// REQUIRES A LOCK BEFOREHAND
func (em *ETCDManager) addRole(roleName string) error {
	em.log.Debugf("sending request to add role: %v", roleName)
	ctx, cancel := context.WithTimeout(context.Background(), defaults.DefaultETCDTimeout)

	_, err := em.client.RoleAdd(ctx, roleName)

	cancel()

	if err != nil {
		err = rpctypes.Error(err)
		if errors.Is(err, rpctypes.ErrRoleAlreadyExist) {
			return nil
		}

		return fmt.Errorf("failed to add role %v to etcd: %w", roleName, err)
	}

	return nil
}

// RemoveUser will remove a user from etcd
//
// REQUIRES A LOCK BEFOREHAND
func (em *ETCDManager) removeUser(username string) error {
	em.log.Debugf("sending request to remove user: %v", username)
	ctx, cancel := context.WithTimeout(context.Background(), defaults.DefaultETCDTimeout)

	_, err := em.client.UserDelete(ctx, username)

	cancel()

	if err != nil {
		err = rpctypes.Error(err)
		if errors.Is(err, rpctypes.ErrUserNotFound) {
			return nil
		}

		return fmt.Errorf("failed to remove user %v from etcd: %v", username, err)
	}

	return nil
}

// RemoveRole will remove a role from etcd
//
// REQUIRES A LOCK BEFOREHAND
func (em *ETCDManager) removeRole(roleName string) error {
	em.log.Debugf("sending request to remove role: %v", roleName)
	ctx, cancel := context.WithTimeout(context.Background(), defaults.DefaultETCDTimeout)

	_, err := em.client.RoleDelete(ctx, roleName)

	cancel()

	if err != nil {
		err = rpctypes.Error(err)
		if errors.Is(err, rpctypes.ErrRoleNotFound) {
			return nil
		}

		return fmt.Errorf("failed to remove role %v from etcd: %w", roleName, err)
	}

	return nil
}

// AddTransferUser will add a user and role for a transfer
func (em *ETCDManager) AddTransferUser(transferID string) error {
	em.cmutex.Lock()
	defer em.cmutex.Unlock()
	// add transferID as etcd user
	err := em.addUser(transferID)
	if err != nil {
		return fmt.Errorf("failed to add transfer user %v to etcd: %v", transferID, err)
	}

	// add role for this user
	err = em.addRole(transferID)
	if err != nil {
		return fmt.Errorf("failed to add role %v to etcd: %v", transferID, err)
	} else {
		em.log.Debugf("successfully added role: %v to etcd", transferID)
	}

	// add user to the role
	ctx, cancel := context.WithTimeout(context.Background(), defaults.DefaultETCDTimeout)
	_, err = em.client.UserGrantRole(ctx, transferID, transferID)
	cancel()
	if err != nil {
		return fmt.Errorf("failed to add user %v to role %v: %v", transferID, transferID, err)
	} else {
		em.log.Debugf("successfully added user: %v to role: %v", transferID, transferID)
	}

	// give role permission to read/write to "transfers/<transferID>"
	ctx, cancel = context.WithTimeout(context.Background(), defaults.DefaultETCDTimeout)
	_, err = em.client.RoleGrantPermission(ctx, transferID, proto.TransferPrefix+transferID, clientv3.GetPrefixRangeEnd(proto.TransferPrefix+transferID), clientv3.PermissionType(authpb.READWRITE))
	cancel()
	if err != nil {
		return fmt.Errorf("failed to grant etcd permission to user %v: %v", transferID, err)
	} else {
		em.log.Debugf("successfully added permissions: %v to role: %v at: %v", authpb.READWRITE.String(), transferID, proto.TransferPrefix+transferID)
	}

	return nil
}

// AddTransferUser will add a user and role for a transfer
func (em *ETCDManager) RemoveTransferUser(transferID string) error {
	em.cmutex.Lock()
	defer em.cmutex.Unlock()
	// remove etcd user
	err := em.removeUser(transferID)
	if err != nil {
		return fmt.Errorf("failed to remove user %v from etcd: %v", transferID, err)
	}

	// remove etcd role
	err = em.removeRole(transferID)
	if err != nil {
		return fmt.Errorf("failed to remove role %v from etcd: %v", transferID, err)
	} else {
		em.log.Debugf("successfully removed role: %v from etcd", transferID)
	}

	return nil
}
