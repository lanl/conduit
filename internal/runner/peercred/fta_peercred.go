// Copyright 2026. Triad National Security, LLC. All rights reserved.

package peercred

import (
	"net"
)

type PeerCredentials struct {
	Pid int
	Uid uint32
	Gid uint32
}

func GetPeerCredentials(conn *net.UnixConn) (*PeerCredentials, error) {
	rawConn, err := conn.SyscallConn()
	if err != nil {
		return nil, err
	}

	var (
		cred    *PeerCredentials
		sockErr error
	)

	err = rawConn.Control(func(fd uintptr) {
		cred, sockErr = getPeerCredentialsFD(int(fd))
	})
	if err != nil {
		return nil, err
	}

	if sockErr != nil {
		return nil, sockErr
	}

	return cred, nil
}
