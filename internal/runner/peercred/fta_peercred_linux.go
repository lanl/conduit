//go:build linux

// Copyright 2026. Triad National Security, LLC. All rights reserved.

package peercred

import "golang.org/x/sys/unix"

func getPeerCredentialsFD(fd int) (*PeerCredentials, error) {
	ucred, err := unix.GetsockoptUcred(
		fd,
		unix.SOL_SOCKET,
		unix.SO_PEERCRED,
	)
	if err != nil {
		return nil, err
	}

	return &PeerCredentials{
		Pid: int(ucred.Pid),
		Uid: ucred.Uid,
		Gid: ucred.Gid,
	}, nil
}
