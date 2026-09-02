//go:build darwin

// Copyright 2026. Triad National Security, LLC. All rights reserved.

package peercred

import (
	"fmt"

	"golang.org/x/sys/unix"
)

func getPeerCredentialsFD(fd int) (*PeerCredentials, error) {
	xucred, err := unix.GetsockoptXucred(
		fd,
		unix.SOL_LOCAL,
		unix.LOCAL_PEERCRED,
	)
	if err != nil {
		return nil, err
	}

	pid, err := unix.GetsockoptInt(
		fd,
		unix.SOL_LOCAL,
		unix.LOCAL_PEERPID,
	)
	if err != nil {
		return nil, err
	}

	if xucred.Ngroups < 1 {
		return nil, fmt.Errorf("peer returned no groups")
	}

	return &PeerCredentials{
		Pid: pid,
		Uid: xucred.Uid,
		Gid: xucred.Groups[0],
	}, nil
}
