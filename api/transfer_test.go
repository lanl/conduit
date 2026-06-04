// Copyright 2026. Triad National Security, LLC. All rights reserved.

package api

import (
	"fmt"
	"net/url"
	"testing"

	"github.com/google/uuid"
)

var (
	commands = []SchedulerCommand{
		SchedulerCommand_TEARDOWN,
		SchedulerCommand_SETUP,
		SchedulerCommand_TRANSFER,
		SchedulerCommand_VALIDATION,
	}

	paths = []string{
		"/this/is/a/test-path",
		"this/test-path/doesnt/have/beginning-slash",
		"testpath",
		"//another/testuser//path",
	}
)

func TestParseETCDTransfersKey(t *testing.T) {
	// example etcd key: transfers/123456/schedulerNodes/TEARDOWN
	for _, schedulerCommand := range commands {
		uid := uuid.New()
		ek := fmt.Sprintf("transfers/%s/schedulerNodes/%s", uid, schedulerCommand.String())

		id, sc, err := ParseETCDTransfersKey(ek)
		if err != nil {
			t.Fatalf("error while parsing etcd key: %v", err)
		}
		if sc != schedulerCommand {
			t.Fatalf("slurm command did not match. found: %v", sc)
		}
		if id != uid {
			t.Fatalf("uuid does not match. found: %s uid: %s", id, uid)
		}
	}
}

func TestParseETCDErrorsKey(t *testing.T) {
	// example etcd key: errors/<user>/<trash_path>
	user := "testuser"
	for _, path := range paths {
		ek := fmt.Sprintf("errors/%s/%s", user, url.QueryEscape(path))

		u, p, err := ParseETCDErrorsKey(ek)
		if err != nil {
			t.Fatalf("error while parsing etcd key: %v", err)
		}
		if u != user {
			t.Fatalf("user did not match. found: %v", u)
		}
		if p != path {
			t.Fatalf("path does not match. found: %s", p)
		}
	}
}
