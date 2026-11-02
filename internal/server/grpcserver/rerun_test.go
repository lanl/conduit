// Copyright 2026. Triad National Security, LLC. All rights reserved.

package grpcserver

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lanl/conduit/api"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type rerunArchiveStub struct {
	transfers  map[string]*api.TransferDetails
	removed    int64
	removeErr  error
	restoreErr error
	restored   *api.TransferDetails
}

func (r *rerunArchiveStub) GetTransfers(_ []string) (map[string]*api.TransferDetails, error) {
	return r.transfers, nil
}

func (r *rerunArchiveStub) RemoveTransfers(_ []string) (int64, error) {
	return r.removed, r.removeErr
}

func (r *rerunArchiveStub) AddTransfer(td *api.TransferDetails) error {
	r.restored = td
	return r.restoreErr
}

type rerunSubmitterStub struct {
	err       error
	submitted []*api.TransferDetails
}

func (s *rerunSubmitterStub) SubmitTransfers(transfers []*api.TransferDetails) error {
	s.submitted = append(s.submitted, transfers...)
	return s.err
}

func TestReRunArchivedTransfersSuccess(t *testing.T) {
	created := timestamppb.New(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
	option := &anypb.Any{TypeUrl: "example.com/option", Value: []byte{1}}
	original := &api.TransferDetails{
		TransferID:  "transfer-1",
		State:       api.TransferState_TRANSFER_FINALIZED,
		Source:      []string{"/source"},
		Destination: "/destination",
		User:        "testuser",
		CreatedTime: created,
		Comment:     "original comment",
		Action:      "COPY",
		Options:     map[string]*anypb.Any{"test": option},
	}
	archive := &rerunArchiveStub{
		transfers: map[string]*api.TransferDetails{original.GetTransferID(): original},
		removed:   1,
	}
	submitter := &rerunSubmitterStub{}

	result, err := reRunArchivedTransfers([]string{original.GetTransferID()}, archive, submitter)
	if err != nil {
		t.Fatalf("rerun failed: %v", err)
	}
	got := result.GetDetails()[original.GetTransferID()]
	if got == nil || len(submitter.submitted) != 1 || submitter.submitted[0] != got {
		t.Fatalf("expected one successfully submitted transfer, got %v", result)
	}
	if got.GetState() != api.TransferState_TRANSFER_INIT_COMPLETE || !got.GetActive() {
		t.Fatalf("rerun has incorrect state: %v", got)
	}
	if got.GetCreatedTime() != created || got.GetTransferID() != original.GetTransferID() ||
		got.GetUser() != original.GetUser() || got.GetAction() != original.GetAction() ||
		got.GetDestination() != original.GetDestination() || got.GetComment() != original.GetComment() ||
		len(got.GetSource()) != 1 || got.GetSource()[0] != original.GetSource()[0] ||
		got.GetOptions()["test"] != option {
		t.Fatalf("rerun did not preserve original transfer settings: %v", got)
	}
	if got.GetExpiry() == nil || len(got.GetWarnings()) != 1 || !strings.Contains(got.GetWarnings()[0], "Rerun Transfer") {
		t.Fatalf("rerun has no fresh expiry or admin warning: %v", got)
	}
	if archive.restored != nil {
		t.Fatal("successful rerun unexpectedly restored the archive")
	}
}

func TestReRunArchivedTransfersAlreadyClaimed(t *testing.T) {
	original := &api.TransferDetails{TransferID: "transfer-1"}
	archive := &rerunArchiveStub{
		transfers: map[string]*api.TransferDetails{original.GetTransferID(): original},
		removed:   0, // Another server already deleted the archived row.
	}
	submitter := &rerunSubmitterStub{}

	result, err := reRunArchivedTransfers([]string{original.GetTransferID()}, archive, submitter)
	if err != nil {
		t.Fatalf("rerun failed: %v", err)
	}
	if len(result.GetDetails()) != 0 || len(submitter.submitted) != 0 {
		t.Fatal("rerun submitted a transfer already claimed by another server")
	}
}

func TestReRunArchivedTransfersSubmitFailure(t *testing.T) {
	original := &api.TransferDetails{TransferID: "transfer-1"}
	archive := &rerunArchiveStub{
		transfers: map[string]*api.TransferDetails{original.GetTransferID(): original},
		removed:   1,
	}
	submitErr := errors.New("submit failed")
	submitter := &rerunSubmitterStub{err: submitErr}

	result, err := reRunArchivedTransfers([]string{original.GetTransferID()}, archive, submitter)
	if !errors.Is(err, submitErr) {
		t.Fatalf("expected submit error, got %v", err)
	}
	if archive.restored != original {
		t.Fatal("original transfer was not restored to rqlite")
	}
	if len(result.GetDetails()) != 0 {
		t.Fatal("failed transfer was incorrectly reported as rerun")
	}
}

func TestReRunArchivedTransfersRemoveFailure(t *testing.T) {
	original := &api.TransferDetails{TransferID: "transfer-1"}
	removeErr := errors.New("remove failed")
	archive := &rerunArchiveStub{
		transfers: map[string]*api.TransferDetails{original.GetTransferID(): original},
		removeErr: removeErr,
	}
	submitter := &rerunSubmitterStub{}

	_, err := reRunArchivedTransfers([]string{original.GetTransferID()}, archive, submitter)
	if !errors.Is(err, removeErr) {
		t.Fatalf("expected removal error, got %v", err)
	}
	if len(submitter.submitted) != 0 {
		t.Fatal("transfer submitted despite failed archive removal")
	}
}
