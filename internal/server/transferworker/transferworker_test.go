// Copyright 2026. Triad National Security, LLC. All rights reserved.

package transferworker

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	proto "github.com/lanl/conduit/api"
)

const (
	testLeasePath = "/foo/bar/hello"
)

var (
	testChildren = []string{
		"/foo/bar/hello/hello",
		"/foo/bar/hello/foo",
		"/foo/bar/hello/hello/foo",
		"/foo/bar/hello/blah",
		"/foo/bar/hello/hello/",
		"/foo/bar/hello/foo/",
		"/foo/bar/hello/hello/foo/",
		"/foo/bar/hello/blah/",
	}
	testSiblings = []string{
		"/foo/bar/goodbye",
		"/foo/bar/blah",
		"/foo/bar/goodbye/",
		"/foo/bar/blah/",
	}
	testParents = []string{
		"/foo/bar",
		"/foo",
		"/foo/bar/",
		"/foo/",
	}
)

func TestFindLeaseChildren(t *testing.T) {
	transferID := uuid.New()

	leaseMap, err := createLeaseMap(transferID)
	if err != nil {
		t.Fatalf("failed to create lease map: %v", err)
	}

	children := findLeaseChildren(testLeasePath, transferID, leaseMap, proto.LeaseType_SOURCE)

	for id, c := range children {
		if id == transferID {
			t.Fatalf("this transfer's id was unexpectedly found")
		}

		if id.String()[0:1] != "1" {
			t.Fatalf("found a path that wasn't a child of [%v]: [%v]", testLeasePath, c)
		}
	}

	if len(children) != len(testChildren) {
		t.Fatalf("did not find the correct number of children. found %v of %v found children: %+v", len(children), len(testChildren), children)
	}
}

func TestFindLeaseParents(t *testing.T) {
	transferID := uuid.New()

	leaseMap, err := createLeaseMap(transferID)
	if err != nil {
		t.Fatalf("failed to create lease map: %v", err)
	}
	parents := findLeaseParents(testLeasePath, transferID, leaseMap, proto.LeaseType_SOURCE)

	t.Logf("lease map: %+v", leaseMap)

	for id, c := range parents {
		if id == transferID {
			t.Fatalf("this transfer's id was unexpectedly found")
		}

		if id.String()[0:1] != "3" {
			t.Fatalf("found a path that wasn't a parent of [%v]: [%v]", testLeasePath, c)
		}
	}

	if len(parents) != len(testParents) {
		t.Fatalf("did not find the correct number of parents. found %v of %v found parents: %+v", len(parents), len(testParents), parents)
	}
}

func TestFindLeaseExacts(t *testing.T) {
	transferID := uuid.New()

	leaseMap, err := createLeaseMap(transferID)
	if err != nil {
		t.Fatalf("failed to create lease map: %v", err)
	}
	exacts := findLeaseExacts(testLeasePath, transferID, leaseMap, proto.LeaseType_SOURCE)

	for id, c := range exacts {
		if id == transferID {
			t.Fatalf("this transfer's id was unexpectedly found")
		}

		if id.String()[0:1] != "4" {
			t.Fatalf("found a path that wasn't an exact of [%v]: [%v]", testLeasePath, c)
		}
	}

	if len(exacts) != 1 {
		t.Fatalf("did not find the correct number of exacts. found %v of %v found exacts: %+v", len(exacts), 1, exacts)
	}
}

func createLeaseMap(id uuid.UUID) (map[uuid.UUID]*proto.Leases, error) {
	finalMap := map[uuid.UUID]*proto.Leases{}

	for i, p := range testChildren {
		tuid := fmt.Sprintf("10000000-0000-0000-0000-%012d", i)
		uuid, err := uuid.Parse(tuid)
		if err != nil {
			return nil, err
		}
		finalMap[uuid] = &proto.Leases{
			Source:      []string{p},
			Destination: []string{p},
		}
	}
	for i, p := range testSiblings {
		uuid, err := uuid.Parse(fmt.Sprintf("20000000-0000-0000-0000-%012d", i))
		if err != nil {
			return nil, err
		}
		finalMap[uuid] = &proto.Leases{
			Source:      []string{p},
			Destination: []string{p},
		}
	}
	for i, p := range testParents {
		uuid, err := uuid.Parse(fmt.Sprintf("30000000-0000-0000-0000-%012d", i))
		if err != nil {
			return nil, err
		}
		finalMap[uuid] = &proto.Leases{
			Source:      []string{p},
			Destination: []string{p},
		}
	}

	// add a transfer with the same lease path that we're matching aginst but a different uuid
	uuid, err := uuid.Parse(fmt.Sprintf("40000000-0000-0000-0000-%012d", 0))
	if err != nil {
		return nil, err
	}
	finalMap[uuid] = &proto.Leases{
		Source:      []string{testLeasePath},
		Destination: []string{testLeasePath},
	}

	// add a transfer with the same lease path and uuid that we're matching aginst
	finalMap[id] = &proto.Leases{
		Source:      []string{testLeasePath},
		Destination: []string{testLeasePath},
	}
	// add parents, siblings, and children to this transfer too
	finalMap[id].Source = append(finalMap[id].Source, testParents...)
	finalMap[id].Source = append(finalMap[id].Source, testSiblings...)
	finalMap[id].Source = append(finalMap[id].Source, testChildren...)
	finalMap[id].Destination = append(finalMap[id].Destination, testParents...)
	finalMap[id].Destination = append(finalMap[id].Destination, testSiblings...)
	finalMap[id].Destination = append(finalMap[id].Destination, testChildren...)

	return finalMap, nil
}
