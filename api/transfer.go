// Copyright 2026. Triad National Security, LLC. All rights reserved.

package api

import (
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	anypb "google.golang.org/protobuf/types/known/anypb"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

const (
	TransferPrefix = "transfers/"
	LeasePrefix    = "leases/"
	JobsPrefix     = "jobs/"
	ErrorsPrefix   = "errors/"

	// transfer details keys
	StateKey          = "state"
	ErrorKey          = "error"
	ExpiryKey         = "expiry"
	SchedulerNodesKey = "schedulerNodes/"

	SourceKey        = "source"
	DestinationKey   = "destination"
	UserKey          = "user"
	StartTimeKey     = "startTime"
	EndTimeKey       = "endTime"
	CreatedTimeKey   = "createdTime"
	ErrorMessageKey  = "errorMessage"
	StatusDetailsKey = "statusDetails"
	ActiveKey        = "active"
	ActionKey        = "action"
	OptionsKey       = "options"
	CommentKey       = "comment"
	PausedStateKey   = "pausedState"
	ArchiveStateKey  = "archiveState"
	LeasesKey        = "leases"
	WarningsKey      = "warnings"
	StagingInfoKey   = "stagingInfo"
	// FullDestinationsKey = "fullDestinations"

	DestInfoKey       = "destInfo"
	ValidationOnlyKey = "validationOnly"
	PluginDataKey     = "pluginData"
	PriorityKey       = "priority"
)

var PurgeValue = timestamppb.New(time.Time{})

var etcdTransferKeyRegex = regexp.MustCompile(`transfers\/(\S+?)\/(?:(?:schedulerNodes)\/([^\s\/]+))?`)
var etcdErrorKeyRegex = regexp.MustCompile(`errors\/(\S+?)\/(\S+)`)

type ETCDStatusDetails struct {
	Data         string `json:"data"`
	Files        uint32 `json:"files"`
	Bandwidth    string `json:"bandwidth"`
	FilesChunks  uint32 `json:"filesChunks"`
	Directories  uint32 `json:"directories"`
	PluginStatus string `json:"pluginStatus"`
}

func (t *TransferDetails) getKey(key string) string {
	if t.GetTransferID() == "" {
		return ""
	}
	return TransferPrefix + t.GetTransferID() + "/" + key
}

// ETCDStateKey requires TransferDetails to have a minimum of TransferID specified
func (t *TransferDetails) ETCDStateKey() string {
	return t.getKey(StateKey)
}

// ETCDErrorKey requires TransferDetails to have a minimum of TransferID specified
func (t *TransferDetails) ETCDErrorKey() string {
	return t.getKey(ErrorKey)
}

// ETCDExpiryKey requires TransferDetails to have a minimum of TransferID specified
func (t *TransferDetails) ETCDExpiryKey() string {
	return t.getKey(ExpiryKey)
}

// ETCDLeasesKey requires TransferDetails to have a minimum of TransferID specified
func (t *TransferDetails) ETCDLeasesKey() string {
	return t.getKey(LeasesKey)
}

// ETCDSourceKey requires TransferDetails to have a minimum of TransferID specified
func (t *TransferDetails) ETCDSourceKey() string {
	return t.getKey(SourceKey)
}

// ETCDDestinationKey requires TransferDetails to have a minimum of TransferID specified
func (t *TransferDetails) ETCDDestinationKey() string {
	return t.getKey(DestinationKey)
}

// ETCDActiveKey requires TransferDetails to have a minimum of TransferID specified
func (t *TransferDetails) ETCDActiveKey() string {
	return t.getKey(ActiveKey)
}

// ETCDActionKey requires TransferDetails to have a minimum of TransferID specified
func (t *TransferDetails) ETCDActionKey() string {
	return t.getKey(ActionKey)
}

// ETCDActionKey requires TransferDetails to have a minimum of TransferID specified
func (t *TransferDetails) ETCDOptionsKey() string {
	return t.getKey(OptionsKey)
}

func (t *TransferDetails) ETCDUserKey() string {
	return t.getKey(UserKey)
}

// ETCDStartTimeKey requires TransferDetails to have a minimum of TransferID specified
func (t *TransferDetails) ETCDStartTimeKey() string {
	return t.getKey(StartTimeKey)
}

// ETCDEndTimeKey requires TransferDetails to have a minimum of TransferID specified
func (t *TransferDetails) ETCDEndTimeKey() string {
	return t.getKey(EndTimeKey)
}

// ETCDCreatedTimeKey requires TransferDetails to have a minimum of TransferID specified
func (t *TransferDetails) ETCDCreatedTimeKey() string {
	return t.getKey(CreatedTimeKey)
}

// ETCDErrorMessageKey requires TransferDetails to have a minimum of TransferID specified
func (t *TransferDetails) ETCDErrorMessageKey() string {
	return t.getKey(ErrorMessageKey)
}

// ETCDStatusDetailsKey requires TransferDetails to have a minimum of TransferID specified
func (t *TransferDetails) ETCDStatusDetailsKey() string {
	return t.getKey(StatusDetailsKey)
}

// ETCDSchedulerNodesKey requires TransferDetails to have a minimum of TransferID specified
//
// if SchedulerCommand_NONE is provided, it will return the key without the scheduler command appended.
// if any other SchedulerCommand is provided, it will append that scheduler command to the end.
func (t *TransferDetails) ETCDSchedulerNodesKey(sc SchedulerCommand) string {
	if sc == SchedulerCommand_NONE {
		return t.getKey(SchedulerNodesKey)
	} else {
		return t.getKey(SchedulerNodesKey) + sc.String()
	}
}

// ETCDCommentKey requires TransferDetails to have a minimum of TransferID specified
func (t *TransferDetails) ETCDCommentKey() string {
	return t.getKey(CommentKey)
}

// ETCDPausedStateKey requires TransferDetails to have a minimum of TransferID specified
func (t *TransferDetails) ETCDPausedStateKey() string {
	return t.getKey(PausedStateKey)
}

// ETCDArchiveStateKey requires TransferDetails to have a minimum of TransferID specified
func (t *TransferDetails) ETCDArchiveStateKey() string {
	return t.getKey(ArchiveStateKey)
}

// ETCDWarningsKey requires TransferDetails to have a minimum of TransferID specified
func (t *TransferDetails) ETCDWarningsKey() string {
	return t.getKey(WarningsKey)
}

// ETCDDestInfoKey requires TransferDetails to have a minimum of TransferID specified
func (t *TransferDetails) ETCDDestInfoKey() string {
	return t.getKey(DestInfoKey)
}

// ETCDValidationOnlyKey requires TransferDetails to have a minimum of TransferID specified
func (t *TransferDetails) ETCDValidationOnlyKey() string {
	return t.getKey(ValidationOnlyKey)
}

// // ETCDFullDestinationsKey requires TransferDetails to have a minimum of TransferID specified
// func (t *TransferDetails) ETCDFullDestinationsKey() string {
// 	return t.getKey(FullDestinationsKey)
// }

// ETCDPluginDataKey requires TransferDetails to have a minimum of TransferID specified
func (t *TransferDetails) ETCDPluginDataKey() string {
	return t.getKey(PluginDataKey)
}

// ETCDPriorityKey requires TransferDetails to have a minimum of TransferID specified
func (t *TransferDetails) ETCDPriorityKey() string {
	return t.getKey(PriorityKey)
}

// ETCDLeaseListKey requires TransferDetails to have a minimum of TransferID specified
func (t *TransferDetails) ETCDLeaseListKey() string {
	return LeasePrefix + t.GetTransferID()
}

// ETCDJobsKey requires TransferDetails to have a minimum of TransferID specified
func (t *TransferDetails) ETCDJobsKey() string {
	return JobsPrefix + t.GetTransferID()
}

// ETCDStatusDetails returns a json marshalled version of a transfer's status details to be put in ETCD
func (t *TransferDetails) ETCDStatusDetails() ([]byte, error) {
	etd := &ETCDStatusDetails{
		Data:         t.DataTransferred,
		Files:        t.FilesTransferred,
		Bandwidth:    t.Bandwidth,
		FilesChunks:  t.FilesChunks,
		Directories:  t.DirectoriesTransferred,
		PluginStatus: t.PluginStatus,
	}

	return json.Marshal(etd)
}

// ParseETCDTransfersKey returns the transfer id, unescaped path of a lease, and a schdulerCommand from an etcd key
//
// schdulerCommand will be NONE if it doesn't exist in the etcdKey and
func ParseETCDTransfersKey(etcdKey string) (id uuid.UUID, schdulerCommand SchedulerCommand, err error) {
	// regex groups:
	// example etcd key: transfers/123456/schedulerNodes/TEARDOWN
	// match 0: entire match
	// match 1: transfer ID (123456)
	// match 2: schedulerNodes map command (TEARDOWN)
	matches := etcdTransferKeyRegex.FindStringSubmatch(etcdKey)

	// check if transferID exists
	if matches[1] != "" {
		// get transfer id
		id, err = uuid.Parse(matches[1])
		if err != nil {
			return uuid.Nil, SchedulerCommand_NONE, fmt.Errorf("failed to parse transfer id from key[%v]: %v", etcdKey, err)
		}
	} else {
		return uuid.Nil, SchedulerCommand_NONE, fmt.Errorf("no transferID found in key[%v]: %v", etcdKey, err)
	}

	// check if schedulerNodes command exists
	if matches[2] != "" {
		sc, ok := SchedulerCommand_value[matches[2]]
		if !ok {
			return uuid.Nil, SchedulerCommand_NONE, fmt.Errorf("invalid schedulerCommand [%v] in ETCD key %v", matches[2], etcdKey)
		}
		schdulerCommand = SchedulerCommand(sc)
	}

	return id, schdulerCommand, err
}

// ParseETCDErrorsKey returns the user and unescaped trash path from an etcd key
func ParseETCDErrorsKey(etcdKey string) (user string, trashPath string, err error) {
	// regex groups:
	// example etcd key: errors/testuser/%2Fthis%2Fis%2Fa%2Ftest%2Fpath
	// match 0: entire match
	// match 1: user (testuser)
	// match 2: path (/this/is/a/test/path)
	matches := etcdErrorKeyRegex.FindStringSubmatch(etcdKey)

	// check if user exists
	if matches[1] != "" {
		user = matches[1]
	} else {
		return "", "", fmt.Errorf("no user found in key[%v]: %v", etcdKey, err)
	}

	// check if schedulerNodes command exists
	if matches[2] != "" {
		trashPath, err = url.QueryUnescape(matches[2])
		if err != nil {
			return "", "", fmt.Errorf("failed to unescape characters from key[%v]: %v", etcdKey, err)
		}
	}

	return user, trashPath, nil
}

// ETCDErrantPathKey returns an escaped etcd errant path key with the provided user and errant path
func ETCDErrantPathKey(user string, errantPath string) string {
	return ErrorsPrefix + user + "/" + url.QueryEscape(strings.TrimSpace(errantPath))
}

func NewTransferDetails() *TransferDetails {
	transferID := uuid.New()

	transfer := &TransferDetails{
		TransferID:             transferID.String(),
		State:                  TransferState_TRANSFER_NONE,
		Error:                  Error_ERROR_NONE,
		Expiry:                 nil,
		SchedulerNodes:         &SchedulerNodes{},
		Source:                 []string{},
		Destination:            "",
		User:                   "",
		StartTime:              nil,
		EndTime:                nil,
		CreatedTime:            timestamppb.Now(),
		ErrorMessage:           "",
		DataTransferred:        "",
		FilesTransferred:       0,
		Bandwidth:              "",
		FilesChunks:            0,
		DirectoriesTransferred: 0,
		Active:                 true,
		Action:                 "",
		Comment:                "",
		PausedState:            TransferState_TRANSFER_NONE,
		ArchiveState:           ArchiveState_ARCHIVE_NONE,
		Leases:                 &Leases{},
		Warnings:               []string{},
		DestInfo:               DestInfo_DEST_NONE,
		ValidationOnly:         false,
		PluginData:             []byte{},
		PluginStatus:           "",
		Priority:               0,
		Options:                make(map[string]*anypb.Any),
	}

	return transfer
}

type StringableState interface {
	String() string
}

type IncompleteTransfer interface {
	GetTransferID() string
	ETCDLeaseListKey() string

	ETCDStateKey() string
	ETCDErrorKey() string
	ETCDErrorMessageKey() string
	ETCDExpiryKey() string
	ETCDSchedulerNodesKey(sc SchedulerCommand) string

	ETCDSourceKey() string
	ETCDDestinationKey() string
	ETCDLeasesKey() string
	ETCDActiveKey() string
	ETCDActionKey() string
	ETCDOptionsKey() string
	ETCDUserKey() string
	ETCDStartTimeKey() string
	ETCDEndTimeKey() string
	ETCDCreatedTimeKey() string
	ETCDStatusDetailsKey() string
	ETCDCommentKey() string
	ETCDPausedStateKey() string
	ETCDArchiveStateKey() string
	ETCDWarningsKey() string

	ETCDDestInfoKey() string
	ETCDValidationOnlyKey() string
	// ETCDFullDestinationsKey() string
	ETCDPluginDataKey() string
	ETCDJobsKey() string
	ETCDPriorityKey() string
}
