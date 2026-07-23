// Copyright 2026. Triad National Security, LLC. All rights reserved.

package etcd

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	proto "github.com/lanl/conduit/api"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func ParseETCDTransfers(events []*clientv3.Event) (map[string]*proto.TransferDetails, error) {
	transfers := make(map[string]*proto.TransferDetails)

	for _, e := range events {
		tid, _, err := proto.ParseETCDTransfersKey(string(e.Kv.Key))
		if err != nil {
			return transfers, fmt.Errorf("failed to parse ETCD Key[%s]: %v", string(e.Kv.Key), err)
		}

		switch e.Type {
		case mvccpb.DELETE:
			delete(transfers, tid.String())
		case mvccpb.PUT:
			if td, ok := transfers[tid.String()]; ok {
				newTd, err := ParseETCDTransfer(tid, []*mvccpb.KeyValue{e.Kv}, td)
				if err != nil {
					return transfers, fmt.Errorf("failed to parse ETCD Transfer from kv[%+v]: %v", e.Kv, err)
				}
				transfers[newTd.GetTransferID()] = newTd
			} else {
				newTd, err := ParseETCDTransfer(tid, []*mvccpb.KeyValue{e.Kv}, nil)
				if err != nil {
					return transfers, fmt.Errorf("failed to parse ETCD Transfer from kv[%+v]: %v", e.Kv, err)
				}
				transfers[newTd.GetTransferID()] = newTd
			}
		}
	}

	return transfers, nil
}

// ParseETCDTransfer will create a full proto.TransferDetails object from a list of key-values from etcd
func ParseETCDTransfer(id uuid.UUID, kvs []*mvccpb.KeyValue, old *proto.TransferDetails) (*proto.TransferDetails, error) {
	t := &proto.TransferDetails{
		TransferID:     id.String(),
		SchedulerNodes: &proto.SchedulerNodes{},
	}

	// if we're given an old, assume we need to update that transferDetails
	if old != nil {
		t = old
	}

	if t.SchedulerNodes == nil {
		t.SchedulerNodes = &proto.SchedulerNodes{}
	}

	for _, kv := range kvs {
		switch {
		case string(kv.Key) == t.ETCDStatusDetailsKey():
			esd := &proto.ETCDStatusDetails{}
			err := json.Unmarshal(kv.Value, esd)
			if err != nil {
				return nil, fmt.Errorf("transfer[%s]: failed to unmarshal status details from etcd into json object: %v [%v]", id, err, string(kv.Value))
			}

			t.DataTransferred = esd.Data
			t.Bandwidth = esd.Bandwidth
			t.FilesChunks = esd.FilesChunks
			t.FilesTransferred = esd.Files
			t.DirectoriesTransferred = esd.Directories
			t.PluginStatus = esd.PluginStatus
		case string(kv.Key) == t.ETCDPluginDataKey():
			t.PluginData = []byte(string(kv.Value))
		case string(kv.Key) == t.ETCDStateKey():
			t.State = proto.TransferState(proto.TransferState_value[string(kv.Value)])
		case string(kv.Key) == t.ETCDPausedStateKey():
			t.PausedState = proto.TransferState(proto.TransferState_value[string(kv.Value)])
		case string(kv.Key) == t.ETCDArchiveStateKey():
			t.ArchiveState = proto.ArchiveState(proto.ArchiveState_value[string(kv.Value)])
		case string(kv.Key) == t.ETCDActionKey():
			t.Action = string(kv.Value)
		case string(kv.Key) == t.ETCDOptionsKey():
			options := make(map[string]*anypb.Any)
			err := json.Unmarshal(kv.Value, &options)
			if err != nil {
				return nil, err
			}
			t.Options = options
		case string(kv.Key) == t.ETCDActiveKey():
			b, err := strconv.ParseBool(string(kv.Value))
			if err != nil {
				return nil, fmt.Errorf("transfer[%s]: failed to parse completed bool: %v", id, err)
			}
			t.Active = b
		case string(kv.Key) == t.ETCDValidationOnlyKey():
			b, err := strconv.ParseBool(string(kv.Value))
			if err != nil {
				return nil, fmt.Errorf("transfer[%s]: failed to parse validation only bool: %v", id, err)
			}
			t.ValidationOnly = b
		case string(kv.Key) == t.ETCDErrorKey():
			t.Error = proto.Error(proto.Error_value[string(kv.Value)])
		case string(kv.Key) == t.ETCDSourceKey():
			pathList := []string{}
			err := json.Unmarshal(kv.Value, &pathList)
			if err != nil {
				return nil, err
			}
			t.Source = pathList
		case string(kv.Key) == t.ETCDWarningsKey():
			warnList := []string{}
			err := json.Unmarshal(kv.Value, &warnList)
			if err != nil {
				return nil, err
			}
			t.Warnings = warnList
		case string(kv.Key) == t.ETCDDestInfoKey():
			t.DestInfo = proto.DestInfo(proto.DestInfo_value[string(kv.Value)])
		case string(kv.Key) == t.ETCDLeasesKey():
			leases := &proto.Leases{}
			err := protojson.Unmarshal(kv.Value, leases)
			if err != nil {
				return nil, err
			}
			t.Leases = leases
		// case string(kv.Key) == t.ETCDFullDestinationsKey():
		// 	pathList := []string{}
		// 	err := json.Unmarshal(kv.Value, &pathList)
		// 	if err != nil {
		// 		return nil, err
		// 	}
		// 	t.FullDestinations = pathList
		case string(kv.Key) == t.ETCDDestinationKey():
			t.Destination = string(kv.Value)
		case string(kv.Key) == t.ETCDUserKey():
			t.User = string(kv.Value)
		case string(kv.Key) == t.ETCDCommentKey():
			t.Comment = string(kv.Value)
		case string(kv.Key) == t.ETCDStartTimeKey():
			startTime, err := time.Parse(time.RFC3339, string(kv.Value))
			if err != nil {
				return nil, fmt.Errorf("transfer[%s]: failed to parse startTime: %v", id, err)
			}
			t.StartTime = timestamppb.New(startTime)
		case string(kv.Key) == t.ETCDEndTimeKey():
			endTime, err := time.Parse(time.RFC3339, string(kv.Value))
			if err != nil {
				return nil, fmt.Errorf("transfer[%s]: failed to parse endTime: %v", id, err)
			}
			t.EndTime = timestamppb.New(endTime)
		case string(kv.Key) == t.ETCDCreatedTimeKey():
			createdTime, err := time.Parse(time.RFC3339, string(kv.Value))
			if err != nil {
				return nil, fmt.Errorf("transfer[%s]: failed to parse createdTime: %v", id, err)
			}
			t.CreatedTime = timestamppb.New(createdTime)
		case string(kv.Key) == t.ETCDErrorMessageKey():
			t.ErrorMessage = strings.ToValidUTF8(string(kv.Value), "[invalid-utf8]")
		case string(kv.Key) == t.ETCDPriorityKey():
			priority, err := strconv.Atoi(string(kv.Value))
			if err != nil {
				return nil, fmt.Errorf("transfer[%s]: failed to parse priority: %v", id, err)
			}
			t.Priority = uint32(priority)
		case string(kv.Key) == t.ETCDExpiryKey():
			expiryTime, err := time.Parse(time.RFC3339, string(kv.Value))
			if err != nil {
				return nil, fmt.Errorf("transfer[%s]: failed to parse lease expiry time: %v", id, err)
			}
			t.Expiry = timestamppb.New(expiryTime)
		case strings.HasPrefix(string(kv.Key), t.ETCDSchedulerNodesKey(proto.SchedulerCommand_NONE)):
			_, sc, err := proto.ParseETCDTransfersKey(string(kv.Key))
			if err != nil {
				return nil, err
			}
			switch sc {
			case proto.SchedulerCommand_VALIDATION:
				t.SchedulerNodes.Validation = string(kv.Value)
			case proto.SchedulerCommand_SETUP:
				t.SchedulerNodes.Setup = string(kv.Value)
			case proto.SchedulerCommand_TRANSFER:
				t.SchedulerNodes.Transfer = string(kv.Value)
			case proto.SchedulerCommand_TEARDOWN:
				t.SchedulerNodes.Teardown = string(kv.Value)
			}
		}
	}
	return t, nil
}

// ConvertETCDTransfer will return a slice of ETCD Ops to put a transferDetails object in ETCD
func ConvertETCDTransfer(t *proto.TransferDetails) ([]clientv3.Op, error) {
	ops := []clientv3.Op{}

	sourceList, err := json.Marshal(t.GetSource())
	if err != nil {
		return nil, fmt.Errorf("transfer[%s]: failed to marshal transfer source for etcd: %v", t.GetTransferID(), err)
	}

	options, err := json.Marshal(t.GetOptions())
	if err != nil {
		return nil, fmt.Errorf("transfer[%s]: failed to marshal transfer options for etcd: %v", t.GetTransferID(), err)
	}

	warningsList, err := json.Marshal(t.GetWarnings())
	if err != nil {
		return nil, fmt.Errorf("transfer[%s]: failed to marshal transfer warnings for etcd: %v", t.GetTransferID(), err)
	}

	leaseList, err := protojson.Marshal(t.GetLeases())
	if err != nil {
		return nil, fmt.Errorf("transfer[%s]: failed to marshal transfer leases for etcd: %v", t.GetTransferID(), err)
	}

	// fullDestinationsList, err := json.Marshal(t.GetFullDestinations())
	// if err != nil {
	// 	return nil, fmt.Errorf("transfer[%s]: failed to marshal transfer user destinations for etcd: %v", t.GetTransferID(), err)
	// }

	// create ops for transfer state, error, user, starttime, endtime, and error message
	ops = append(ops, clientv3.OpPut(t.ETCDStateKey(), t.GetState().String()))
	ops = append(ops, clientv3.OpPut(t.ETCDErrorKey(), t.GetError().String()))
	ops = append(ops, clientv3.OpPut(t.ETCDSourceKey(), string(sourceList)))
	ops = append(ops, clientv3.OpPut(t.ETCDWarningsKey(), string(warningsList)))
	ops = append(ops, clientv3.OpPut(t.ETCDLeasesKey(), string(leaseList)))
	ops = append(ops, clientv3.OpPut(t.ETCDDestinationKey(), t.GetDestination()))
	ops = append(ops, clientv3.OpPut(t.ETCDActiveKey(), strconv.FormatBool(t.GetActive())))
	ops = append(ops, clientv3.OpPut(t.ETCDUserKey(), t.GetUser()))
	ops = append(ops, clientv3.OpPut(t.ETCDStartTimeKey(), t.GetStartTime().AsTime().Format(time.RFC3339)))
	ops = append(ops, clientv3.OpPut(t.ETCDEndTimeKey(), t.GetEndTime().AsTime().Format(time.RFC3339)))
	ops = append(ops, clientv3.OpPut(t.ETCDCreatedTimeKey(), t.GetCreatedTime().AsTime().Format(time.RFC3339)))
	ops = append(ops, clientv3.OpPut(t.ETCDErrorMessageKey(), t.GetErrorMessage()))
	ops = append(ops, clientv3.OpPut(t.ETCDActionKey(), t.GetAction()))
	ops = append(ops, clientv3.OpPut(t.ETCDOptionsKey(), string(options)))
	ops = append(ops, clientv3.OpPut(t.ETCDCommentKey(), t.GetComment()))
	ops = append(ops, clientv3.OpPut(t.ETCDPausedStateKey(), t.GetPausedState().String()))
	ops = append(ops, clientv3.OpPut(t.ETCDExpiryKey(), t.GetExpiry().AsTime().Format(time.RFC3339)))
	ops = append(ops, clientv3.OpPut(t.ETCDArchiveStateKey(), t.GetArchiveState().String()))
	ops = append(ops, clientv3.OpPut(t.ETCDDestInfoKey(), t.GetDestInfo().String()))
	ops = append(ops, clientv3.OpPut(t.ETCDValidationOnlyKey(), strconv.FormatBool(t.GetValidationOnly())))
	ops = append(ops, clientv3.OpPut(t.ETCDPluginDataKey(), string(t.GetPluginData())))
	// ops = append(ops, clientv3.OpPut(t.ETCDFullDestinationsKey(), string(fullDestinationsList)))
	ops = append(ops, clientv3.OpPut(t.ETCDPriorityKey(), strconv.Itoa(int(t.GetPriority()))))

	// add ops for schedulerNodes struct
	for _, scv := range proto.SchedulerCommand_value {
		sc := proto.SchedulerCommand(scv)
		switch sc {
		case proto.SchedulerCommand_VALIDATION:
			ops = append(ops, clientv3.OpPut(t.ETCDSchedulerNodesKey(sc), t.GetSchedulerNodes().GetValidation()))
		case proto.SchedulerCommand_SETUP:
			ops = append(ops, clientv3.OpPut(t.ETCDSchedulerNodesKey(sc), t.GetSchedulerNodes().GetSetup()))
		case proto.SchedulerCommand_TRANSFER:
			ops = append(ops, clientv3.OpPut(t.ETCDSchedulerNodesKey(sc), t.GetSchedulerNodes().GetTransfer()))
		case proto.SchedulerCommand_TEARDOWN:
			ops = append(ops, clientv3.OpPut(t.ETCDSchedulerNodesKey(sc), t.GetSchedulerNodes().GetTeardown()))
		}
	}

	// create op for transfer status details
	esd, err := t.ETCDStatusDetails()
	if err != nil {
		return nil, fmt.Errorf("transfer[%s]: failed to marshal transfer status details for etcd: %v", t.GetTransferID(), err)
	}
	ops = append(ops, clientv3.OpPut(t.ETCDStatusDetailsKey(), string(esd)))

	return ops, nil
}
