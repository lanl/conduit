// Copyright 2026. Triad National Security, LLC. All rights reserved.

package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/lanl/conduit/api"
	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/defaults"
	"github.com/lanl/conduit/internal/etcd"
	"github.com/lanl/conduit/internal/fta/plugin"
	"github.com/lanl/conduit/internal/logger"
	"github.com/lanl/conduit/internal/runner/peercred"
	"github.com/spf13/viper"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/emptypb"
)

var _ api.ConduitFTAApiServer = (*FtaApi)(nil)

type FtaApi struct {
	api.UnimplementedConduitFTAApiServer
	log *logger.ConduitLogger

	runner *Runner

	// These define the authority of this FTA.
	transferID uuid.UUID
	command    api.SchedulerCommand
	nodes      []string
}

func NewFtaApi(runner *Runner, transferID uuid.UUID, command api.SchedulerCommand, nodes []string) *FtaApi {
	l := logger.NewConduitLogger(runner.log.GetLevel(), fmt.Sprintf("%s [%s] api:", runner.log.GetPrefix(), transferID))
	if runner.log.GetPrefix() == "" {
		l = logger.NewConduitLogger(runner.log.GetLevel(), fmt.Sprintf("[%s] api:", transferID))
	}

	return &FtaApi{
		log:        l,
		runner:     runner,
		transferID: transferID,
		command:    command,
		nodes:      append([]string(nil), nodes...),
	}
}

func acceptFTA(listener *net.UnixListener, expectedPID int, expectedUID uint32) (*net.UnixConn, error) {
	err := listener.SetDeadline(time.Now().Add(30 * time.Second))
	if err != nil {
		return nil, err
	}

	for {
		conn, err := listener.AcceptUnix()
		if err != nil {
			return nil, err
		}

		cred, err := peercred.GetPeerCredentials(conn)
		if err != nil {
			_ = conn.Close()
			continue
		}

		if cred.Pid != expectedPID ||
			cred.Uid != expectedUID {

			_ = conn.Close()
			continue
		}

		return conn, nil
	}
}

type singleConnListener struct {
	conn net.Conn
	addr net.Addr

	mu     sync.Mutex
	closed chan struct{}
}

func newSingleConnListener(conn net.Conn) *singleConnListener {
	return &singleConnListener{
		conn:   conn,
		addr:   conn.LocalAddr(),
		closed: make(chan struct{}),
	}
}

func (l *singleConnListener) Accept() (net.Conn, error) {
	l.mu.Lock()

	if l.conn != nil {
		conn := l.conn
		l.conn = nil
		l.mu.Unlock()

		return conn, nil
	}

	l.mu.Unlock()

	<-l.closed

	return nil, net.ErrClosed
}

func (l *singleConnListener) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	select {
	case <-l.closed:
		return nil
	default:
		close(l.closed)
	}

	if l.conn != nil {
		err := l.conn.Close()
		l.conn = nil
		return err
	}

	return nil
}

func (l *singleConnListener) Addr() net.Addr {
	return l.addr
}

func (f *FtaApi) GetTransfer(ctx context.Context, _ *emptypb.Empty) (*api.TransferDetails, error) {
	f.log.Debugf("get transfer message received for transfer[%s][%s]", f.transferID, f.command)
	transfer, _, err := f.runner.em.GetTransfer(f.transferID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get transfer: %v", err)
	}

	return transfer, nil
}

func (f *FtaApi) Start(ctx context.Context, _ *emptypb.Empty) (*api.FTAStartResponse, error) {
	f.log.Debugf("start plugin message received for transfer[%s][%s]", f.transferID, f.command)
	it := api.IncompleteTransfer(&api.TransferDetails{TransferID: f.transferID.String()})

	// sometimes if conduit gets hammered with requests, it might not be able to tell etcd that the validation state was submitted before
	// we check for it here. Therefore we retry for a bit before giving up
	retryCount := viper.GetInt(defaults.ConfigFTAVerifyRetryCountKey)
	sleepDur := viper.GetDuration(defaults.ConfigFTAVerifySleepDurationKey)

	etcdErrorKey := it.ETCDErrorKey()
	etcdStateKey := it.ETCDStateKey()

	submittedState, runningState, _, err := getCommandStates(f.command)
	if err != nil {
		rErr := fmt.Errorf("failed to get command states for command[%s]: %v", f.command, err)
		return &api.FTAStartResponse{
			Error:        api.Error_ERROR_CONDUIT_INTERNAL,
			ErrorMessage: rErr.Error(),
		}, rErr
	}

	txnCompare := []clientv3.Cmp{
		clientv3.Compare(clientv3.Value(etcdErrorKey), "=", api.Error_ERROR_NONE.String()),
		clientv3.Compare(clientv3.Value(etcdStateKey), "=", submittedState.String()),
	}

	txnActions := []clientv3.Op{
		clientv3.OpPut(etcdStateKey, runningState.String()),
		clientv3.OpPut(it.ETCDSchedulerNodesKey(f.command), strings.Join(f.nodes, ",")),
	}

	txnElses := []clientv3.Op{
		clientv3.OpGet(it.ETCDStateKey()),
		clientv3.OpGet(it.ETCDErrorKey()),
		clientv3.OpGet(it.ETCDErrorMessageKey()),
	}

	resp, err := f.runner.em.RetryTxn(&txnCompare, &txnActions, &txnElses, retryCount, sleepDur)
	if err != nil {
		rErr := fmt.Errorf("failed to set plugin start state in etcd: %v", err)

		return &api.FTAStartResponse{
			Error:        api.Error_ERROR_ETCD_CONNECTION,
			ErrorMessage: rErr.Error(),
		}, rErr
	}

	// if everything got added to etcd, return
	if resp.Succeeded {
		return &api.FTAStartResponse{Error: api.Error_ERROR_NONE, ErrorMessage: ""}, nil
	}

	var state, spErr, errMessage string

	if len(resp.Responses) == 3 &&
		len(resp.Responses[0].GetResponseRange().Kvs) != 0 &&
		len(resp.Responses[1].GetResponseRange().Kvs) != 0 &&
		len(resp.Responses[2].GetResponseRange().Kvs) != 0 {

		state = string(resp.Responses[0].GetResponseRange().Kvs[0].Value)
		spErr = string(resp.Responses[1].GetResponseRange().Kvs[0].Value)
		errMessage = string(resp.Responses[2].GetResponseRange().Kvs[0].Value)
	}

	var pErr api.Error
	if vpErr, ok := api.Error_value[spErr]; ok {
		pErr = api.Error(vpErr)
	}

	rErr := fmt.Errorf("setting plugin start state in etcd was unsuccessful:[state=%v,error=%s,errMessage=%v]", state, pErr, errMessage)
	return &api.FTAStartResponse{Error: api.Error_ERROR_CONDUIT_INTERNAL, ErrorMessage: rErr.Error()}, rErr
}

// CompletePluginETCD sets the related keys in etcd to signal that the plugin has ended on the FTA node
func (f *FtaApi) Complete(ctx context.Context, req *api.FTACompleteRequest) (*api.FTACompleteResponse, error) {
	f.log.Debugf("complete plugin message received for transfer[%s][%s]", f.transferID, f.command)

	it := api.IncompleteTransfer(&api.TransferDetails{TransferID: f.transferID.String()})

	// sometimes if conduit gets hammered with requests, it might not be able to tell etcd that the validation state was submitted before
	// we check for it here. Therefore we retry for a bit before giving up
	retryCount := viper.GetInt(defaults.ConfigFTAVerifyRetryCountKey)
	sleepDur := viper.GetDuration(defaults.ConfigFTAVerifySleepDurationKey)

	txnCompare := []clientv3.Cmp{}
	txnActions := []clientv3.Op{}

	_, runningState, completedState, err := getCommandStates(f.command)
	if err != nil {
		rErr := fmt.Errorf("failed to get command states for command[%s]: %v", f.command, err)
		return &api.FTACompleteResponse{
			Error:        api.Error_ERROR_CONDUIT_INTERNAL,
			ErrorMessage: rErr.Error(),
		}, rErr
	}

	// try to set the plugin to complete
	txnCompare = []clientv3.Cmp{
		clientv3.Compare(clientv3.Value(it.ETCDErrorKey()), "=", api.Error_ERROR_NONE.String()),
		clientv3.Compare(clientv3.Value(it.ETCDStateKey()), "=", runningState.String()),
	}

	txnActions = []clientv3.Op{
		clientv3.OpPut(it.ETCDStateKey(), completedState.String()),
	}

	if f.command == api.SchedulerCommand_VALIDATION {
		sourceLeases := []string{}
		destinationLeases := []string{}

		if req.PluginData != nil {
			pluginData, err := plugin.DecodePluginData(bytes.NewReader(req.GetPluginData()))
			if err != nil {
				rErr := fmt.Errorf("failed to decode pluginData[%s]: %v", string(req.GetPluginData()), err)
				return &api.FTACompleteResponse{
					Error:        api.Error_ERROR_CONDUIT_INTERNAL,
					ErrorMessage: rErr.Error(),
				}, rErr
			}

			for _, s := range pluginData.SourcePluginInfo {
				sourceLeases = append(sourceLeases, s.ResolvedFTAPath)
			}

			for _, d := range pluginData.DestinationsPluginInfo {
				destinationLeases = append(destinationLeases, d.ResolvedFTAPath)
			}
		}

		allLeasesJSON, err := protojson.Marshal(&api.Leases{
			Source:      sourceLeases,
			Destination: destinationLeases,
		})
		if err != nil {
			rErr := fmt.Errorf("failed to marshal lease list[%v, %v] into json for transfer: %v", sourceLeases, destinationLeases, err)
			return &api.FTACompleteResponse{
				Error:        api.Error_ERROR_CONDUIT_INTERNAL,
				ErrorMessage: rErr.Error(),
			}, rErr

			// log.Error(tErr)
			// transferError = tErr
		}

		txnActions = append(txnActions, clientv3.OpPut(it.ETCDLeasesKey(), string(allLeasesJSON)))

		// add warnings for a validation job. This assumes that there will not be any warnings before validation and overwrites anything existing there
		if len(req.GetPluginErrors().GetWarnings()) > 0 {
			warnlist := []string{}
			for _, w := range req.GetPluginErrors().GetWarnings() {
				warnlist = append(warnlist, fmt.Sprintf("%s: %s", f.command, w.GetErrMessage()))
			}

			warningsJson, err := json.Marshal(warnlist)
			if err != nil {
				rErr := fmt.Errorf("transfer[%s]: failed to marshal transfer warnings[%v] for etcd: %v", it.GetTransferID(), warnlist, err)
				return &api.FTACompleteResponse{
					Error:        api.Error_ERROR_CONDUIT_INTERNAL,
					ErrorMessage: rErr.Error(),
				}, rErr
			}

			txnActions = append(txnActions, clientv3.OpPut(it.ETCDWarningsKey(), string(warningsJson)))
		}
	} else {
		if len(req.GetPluginErrors().GetWarnings()) > 0 {
			err := warnTransfer(f.log, f.runner.em, it, req.GetPluginErrors().GetWarnings(), f.command)
			if err != nil {
				f.log.Errorf("failed to add warnings: %v", err)
			}
		}
	}

	if req.GetDestInfo() != api.DestInfo_DEST_NONE {
		// txnActions = append(txnActions, clientv3.OpPut(ft.transfer.ETCDFullDestinationsKey(), string(fullDestJSON)))
		txnActions = append(txnActions, clientv3.OpPut(it.ETCDDestInfoKey(), req.GetDestInfo().String()))
	}

	if req.PluginData != nil {
		txnActions = append(txnActions, clientv3.OpPut(it.ETCDPluginDataKey(), string(req.PluginData)))
	}

	resp, err := f.runner.em.RetryTxn(&txnCompare, &txnActions, nil, retryCount, sleepDur)
	if err != nil {
		rErr := fmt.Errorf("failed to set plugin complete state in etcd: %v", err)
		return &api.FTACompleteResponse{
			Error:        api.Error_ERROR_ETCD_CONNECTION,
			ErrorMessage: rErr.Error(),
		}, rErr
	}
	if !resp.Succeeded {
		rErr := fmt.Errorf("setting plugin complete state in etcd was unsuccessful")
		return &api.FTACompleteResponse{
			Error:        api.Error_ERROR_ETCD_INTERNAL,
			ErrorMessage: rErr.Error(),
		}, rErr

	}

	return &api.FTACompleteResponse{Error: api.Error_ERROR_NONE, ErrorMessage: ""}, nil
}

// ErrorPluginETCD sets the related keys in etcd to signal that the plugin has encountered an error on the FTA node
func (f *FtaApi) Fail(ctx context.Context, req *api.FTAFailRequest) (*api.FTAFailResponse, error) {
	f.log.Debugf("fail plugin message received for transfer[%s][%s]", f.transferID, f.command)

	it := api.IncompleteTransfer(&api.TransferDetails{TransferID: f.transferID.String()})

	defer func(warnings []*api.FTAPathError, ict proto.IncompleteTransfer) {
		if len(warnings) > 0 {
			err := warnTransfer(f.log, f.runner.em, ict, warnings, f.command)
			if err != nil {
				f.log.Errorf("failed to add warnings: %v", err)
			}
		}
	}(req.GetPluginErrors().GetWarnings(), it)

	retryCount := viper.GetInt(defaults.ConfigFTAVerifyRetryCountKey)
	sleepDur := viper.GetDuration(defaults.ConfigFTAVerifySleepDurationKey)

	etcdErrorKey := it.ETCDErrorKey()
	etcdErrorMessageKey := it.ETCDErrorMessageKey()

	pErr := proto.Error_ERROR_CONDUIT_INTERNAL
	var sErr string
	if len(req.GetPluginErrors().GetErrors()) > 1 {
		// if we have more than one error, use the error status of the first one and cat all the other errors into the message
		for _, e := range req.GetPluginErrors().GetErrors() {
			if sErr == "" {
				sErr = e.ErrMessage
			} else {
				sErr = fmt.Sprintf("%v; %v", sErr, e.ErrMessage)
			}
			if pErr == proto.Error_ERROR_NONE && e.PErr != proto.Error_ERROR_NONE {
				pErr = e.PErr
			}
		}
	} else if len(req.GetPluginErrors().GetErrors()) == 1 {
		for _, e := range req.GetPluginErrors().GetErrors() {
			sErr = e.ErrMessage
			pErr = e.PErr
		}
	} else {
		rErr := fmt.Errorf("fail transfer was called with no provided errors")
		return &api.FTAFailResponse{
			Error:        api.Error_ERROR_CONDUIT_INTERNAL,
			ErrorMessage: rErr.Error(),
		}, rErr

	}

	if f.command == proto.SchedulerCommand_VALIDATION {
		pErr = proto.Error_ERROR_VALIDATION
	}

	txnCompare := []clientv3.Cmp{
		clientv3.Compare(clientv3.Value(etcdErrorKey), "=", proto.Error_ERROR_NONE.String()),
	}
	txnActions := []clientv3.Op{
		clientv3.OpPut(etcdErrorKey, pErr.String()),
		clientv3.OpPut(etcdErrorMessageKey, sErr),
	}

	if req.GetDestInfo() != proto.DestInfo_DEST_NONE {
		// txnActions = append(txnActions, clientv3.OpPut(ft.transfer.ETCDFullDestinationsKey(), string(fullDestJSON)))
		txnActions = append(txnActions, clientv3.OpPut(it.ETCDDestInfoKey(), req.GetDestInfo().String()))
	}

	if req.PluginData != nil {
		txnActions = append(txnActions, clientv3.OpPut(it.ETCDPluginDataKey(), string(req.PluginData)))
	}

	resp, err := f.runner.em.RetryTxn(&txnCompare, &txnActions, nil, retryCount, sleepDur)
	if err != nil {
		rErr := fmt.Errorf("failed to set plugin error state in etcd: %v", err)
		return &api.FTAFailResponse{
			Error:        api.Error_ERROR_ETCD_CONNECTION,
			ErrorMessage: rErr.Error(),
		}, rErr
	}
	if !resp.Succeeded {
		rErr := fmt.Errorf("setting plugin complete error in etcd was unsuccessful")
		return &api.FTAFailResponse{
			Error:        api.Error_ERROR_ETCD_INTERNAL,
			ErrorMessage: rErr.Error(),
		}, rErr
	}

	return &api.FTAFailResponse{Error: api.Error_ERROR_NONE, ErrorMessage: ""}, nil
}

// warnTransfer simply adds warnings to etcd for a specific transfer
func warnTransfer(log *logger.ConduitLogger, em *etcd.ETCDManager, it proto.IncompleteTransfer, warnings []*api.FTAPathError, command proto.SchedulerCommand) error {
	warnlist := []string{}
	for _, w := range warnings {
		warnlist = append(warnlist, fmt.Sprintf("%s: %s", command, w.ErrMessage))
	}
	err := em.AddWarnings(it, warnlist)
	if err != nil {
		return fmt.Errorf("failed to add warnings in etcd: %v", err)
	}

	log.Debugf("successfully added warnings: %v", warnlist)
	return nil
}

func (f *FtaApi) Heartbeat(ctx context.Context, _ *emptypb.Empty) (*api.FTAHeartbeatResponse, error) {
	f.log.Debugf("heartbeat message received for transfer[%s][%s]", f.transferID, f.command)

	it := api.IncompleteTransfer(&api.TransferDetails{TransferID: f.transferID.String()})

	succeed, err, newExpiry, transferError := f.runner.em.UpdateExpiryOnce(it, "")

	return &proto.FTAHeartbeatResponse{TransferError: transferError, Successful: succeed, NewExpiry: newExpiry}, err
}

func (f *FtaApi) UpdateStatus(ctx context.Context, esd *api.ETCDStatusDetails) (*emptypb.Empty, error) {
	f.log.Debugf("update status message received for transfer[%s][%s]", f.transferID, f.command)

	it := api.IncompleteTransfer(&api.TransferDetails{TransferID: f.transferID.String()})

	txnCompare := []clientv3.Cmp{
		clientv3.Compare(clientv3.Value(it.ETCDActiveKey()), "=", strconv.FormatBool(true)),
	}

	esdJson, err := protojson.Marshal(esd)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal status details: %v", err)
	}

	txnActions := []clientv3.Op{
		clientv3.OpPut(it.ETCDStatusDetailsKey(), string(esdJson)),
	}

	retryCount := viper.GetInt(defaults.ConfigFTAVerifyRetryCountKey)
	sleepDur := viper.GetDuration(defaults.ConfigFTAVerifySleepDurationKey)

	resp, err := f.runner.em.RetryTxn(&txnCompare, &txnActions, nil, retryCount, sleepDur)
	if err != nil || !resp.Succeeded {
		return nil, fmt.Errorf("failed to update transfer progress in etcd: %v %v", err, resp.Succeeded)
	}

	return nil, nil
}

func (f *FtaApi) UpdateAction(ctx context.Context, actionUpdate *api.FTAActionUpdate) (*emptypb.Empty, error) {
	f.log.Debugf("update action message received for transfer[%s][%s]", f.transferID, f.command)

	it := api.IncompleteTransfer(&api.TransferDetails{TransferID: f.transferID.String()})

	txnCompare := []clientv3.Cmp{
		clientv3.Compare(clientv3.Value(it.ETCDActionKey()), "=", actionUpdate.GetCurrentAction()),
	}

	txnActions := []clientv3.Op{
		clientv3.OpPut(it.ETCDActionKey(), actionUpdate.GetNewAction()),
	}

	retryCount := viper.GetInt(defaults.ConfigFTAVerifyRetryCountKey)
	sleepDur := viper.GetDuration(defaults.ConfigFTAVerifySleepDurationKey)

	resp, err := f.runner.em.RetryTxn(&txnCompare, &txnActions, nil, retryCount, sleepDur)
	if err != nil || !resp.Succeeded {
		return nil, fmt.Errorf("failed to set new action in etcd: %v", err)
	}

	return nil, nil
}
