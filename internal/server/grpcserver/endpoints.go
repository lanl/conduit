// Copyright 2026. Triad National Security, LLC. All rights reserved.

package grpcserver

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/google/uuid"
	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/defaults"
	"github.com/spf13/viper"
	goproto "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// StartTransfer is the initial handle of all StartTransfer requests from the gRPC API
func (s *ConduitServer) StartTransfer(ctx context.Context, tr *proto.TransferRequest) (*proto.TransferDetails, error) {
	// check if server is shutting down
	s.ssMutex.RLock()
	if s.Shutdown {
		s.ssMutex.RUnlock()
		return nil, fmt.Errorf("server shutting down")
	} else {
		s.ssMutex.RUnlock()
		s.jobs.Add(1)
		defer s.jobs.Done()
	}

	// limit character count. This is to prevent a user from passing a wildcard that blows out etcd and slows down queries
	maxSourceBytes := viper.GetInt(defaults.ConfigMaxSourceBytesKey)
	bs := []byte{}
	for _, s := range tr.GetSource() {
		bs = append(bs, []byte(s)...)
	}

	if len(bs) > maxSourceBytes {
		err := fmt.Errorf("request contains too many sources. byte limit: %v, received: %v. Please use a directory instead of a wildcard when transferring a large number of sources", maxSourceBytes, len(bs))
		s.log.Error(err)
		return &proto.TransferDetails{Source: []string{}, Destination: "", User: "", StartTime: nil, EndTime: nil, State: proto.TransferState_TRANSFER_ERROR, Error: proto.Error_ERROR_MAX_ARG_STRLEN}, err
	}

	// get the user from the request
	requestedUser := tr.GetUser()
	user, reqPrivLevel, err := s.getUserFromRequest(ctx, &requestedUser)
	if err != nil {
		err := fmt.Errorf("error getting user from request: %v", err)
		s.log.Error(err)
		return &proto.TransferDetails{Source: []string{}, Destination: "", User: "", StartTime: nil, EndTime: nil, State: proto.TransferState_TRANSFER_ERROR, Error: proto.Error_ERROR_AUTH}, err
	}
	if user == "" {
		err := fmt.Errorf("no user provided in request")
		s.log.Error(err)
		return &proto.TransferDetails{Source: []string{}, Destination: "", User: "", StartTime: nil, EndTime: nil, State: proto.TransferState_TRANSFER_ERROR, Error: proto.Error_ERROR_AUTH}, err
	}

	s.log.Debugf("received StartTransfer request from user: %v", user)

	transferID := uuid.New()

	pauseState := proto.TransferState_TRANSFER_NONE
	if viper.GetBool(defaults.ConfigTestKey) && tr.GetPausedState() != proto.TransferState_TRANSFER_NONE {
		s.log.Debugf("received a transfer with a pause state: %v %v", tr.GetPausedState(), viper.GetBool(defaults.ConfigTestKey))
		pauseState = tr.GetPausedState()
	}

	comment := ""
	if reqPrivLevel == privilegedService || reqPrivLevel == privilegedAdmin {
		comment = tr.GetComment()
	}

	transfer := proto.NewTransferDetails()

	transfer.State = proto.TransferState_TRANSFER_INIT
	transfer.TransferID = transferID.String()
	transfer.Source = tr.GetSource()
	transfer.Destination = tr.GetDestination()
	transfer.Active = true
	transfer.User = user
	transfer.CreatedTime = timestamppb.Now()
	transfer.Comment = comment
	transfer.Action = tr.GetAction()
	transfer.Options = tr.GetOptions()
	transfer.PausedState = pauseState
	transfer.ValidationOnly = false
	transfer.Expiry = timestamppb.New(time.Now().Add(viper.GetDuration(defaults.ConfigExpiryAdvanceKey)))

	if reqPrivLevel == privilegedAdmin {
		transfer.Warnings = append(transfer.Warnings, fmt.Sprintf("%s (Start Transfer)", adminWarning))
	}

	errantExpiration := viper.GetDuration(defaults.ConfigErrantExpiration)
	if errantExpiration != 0 {
		s.eMutex.RLock()
		// check if user has any errant paths that have been around too long
		if ue, ok := s.usersErrants[user]; ok {
			for p, t := range ue {
				if !t.AsTime().IsZero() && t.AsTime().Add(errantExpiration).Before(time.Now()) {
					err := fmt.Errorf("user has errored trash transfer path(s) that are too old: %v", p)
					s.eMutex.RUnlock()
					return &proto.TransferDetails{Source: []string{}, Destination: "", User: "", StartTime: nil, EndTime: nil, State: proto.TransferState_TRANSFER_ERROR, Error: proto.Error_ERROR_VALIDATION}, err
				}
			}
		}
		s.eMutex.RUnlock()
	}

	s.em.SubmitTransfer(transfer)

	return transfer, nil
}

// StopTransfer will stop a transfer that is in progress
func (s *ConduitServer) StopTransfer(ctx context.Context, tids *proto.TransferIds) (*proto.MultiTransferDetails, error) {
	// check if server is shutting down
	s.ssMutex.RLock()
	if s.Shutdown {
		s.ssMutex.RUnlock()
		return nil, fmt.Errorf("server shutting down")
	} else {
		s.ssMutex.RUnlock()
		s.jobs.Add(1)
		defer s.jobs.Done()
	}

	// get the user from the request
	user, reqPrivLevel, err := s.getUserFromRequest(ctx, nil)
	if err != nil {
		err := fmt.Errorf("error getting user from request: %v", err)
		s.log.Error(err)
		mtd := &proto.MultiTransferDetails{}
		return mtd, err
	}

	s.log.Debugf("received StopTransfer request from user[%v] for ids: %v", user, tids.GetValue())

	s.tMutex.RLock()
	defer s.tMutex.RUnlock()

	rtids := []uuid.UUID{}

	for _, tid := range tids.GetValue() {
		requestedTransferID, err := uuid.Parse(tid)
		if err != nil {
			err := fmt.Errorf("failed to parse transferid[%s]: %v", tid, err)
			s.log.Error(err)
			td := &proto.TransferDetails{Source: []string{}, Destination: "", User: "", StartTime: nil, EndTime: nil, Error: proto.Error_ERROR_CONDUIT_INTERNAL}
			mtd := &proto.MultiTransferDetails{Details: map[string]*proto.TransferDetails{"": td}}
			return mtd, err
		}
		rtids = append(rtids, requestedTransferID)
	}

	transfers := make(map[string]*proto.TransferDetails)

	for _, rtid := range rtids {
		// check if this was a privileged request
		if reqPrivLevel == unprivileged {
			// user is not a privileged account so they can only abort their own transfers
			userTransfers, ok := s.usersTransfers[user]
			if ok {
				_, ok := userTransfers[rtid]
				if !ok {
					// transfer doesn't exist for user
					return &proto.MultiTransferDetails{}, fmt.Errorf("transfer[%s] not found for user[%s]", rtid, user)
				}
				transfers[rtid.String()] = s.transfers[rtid.String()]
			} else {
				return &proto.MultiTransferDetails{}, fmt.Errorf("no transfers found for user: %s", user)
			}
		} else {
			// user is privileged so they can abort any transfer
			t, ok := s.transfers[rtid.String()]
			if !ok {
				return &proto.MultiTransferDetails{}, fmt.Errorf("transfer[%s] not found in conduit", rtid)
			}
			transfers[rtid.String()] = t
		}
	}

	var finalErrors error

	for _, t := range transfers {
		if reqPrivLevel == privilegedAdmin {
			err := s.em.AddWarnings(t, []string{fmt.Sprintf("%s (Stop Transfer)", adminWarning)})
			if err != nil {
				s.log.Errorf("failed to add warnings for transfer[%s]: %v", t.GetTransferID(), err)
			}
		}
		err := s.em.AbortTransfer(t, fmt.Errorf("user aborted"))
		if err != nil {
			if finalErrors != nil {
				finalErrors = fmt.Errorf("%s; %s", finalErrors.Error(), err)
			} else {
				finalErrors = err
			}
		}
	}

	mtd := goproto.Clone(&proto.MultiTransferDetails{Details: transfers}).(*proto.MultiTransferDetails)

	return mtd, finalErrors
}

func (s *ConduitServer) PauseTransfer(ctx context.Context, pr *proto.PauseRequest) (*proto.TransferDetails, error) {
	// check if server is shutting down
	s.ssMutex.RLock()
	if s.Shutdown {
		s.ssMutex.RUnlock()
		return nil, fmt.Errorf("server shutting down")
	} else {
		s.ssMutex.RUnlock()
		s.jobs.Add(1)
		defer s.jobs.Done()
	}

	if !viper.GetBool(defaults.ConfigTestKey) {
		return nil, fmt.Errorf("pause command only allowed while conduit is in test mode")
	}

	// get the user from the request
	user, reqPrivLevel, err := s.getUserFromRequest(ctx, nil)
	if err != nil {
		err := fmt.Errorf("error getting user from request: %v", err)
		s.log.Error(err)
		return &proto.TransferDetails{Source: []string{}, Destination: "", User: "", StartTime: nil, EndTime: nil, State: proto.TransferState_TRANSFER_ERROR, Error: proto.Error_ERROR_AUTH}, err
	}

	s.log.Debugf("received pause request from user[%v] for transer[%v] [%s]", user, pr.GetTransferID(), pr.GetPausedState().String())

	s.tMutex.RLock()
	defer s.tMutex.RUnlock()

	requestedTransferID, err := uuid.Parse(pr.GetTransferID())
	if err != nil {
		return nil, fmt.Errorf("failed to parse transferid[%s]: %v", pr.GetTransferID(), err)
	}
	var thisTransfer *proto.TransferDetails

	// if user is a privileged account, assume they can pause any transfer in conduit
	if reqPrivLevel == unprivileged {
		// user is not a privileged account so they can only pause their own transfers
		userTransfers, ok := s.usersTransfers[user]
		if ok {
			_, ok := userTransfers[requestedTransferID]
			if !ok {
				// transfer doesn't exist for user
				return nil, fmt.Errorf("transfer[%s] not found for user[%s]", pr.GetTransferID(), user)
			}
			thisTransfer = s.transfers[requestedTransferID.String()]
		} else {
			return nil, fmt.Errorf("no transfers found for user: %s", user)
		}
	} else {
		// user is privileged so they can pause any transfer
		var ok bool
		thisTransfer, ok = s.transfers[requestedTransferID.String()]
		if !ok {
			return nil, fmt.Errorf("transfer[%s] not found in conduit", requestedTransferID)
		}
	}

	succeed, err := s.em.PauseTransfer(thisTransfer, pr.GetPausedState())
	if err != nil {
		return thisTransfer, fmt.Errorf("failed to pause transfer[%s]: %v", requestedTransferID, err)
	}
	if !succeed {
		return thisTransfer, fmt.Errorf("pausing transfer[%s] did not succeed", requestedTransferID)
	}

	if reqPrivLevel == privilegedAdmin {
		err := s.em.AddWarnings(thisTransfer, []string{fmt.Sprintf("%s (Pause Transfer)", adminWarning)})
		if err != nil {
			s.log.Errorf("failed to add warnings for transfer[%s]: %v", thisTransfer.GetTransferID(), err)
		}
	}

	tt := goproto.Clone(thisTransfer).(*proto.TransferDetails)

	return tt, nil
}

// Query is the initial handle of all Query requests from the gRPC API
func (s *ConduitServer) Query(ctx context.Context, qo *proto.QueryOptions) (*proto.MultiTransferDetails, error) {
	s.log.Debugf("got query request")
	// get the user from the request
	requestedUser := qo.GetUser()
	user, reqPrivLevel, err := s.getUserFromRequest(ctx, &requestedUser)
	if err != nil {
		err = fmt.Errorf("error getting user from request: %v", err)
		s.log.Error(err)
		td := &proto.TransferDetails{Source: []string{}, Destination: "", User: "", StartTime: nil, EndTime: nil, Error: proto.Error_ERROR_AUTH}
		mtd := &proto.MultiTransferDetails{Details: map[string]*proto.TransferDetails{"": td}}
		return mtd, err
	}

	s.log.Debugf("verifying keys")

	// verify that the keys in the query map are actually in the transfer details
	qo.QueryMap, err = verifyKeys(qo.GetQueryMap(), queryFields)
	if err != nil {
		return nil, fmt.Errorf("provided query contains an invalid key: %v\npossible keys: %v", err, queryFields)
	}

	s.log.Debugf("get transfers by user")

	s.tMutex.RLock()
	defer s.tMutex.RUnlock()
	transfers := make(map[string]*proto.TransferDetails)

	// if user is a privileged account, assume they want all transfers in conduit
	if (reqPrivLevel == privilegedAdmin || reqPrivLevel == privilegedService) && user == "" {
		transfers = s.transfers
		rTransfers, err := s.rm.GetTransfersByUser(nil)
		if err != nil {
			return nil, fmt.Errorf("failed to get transfers from rqlite: %v", err)
		}
		for _, rt := range rTransfers {
			transfers[rt.GetTransferID()] = rt
		}
	} else {
		// user is not a privileged account so we only return transfers that are for that user
		transferIDs, ok := s.usersTransfers[user]
		if ok {
			for id := range transferIDs {
				transfers[id.String()] = s.transfers[id.String()]
			}
		}

		rTransfers, err := s.rm.GetTransfersByUser(&user)
		if err != nil {
			return nil, fmt.Errorf("failed to get user transfers from rqlite: %v", err)
		}
		for _, rt := range rTransfers {
			transfers[rt.GetTransferID()] = rt
		}
	}

	if len(transfers) == 0 {
		return &proto.MultiTransferDetails{
			Details: make(map[string]*proto.TransferDetails),
		}, nil
	}

	s.log.Debugf("filtering transfers")

	ft, err := filterTransfers(qo, transfers)
	if err != nil {
		return &proto.MultiTransferDetails{}, fmt.Errorf("failed to filter transfers: %v", err)
	}

	s.log.Debugf("cloning transfers")

	mtd := goproto.Clone(&proto.MultiTransferDetails{Details: ft}).(*proto.MultiTransferDetails)

	s.log.Debugf("returning transfers")

	return mtd, nil
}

// StartTransfer is the initial handle of all StartTransfer requests from the gRPC API
func (s *ConduitServer) Version(ctx context.Context, _ *emptypb.Empty) (*proto.VersionInfo, error) {
	vi := &proto.VersionInfo{
		Version:  "",
		Modified: false,
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" {
				vi.Version = setting.Value
			}
			if setting.Key == "vcs.modified" {
				if setting.Value == "true" {
					vi.Modified = true
				}
			}
			if setting.Key == "vcs.time" {
				vi.Time = setting.Value
			}
		}
	} else {
		return nil, fmt.Errorf("failed to get build info")
	}

	return vi, nil
}

func (s *ConduitServer) WatchStatus(tids *proto.TransferIds, stream proto.ConduitApi_WatchStatusServer) error {
	user, reqPrivLevel, err := s.getUserFromRequest(stream.Context(), nil)
	if err != nil {
		err = fmt.Errorf("error getting user from request: %v", err)
		s.log.Error(err)
		return err
	}

	if user != "" {
		s.log.Debugf("using user from context: %v", user)
	}

	requestedIDs := []string{}

	// clean up any blank transferIDs
	for _, value := range tids.GetValue() {
		if value != "" {
			requestedIDs = append(requestedIDs, value)
		}
	}

	userTransferIDs := make(map[string]bool)
	var rqliteFoundTransfers map[string]*proto.TransferDetails
	transferIDs := []uuid.UUID{}

	// get all transfers for this user
	// if user is a privileged account, assume they want all transfers in conduit
	if (reqPrivLevel == privilegedAdmin || reqPrivLevel == privilegedService) && user == "" {
		s.tMutex.RLock()
		for tid := range s.transfers {
			userTransferIDs[tid] = true
		}
		s.tMutex.RUnlock()
		rqliteFoundTransfers, err = s.rm.GetTransfersByUser(nil)
		if err != nil {
			return fmt.Errorf("failed to get transfers from rqlite: %v", err)
		}
		for _, rt := range rqliteFoundTransfers {
			userTransferIDs[rt.GetTransferID()] = true
		}
	} else {
		// user is not a privileged account so we only return transfers that are for that user
		s.tMutex.RLock()
		transferIDs, ok := s.usersTransfers[user]
		if ok {
			for tid := range transferIDs {
				userTransferIDs[tid.String()] = true
			}
		}
		s.tMutex.RUnlock()

		rqliteFoundTransfers, err = s.rm.GetTransfersByUser(&user)
		if err != nil {
			return fmt.Errorf("failed to get user transfers from rqlite: %v", err)
		}
		for _, rt := range rqliteFoundTransfers {
			userTransferIDs[rt.GetTransferID()] = true
		}
	}

	// check if user provided specific transfers to watch
	if len(requestedIDs) > 0 {
		finalUserTransferIDs := make(map[string]bool)
		for _, rid := range requestedIDs {
			if _, ok := userTransferIDs[rid]; ok {
				finalUserTransferIDs[rid] = true
			}
		}

		// if the user asked for specific IDs and we didn't find any. double check etcd to make sure the cache isn't just slow
		if len(finalUserTransferIDs) == 0 {
			for _, rid := range requestedIDs {
				uid, err := uuid.Parse(rid)
				if err != nil {
					err = fmt.Errorf("failed to parse transferID[%v]: %v", rid, err)
					s.log.Error(err)
					return err
				}

				it := proto.IncompleteTransfer(&proto.TransferDetails{TransferID: uid.String()})
				resUser, err := s.em.GetTransferUser(it)
				if err != nil {
					err = fmt.Errorf("failed to get user for tranfser[%v]: %v", rid, err)
					s.log.Error(err)
					// not returning the error because it could be that the user provided an invalid uid
				}
				// only add the transfer to the slice if the user matches our requested user
				if resUser == user {
					finalUserTransferIDs[rid] = true
				}
			}
		}

		userTransferIDs = finalUserTransferIDs
	}

	// convert to uuid
	for id := range userTransferIDs {
		uid, err := uuid.Parse(id)
		if err != nil {
			err = fmt.Errorf("failed to parse transferID[%v]: %v", id, err)
			s.log.Error(err)
			return err
		}

		transferIDs = append(transferIDs, uid)
	}

	if len(transferIDs) == 0 {
		return fmt.Errorf("failed to find transfers to watch")
	}

	streamID := uuid.New()
	// map of update channels used for aggregated updates
	uChans := make(map[uuid.UUID]chan bool)

	s.log.Debugf("starting stream[%v] for transfers[%v]", streamID, transferIDs)

	s.asMutex.Lock()

	// check for each transfer id, check if there is already a key for it in the activestreams map and add it if it isn't
	for _, id := range transferIDs {
		if _, ok := s.activeStreams[id]; !ok {
			streamMap := make(map[uuid.UUID]chan bool)
			s.activeStreams[id] = streamMap
		}
		uChan := make(chan bool)
		s.activeStreams[id][streamID] = uChan
		uChans[id] = uChan
	}

	s.asMutex.Unlock()

	// always send a full update first
	s.tMutex.RLock()
	details := make(map[string]*proto.TransferDetails)
	for _, id := range transferIDs {
		td, ok := s.transfers[id.String()]
		if ok {
			details[td.GetTransferID()] = td
		} else {
			s.log.Debugf("watch failed to find transferID in server cache, checking cache of rqlite transfers")
			// if it's not in the transfers, check the rqlite transfers we just got
			td, ok := rqliteFoundTransfers[id.String()]
			if ok {
				details[td.GetTransferID()] = td
			} else {
				s.log.Debugf("watch failed to find transferID in rqlite transfer cache, checking rqlite itself")
				// if their not in those rqlite transfers, check rqlite itself to see if it has been archived since the last time we talked to rqlite
				rqliteFoundTransfers, err = s.rm.GetTransfers([]string{id.String()})
				if err != nil {
					s.log.Errorf("failed to get transfer from rqlite for second check: %v", err)
					continue
				}
				for _, rt := range rqliteFoundTransfers {
					details[td.GetTransferID()] = rt
				}
			}
		}
	}

	mtd := goproto.Clone(&proto.MultiTransferDetails{Details: details}).(*proto.MultiTransferDetails)

	err = stream.Send(mtd)
	if err != nil {
		s.log.Errorf("failed to send multi transfer details to grpc stream[%v]: %v", streamID, err)
	}
	s.tMutex.RUnlock()

	// in order to watch n channels we need to create an aggregate channel
	//
	// if any updates come in from etcd, send that transfer id to the aggregate channel.
	agg := make(chan uuid.UUID, 50)
	for id, ch := range uChans {
		go func(id uuid.UUID, c chan bool) {
			for u := range c {
				switch u {
				case true:
					agg <- id
				case false:
					return
				}
			}
		}(id, ch)
	}

	for {
		select {
		case id := <-agg:
			// when a transfer id comes through the aggregate channel, send the entire transfer details object to the stream
			s.tMutex.RLock()
			td, ok := s.transfers[id.String()]
			if ok {
				details := map[string]*proto.TransferDetails{td.GetTransferID(): td}
				mtd := goproto.Clone(&proto.MultiTransferDetails{Details: details}).(*proto.MultiTransferDetails)
				err := stream.Send(mtd)
				if err != nil {
					s.log.Errorf("failed to send multi transfer details to grpc stream[%v]: %v", streamID, err)
				}
			}
			s.tMutex.RUnlock()

		case <-stream.Context().Done():
			s.log.Debugf("stream[%v] is closed", streamID)
			s.asMutex.Lock()
			for _, tid := range transferIDs {
				// tell the aggregate updater go routines to stop
				u := s.activeStreams[tid][streamID]
				u <- false
				delete(s.activeStreams[tid], streamID)
				if len(s.activeStreams[tid]) == 0 {
					delete(s.activeStreams, tid)
				}
			}
			s.asMutex.Unlock()

			return stream.Context().Err()
		}
	}
}

// ValidateTransfer will only run validation on a transfer and then stop
func (s *ConduitServer) ValidateTransfer(ctx context.Context, tr *proto.TransferRequest) (*proto.TransferDetails, error) {
	// check if server is shutting down
	s.ssMutex.RLock()
	if s.Shutdown {
		s.ssMutex.RUnlock()
		return nil, fmt.Errorf("server shutting down")
	} else {
		s.ssMutex.RUnlock()
		s.jobs.Add(1)
		defer s.jobs.Done()
	}

	// get the user from the request
	requestedUser := tr.GetUser()
	user, reqPrivLevel, err := s.getUserFromRequest(ctx, &requestedUser)
	if err != nil {
		err := fmt.Errorf("error getting user from request: %v", err)
		s.log.Error(err)
		return &proto.TransferDetails{Source: []string{}, Destination: "", User: "", StartTime: nil, EndTime: nil, State: proto.TransferState_TRANSFER_ERROR, Error: proto.Error_ERROR_AUTH}, err
	}
	if user == "" {
		err := fmt.Errorf("no user provided in request")
		s.log.Error(err)
		return &proto.TransferDetails{Source: []string{}, Destination: "", User: "", StartTime: nil, EndTime: nil, State: proto.TransferState_TRANSFER_ERROR, Error: proto.Error_ERROR_AUTH}, err
	}

	s.log.Debugf("received StartTransfer request from user: %v", user)

	transferID := uuid.New()

	pauseState := proto.TransferState_TRANSFER_NONE
	if viper.GetBool(defaults.ConfigTestKey) && tr.GetPausedState() != proto.TransferState_TRANSFER_NONE {
		s.log.Debugf("received a transfer with a pause state: %v %v", tr.GetPausedState(), viper.GetBool(defaults.ConfigTestKey))
		pauseState = tr.GetPausedState()
	}

	transfer := proto.NewTransferDetails()

	transfer.State = proto.TransferState_TRANSFER_INIT
	transfer.TransferID = transferID.String()
	transfer.Source = tr.GetSource()
	transfer.Destination = tr.GetDestination()
	transfer.Active = true
	transfer.User = user
	transfer.CreatedTime = timestamppb.Now()
	transfer.Comment = tr.GetComment()
	transfer.Action = tr.GetAction()
	transfer.Options = tr.GetOptions()
	transfer.PausedState = pauseState
	transfer.ValidationOnly = true
	transfer.Expiry = timestamppb.New(time.Now().Add(viper.GetDuration(defaults.ConfigExpiryAdvanceKey)))

	if reqPrivLevel == privilegedAdmin {
		transfer.Warnings = append(transfer.Warnings, fmt.Sprintf("%s (Validate Transfer)", adminWarning))
	}

	s.em.SubmitTransfer(transfer)

	return transfer, nil
}

func (s *ConduitServer) ServerControl(ctx context.Context, scr *proto.ServerControlRequest) (*proto.ServerControlResponse, error) {
	// get the user from the request
	user, reqPrivLevel, err := s.getUserFromRequest(ctx, nil)
	if err != nil {
		err := fmt.Errorf("error getting user from request: %v", err)
		s.log.Error(err)
		return nil, err
	}

	s.log.Debugf("received server control request from user[%v]: %v", user, reqPrivLevel)

	// verfiy that this is a privileged admin account
	if reqPrivLevel != privilegedAdmin {
		return nil, fmt.Errorf("you do not have permission to control server state")
	}

	var state proto.ServerState

	switch scr.Action {
	case proto.ServerControlAction_SERVER_CONTROL_STATUS:
		s.ssMutex.RLock()
		state = s.serverState
		s.ssMutex.RUnlock()
	case proto.ServerControlAction_SERVER_CONTROL_STOP:
		go func() {
			err := s.pauseConduit()
			if err != nil {
				s.log.Errorf("failed to pause conduit server: %v", err)
			}
		}()
		state = proto.ServerState_SERVER_UNKNOWN
	case proto.ServerControlAction_SERVER_CONTROL_DRAIN:
		go func() {
			err := s.drainConduit()
			if err != nil {
				s.log.Errorf("failed to drain conduit server: %v", err)
			}
		}()
		state = proto.ServerState_SERVER_UNKNOWN
	case proto.ServerControlAction_SERVER_CONTROL_START:
		err := s.resumeConduit()
		if err != nil {
			return nil, fmt.Errorf("failed to resume conduit server: %v", err)
		}
		s.ssMutex.RLock()
		state = s.serverState
		s.ssMutex.RUnlock()
	default:
		return nil, fmt.Errorf("unknown action provided: %s", scr.Action)
	}

	return &proto.ServerControlResponse{ServerState: state}, nil
}

func (s *ConduitServer) SchedulerInfo(ctx context.Context, _ *emptypb.Empty) (*proto.SchedulerInfoResponse, error) {
	// get the user from the request
	user, reqPrivLevel, err := s.getUserFromRequest(ctx, nil)
	if err != nil {
		err := fmt.Errorf("error getting user from request: %v", err)
		s.log.Error(err)
		return nil, err
	}

	s.log.Debugf("received schduler info request from user[%v]: %v", user, reqPrivLevel)

	// verfiy that this is a privileged admin account
	if reqPrivLevel != privilegedAdmin {
		return nil, fmt.Errorf("you do not have permission to get scheduler info")
	}

	schedulers := make(map[string]*proto.SchedulerStatus)

	for _, schduler := range s.sched {
		schedulerStatus := &proto.SchedulerStatus{
			Nodes: make(map[string]*proto.NodeStatus),
		}

		nodes := schduler.GetNodeInfo()
		for _, node := range nodes {
			nodeStatus := &proto.NodeStatus{
				Jobs:            make(map[string]*proto.JobInfo),
				AvailableMemory: node.Memory,
			}

			for transferID, jobInfo := range node.Jobs {
				nodeStatus.Jobs[transferID] = jobInfo
			}

			schedulerStatus.Nodes[node.Name] = nodeStatus
		}
		schedulers[schduler.GetSchedulerID().String()] = schedulerStatus
	}

	return &proto.SchedulerInfoResponse{Schedulers: schedulers}, nil
}

func (s *ConduitServer) ErrantPaths(ctx context.Context, epr *proto.ErrantPathsRequest) (*proto.ErrantPathsResponse, error) {
	s.log.Debugf("got request for errant paths")
	// get the user from the request
	requestedUser := epr.GetUser()
	user, reqPrivLevel, err := s.getUserFromRequest(ctx, &requestedUser)
	if err != nil {
		err = fmt.Errorf("error getting user from request: %v", err)
		s.log.Error(err)
		epresp := &proto.ErrantPathsResponse{Paths: make(map[string]*timestamppb.Timestamp)}
		return epresp, err
	}

	s.eMutex.RLock()
	defer s.eMutex.RUnlock()
	errants := make(map[string]*timestamppb.Timestamp)

	// if user is a privileged account without a provided user, assume they want all errant paths in conduit
	if (reqPrivLevel == privilegedAdmin || reqPrivLevel == privilegedService) && user == "" {
		for _, ue := range s.usersErrants {
			for path, tid := range ue {
				errants[path] = tid
			}
		}
	} else {
		// user is not a privileged account so we only return transfers that are for that user
		ue, ok := s.usersErrants[user]
		if ok {
			errants = ue
		}
	}

	epresp := goproto.Clone(&proto.ErrantPathsResponse{Paths: errants}).(*proto.ErrantPathsResponse)

	s.log.Debugf("returning errant paths for user [%v]", user)

	return epresp, nil
}

func (s *ConduitServer) PurgeErrantPath(ctx context.Context, pepr *proto.PurgeErrantPathRequest) (*proto.ErrantPathsResponse, error) {
	// get the user from the request
	requestedUser := pepr.GetUser()
	user, reqPrivLevel, err := s.getUserFromRequest(ctx, &requestedUser)
	if err != nil {
		err = fmt.Errorf("error getting user from request: %v", err)
		s.log.Error(err)
		epresp := &proto.ErrantPathsResponse{Paths: make(map[string]*timestamppb.Timestamp)}
		return epresp, err
	}

	s.eMutex.Lock()
	defer s.eMutex.Unlock()
	errants := make(map[string]*timestamppb.Timestamp)

	// if user is a privileged account they can remove any path they want
	if (reqPrivLevel == privilegedAdmin || reqPrivLevel == privilegedService) && user == "" {
		for _, purgePath := range pepr.GetPaths() {
			for u, ue := range s.usersErrants {
				// if the requested purge path is in this user, set it to PURGE
				if _, ok := ue[purgePath]; ok {
					s.em.RemoveErrant(u, purgePath)
					errants[purgePath] = proto.PurgeValue
				}
			}
		}
	} else {
		// user is not a privileged account so we only purge paths that are for that user
		ue, ok := s.usersErrants[user]
		if ok {
			for _, purgePath := range pepr.GetPaths() {
				// if the requested purge path is in this user, set it to PURGE
				if _, ok := ue[purgePath]; ok {
					s.em.RemoveErrant(user, purgePath)
					errants[purgePath] = proto.PurgeValue
				}
			}
		}
	}

	epresp := goproto.Clone(&proto.ErrantPathsResponse{Paths: errants}).(*proto.ErrantPathsResponse)

	return epresp, nil
}

func (s *ConduitServer) GetCert(ctx context.Context, cr *proto.CertRequest) (*proto.CertResponse, error) {
	// get the user from the request
	requestedUser := cr.GetUser()
	user, reqPrivLevel, err := s.getUserFromRequest(ctx, &requestedUser)
	if err != nil {
		err := fmt.Errorf("error getting user from request: %v", err)
		s.log.Error(err)
		return nil, err
	}

	s.log.Debugf("received cert request from user[%v]: %v", user, reqPrivLevel)

	// verify we have a user
	if (reqPrivLevel == privilegedAdmin || reqPrivLevel == privilegedService) && user == "" {
		return nil, fmt.Errorf("no user was provided")
	}

	expiration := viper.GetDuration(defaults.ConfigRequestedCertLifetime)

	certPEM, err := s.cm.ExternalCertManager.GetClientCreds(user, time.Now().Add(expiration))
	if err != nil {
		err := fmt.Errorf("failed to get client credentials: %v", err)
		s.log.Error(err)
		return nil, err
	}

	return &proto.CertResponse{Cert: certPEM}, nil
}
