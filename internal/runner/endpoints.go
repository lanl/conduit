// Copyright 2026. Triad National Security, LLC. All rights reserved.

package internal

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	proto "github.com/lanl/conduit/api"
	"google.golang.org/grpc/peer"
	goproto "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

// GetNodeStatusStream responds with the list of all jobs running on nodes
func (r *Runner) GetNodeStatusStream(_ *emptypb.Empty, stream proto.ConduitRunnerApi_GetNodeStatusStreamServer) error {

	streaminfo := &StreamInfo{
		quitChan: make(chan bool),
		stream:   &stream,
	}

	client := ""

	if p, ok := peer.FromContext(stream.Context()); ok && p.Addr != nil {
		_, _, _, _, cn, err := clientCertInfo(stream.Context())
		if err != nil {
			r.log.Errorf("failed to get client cert info: %v", err)
		}

		client = fmt.Sprintf("%s %s", p.Addr.String(), cn)
	}

	r.log.Infof("adding new stream from conduit server: %v", client)

	r.StreamsLock.Lock()

	id := uuid.New()
	// Adding stream to StreamsInfo map
	r.StreamsInfo[id] = streaminfo

	r.StreamsLock.Unlock()

	select {
	case <-stream.Context().Done():
		// client closed the stream, delete from map
		r.log.Infof("conduit server closed stream[%v]. Removing from stream map: %v", id, client)
	case <-streaminfo.quitChan:
		// somewhere in the runner closed the stream
		r.log.Infof("conduit runner is closing stream[%v] for conduit server: %v", id, client)
	}

	r.StreamsLock.Lock()
	delete(r.StreamsInfo, id)
	r.StreamsLock.Unlock()

	r.log.Infof("successfully removed stream[%v]", id)

	return nil
}

// GetNodeStatus returns the status of the nodes which includes the jobs running on the nodes and the nodes' avaliable memory
func (r *Runner) GetNodeStatus(context.Context, *emptypb.Empty) (*proto.NodeStatus, error) {

	r.JobsInfoLock.RLock()

	currentJobs := r.getCurrentJobs()

	r.JobsInfoLock.RUnlock()

	status := &proto.NodeStatus{
		Jobs:            currentJobs,
		AvailableMemory: r.AvailableMemory,
		JobsVersion:     r.JobsVersion,
	}

	return status, nil
}

// SubmitFTAJob submits the job to the runner
// will only run the requested job if the provided existing jobs match the runners current jobs
// will only return the runners current jobs, not the available memory
func (r *Runner) SubmitFTAJob(ctx context.Context, req *proto.JobRequest) (*proto.NodeStatus, error) {

	// Getting the transferID of the job
	id, err := uuid.Parse(req.GetTransferID())
	if err != nil {
		tErr := fmt.Errorf("error getting transferID [%v]: %v", req.GetTransferID(), err)
		r.log.Error(tErr)
		return nil, tErr
	}

	r.log.Debugf("request for %s transfer[%s]", req.GetCmd(), req.GetTransferID())

	r.JobsInfoLock.Lock()
	defer r.JobsInfoLock.Unlock()

	// check that provided jobinfo is mostly correct
	// we allow for the provided jobinfo to contain extra jobs, just so long as it isn't missing any jobs we are running
	// also only do this if this is a HEAD allocation
	if req.GetType() == proto.JobType_HEAD {
		providedJobs := req.GetExistingJobs()
		for tid, ji := range r.JobsInfo {
			if pji, ok := providedJobs[tid]; ok {
				for cmd, _ := range ji.GetActions() {
					if _, ok := pji.GetActions()[cmd]; !ok {
						return &proto.NodeStatus{
							Jobs:            r.getCurrentJobs(),
							AvailableMemory: r.AvailableMemory,
							JobsVersion:     r.JobsVersion,
						}, nil
					}
				}
			} else {
				return &proto.NodeStatus{
					Jobs:            r.getCurrentJobs(),
					AvailableMemory: r.AvailableMemory,
					JobsVersion:     r.JobsVersion,
				}, nil
			}
		}
	}

	// Adding scheduler command to Job_Info map
	if _, ok := r.JobsInfo[id.String()]; !ok {
		r.JobsInfo[id.String()] = &proto.JobInfo{
			Actions: make(map[int32]bool),
		}
	}

	r.JobsInfo[id.String()].GetActions()[int32(req.GetCmd())] = true
	r.JobsVersion++

	if req.Type == proto.JobType_ALLOCATE {
		r.log.Infof("allocate %s job for transfer %s", req.GetTransferID(), req.GetCmd())

		// Watching etcd for new commands
		go r.ETCDWatcher(req.GetCmd(), id.String())
	} else {
		// run the requested FTA job
		go r.RunConduitFTA(id, req)
	}

	// Sending the status of the job over the channel
	r.job_channel <- true

	return &proto.NodeStatus{
		Jobs:            r.getCurrentJobs(),
		AvailableMemory: r.AvailableMemory,
		JobsVersion:     r.JobsVersion,
	}, nil
}

// assumes a lock is already held
func (r *Runner) getCurrentJobs() map[string]*proto.JobInfo {
	currentJobs := make(map[string]*proto.JobInfo)

	for tid, ji := range r.JobsInfo {
		currentJobs[tid] = goproto.Clone(ji).(*proto.JobInfo)
	}

	return currentJobs
}
