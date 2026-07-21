// Copyright 2026. Triad National Security, LLC. All rights reserved.

package internal

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"os/user"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/defaults"
	"github.com/lanl/conduit/internal/etcd"
	"github.com/lanl/conduit/internal/logger"
	"github.com/lanl/conduit/internal/pki"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

/*
The ruuners periodically sends the current status of the nodes to the scheduler.
This information includes what jobs are currently running on the nodes along with the current memory usage.
The runners also:
  - Receives job submissions from the scheduler
  - Executes CONDUIT transfer steps with restrictured user permissions on the fta nodes
  - Reports errors back to the scheduler
*/
type Runner struct {
	proto.UnimplementedConduitRunnerApiServer
	AvailableMemory uint64
	JobsInfoLock    sync.RWMutex
	JobsInfo        map[string]*proto.JobInfo // key: transferID value: map of Slurm Commands
	StreamsInfo     map[uuid.UUID]*StreamInfo
	StreamsLock     sync.RWMutex

	cm  *pki.InternalCertManager
	em  *etcd.ETCDManager
	log *logger.ConduitLogger

	mem_channel chan bool
	job_channel chan bool

	runnerState proto.ServerState
	Shutdown    bool         // shutdown is used to signal that we are trying to shutdown so prevent the api endpoints from responding
	rsMutex     sync.RWMutex // lock for runner state

}

// StreamInfo contains information regarding the channel streams
type StreamInfo struct {
	quitChan chan bool
	stream   *proto.ConduitRunnerApi_GetNodeStatusStreamServer
}

// NewRunner creates a new instance of the runner and starts the runner
func NewRunner(log *logger.ConduitLogger, cert *pki.InternalCertManager, em *etcd.ETCDManager) *Runner {

	run := &Runner{
		AvailableMemory: 0,
		JobsInfo:        make(map[string]*proto.JobInfo),
		StreamsInfo:     make(map[uuid.UUID]*StreamInfo),
		cm:              cert,
		em:              em,
		log:             log,

		// Making channels to send node's memory and job information on
		mem_channel: make(chan bool, 100),
		job_channel: make(chan bool, 100),

		runnerState: proto.ServerState_SERVER_STARTING,
		Shutdown:    false,
	}

	return run
}

// StartRunner creates and get the transport layer security (tls) certificate, sets up the grpc stream and server
func (r *Runner) StartRunner() error {
	// watch for a kill from the operating system
	go r.signalHandler()

	serverCert, err := r.cm.GetServerTLSCert()
	if err != nil {
		r.log.Fatalf("failed to get server cert: %v", err)
	}

	cm := &pki.CertManager{
		InternalCertManager: r.cm,
	}

	certPool, err := cm.GetCertPool(pki.INTERNAL)
	if err != nil {
		r.log.Fatalf("Failed to get cert pool for server cert: %v", err)
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{*serverCert},
		RootCAs:      certPool,
		ClientCAs:    certPool,
		MinVersion:   tls.VersionTLS13,
		MaxVersion:   tls.VersionTLS13,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}

	creds := credentials.NewTLS(tlsConfig)
	opts := []grpc.ServerOption{
		grpc.Creds(creds),
	}

	// Setting up grpc stream to send info to the scheduler
	s := grpc.NewServer(opts...)
	proto.RegisterConduitRunnerApiServer(s, r)

	// Getting addresss, ip, and port number
	ip := viper.GetString(defaults.ConfigServerIPKey)
	port := viper.GetString(defaults.ConfigServerPortKey)

	addr := net.JoinHostPort(ip, port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on address: %v", addr)
	}

	// Start monitoring nodes current memory and job status
	go r.MemoryMonitor(defaults.DefaultRunnerMemMonitorDelay)
	go r.UpdateStreams()

	r.rsMutex.Lock()
	r.runnerState = proto.ServerState_SERVER_RUNNING
	r.rsMutex.Unlock()

	// Runner sends the scheduler a new message if it listened correctly
	r.log.Infof("Listening on address: %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		return fmt.Errorf("failed to serve: %s", err)
	}

	return nil
}

// RunConduitFTA
func (r *Runner) RunConduitFTA(id uuid.UUID, req *proto.JobRequest) {
	var dErr error
	var dpErr proto.Error

	r.log.Infof("Running conduit-fta for Transfer[%s]", id)

	defer func(deferErr *error, deferProtoErr *proto.Error) {
		r.JobsInfoLock.Lock()

		if deferErr != nil && *deferErr != nil {
			*deferProtoErr = proto.Error_ERROR_CONDUIT_INTERNAL
			it := proto.IncompleteTransfer(&proto.TransferDetails{TransferID: id.String()})

			success, _, err := r.em.SafelyAddErr(it, *deferProtoErr, *deferErr)
			if err != nil {
				r.log.Errorf("failed to safely add error to transfer[%v]: %v", it.GetTransferID(), err)
			}
			if !success {
				r.log.Warnf("adding error to transfer was unsuccessful")
			} else {
				r.log.Infof("successfully added error for transfer[%v]: %v", it.GetTransferID(), *deferErr)
			}
		}

		// When job is completed remove from jobs map
		delete(r.JobsInfo[id.String()].GetActions(), int32(req.GetCmd()))

		if len(r.JobsInfo[id.String()].GetActions()) == 0 {
			delete(r.JobsInfo, id.String())
		}

		r.log.Infof("removed transfer %s job %s from job map", id, req.GetCmd())

		r.JobsInfoLock.Unlock()

		r.job_channel <- true
	}(&dErr, &dpErr)

	// Getting the transfer from the etcd
	transfer, _, err := r.em.GetTransfer(id)
	if err != nil {
		dErr = fmt.Errorf("error getting transfer from etcd: %v", err)
		r.log.Error(dErr)
		return
	}

	// Getting the users credentials
	cred, err := getCredentials(transfer.GetUser())
	if err != nil {
		dErr = fmt.Errorf("failed to get user credentials for %v: %v", transfer.GetUser(), err)
		r.log.Error(dErr)
		return
	}

	// Creating the client's certificate
	cert, err := r.cm.CreateSignedClientCert(req.GetTransferID(), time.Now().AddDate(0, 0, 10)) // tls stuff : what we pass to etcd
	if err != nil {
		dErr = fmt.Errorf("error defining cert: %v", err)
		r.log.Error(dErr)
		return
	}

	cmdOptions := viper.GetStringSlice(defaults.ConfigFTAOptionsKey)

	cmdArgs := []string{req.Cmd.String()}
	cmdArgs = append(cmdArgs, cmdOptions...)

	ftaPath := viper.GetString(defaults.ConfigFTAPathKey)

	cmd := exec.Command(ftaPath, cmdArgs...)

	r.log.Debugf("command: %s", strings.Join(append([]string{ftaPath}, cmdArgs...), " "))

	// Appending the command enviornment variable
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Environ(), fmt.Sprintf("SLURM_JOB_NODELIST=%s", strings.Join(req.Nodes, ",")))

	// set extra config defined environment variables
	cmdEnvMap := viper.GetStringMapString(defaults.ConfigFTAEnvKey)
	for k, v := range cmdEnvMap {
		cmd.Env = append(cmd.Environ(), fmt.Sprintf("%s=%s", k, v))
	}

	inBuffer := new(bytes.Buffer)
	outBuffer := new(bytes.Buffer)
	errBuffer := new(bytes.Buffer)

	// writing the certificate to the input buffer
	inBuffer.Write(cert)

	cmd.Stdin = inBuffer
	cmd.Stdout = outBuffer
	cmd.Stderr = errBuffer

	// getting the credentials so we can switch to the new user
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Credential: cred,
	}
	r.log.Infof("running %s for transfer %s with uid: %v gid: %v, groups: %v", req.GetCmd(), transfer.GetTransferID(), cred.Uid, cred.Gid, cred.Groups)

	fErr := cmd.Run()
	if fErr != nil {
		dErr = fmt.Errorf("error running command[%v]: %v %v", strings.Join(append([]string{ftaPath}, cmdArgs...), " "), fErr, errBuffer.String())
		r.log.Error(dErr)
		r.log.Errorf("Command stderr: %v", errBuffer.String())
		r.log.Errorf("Command stdout: %v", outBuffer.String())
		return
	}

	r.log.Debugf("Command stderr: %v", errBuffer.String())
	r.log.Debugf("Command stdout: %v", outBuffer.String())

}

// ETCDWatcher watches the status of the jobs in etcd
func (r *Runner) ETCDWatcher(cmd proto.SchedulerCommand, tid string) {

	it := proto.IncompleteTransfer(&proto.TransferDetails{TransferID: tid})
	it.ETCDStateKey()

	var completedState proto.TransferState

	// updating the job's current status
	switch cmd {
	case proto.SchedulerCommand_VALIDATION:
		completedState = proto.TransferState_TRANSFER_VALIDATION_COMPLETE

	case proto.SchedulerCommand_SETUP:
		completedState = proto.TransferState_TRANSFER_SETUP_COMPLETE

	case proto.SchedulerCommand_TEARDOWN:
		completedState = proto.TransferState_TRANSFER_TEARDOWN_COMPLETE

	case proto.SchedulerCommand_TRANSFER:
		completedState = proto.TransferState_TRANSFER_DATA_COMPLETE

	default:
		r.log.Error("Completed state of command was not reached")
	}

	// watching the job's status in etcd
	watch, cancel := r.em.GetWatchChannel(it.ETCDStateKey())
	defer cancel()

	for resp := range watch {

		for _, ev := range resp.Events {

			s, ok := proto.TransferState_value[string(ev.Kv.Value)]
			if !ok {
				continue
			}

			// when the job has been completed delete it from the jobs info map
			if completedState == proto.TransferState(s) || proto.TransferState_TRANSFER_ERROR == proto.TransferState(s) || proto.TransferState_TRANSFER_ABORT == proto.TransferState(s) || proto.TransferState_TRANSFER_ABORTED == proto.TransferState(s) {

				r.JobsInfoLock.Lock()

				// When job is completed remove from jobs map
				delete(r.JobsInfo[tid].GetActions(), int32(cmd))

				if len(r.JobsInfo[tid].GetActions()) == 0 {
					delete(r.JobsInfo, tid)
				}

				r.log.Infof("removed allocated transfer %s job %s from job map", tid, cmd)

				r.JobsInfoLock.Unlock()

				r.job_channel <- true

				return
			}
		}
	}
}

// MemoryMonitor monitors the node's memory and sends it across the channel to the schedulers
func (r *Runner) MemoryMonitor(sleepDuration time.Duration) {
	for {
		r.rsMutex.RLock()
		shutdown := r.Shutdown
		r.rsMutex.RUnlock()

		var availableMemory uint64

		if shutdown {
			availableMemory = 0
		} else {
			vm, err := mem.VirtualMemory()
			if err != nil {
				r.log.Errorf("Failed to retrieve virtual memory: %v", err)
			} else {
				availableMemory = vm.Available
			}
		}

		r.AvailableMemory = availableMemory

		// tell runner to send memory update to any clients
		r.mem_channel <- true

		<-time.After(sleepDuration)
	}
}

// UpdateStreams monitors the number of current running jobs on the nodes along with the nodes' current memory usage and streams that information to scheduler
func (r *Runner) UpdateStreams() {

	for {

		select {
		// Sending the updated memory to the memory channel
		case <-r.mem_channel:

		// Sending the updated job to the job channel
		case <-r.job_channel:

		}

		r.JobsInfoLock.RLock()

		currentJobs := r.getCurrentJobs()

		r.JobsInfoLock.RUnlock()

		status := &proto.NodeStatus{
			Jobs:            currentJobs,
			AvailableMemory: r.AvailableMemory,
		}

		r.StreamsLock.Lock()
		for sid, si := range r.StreamsInfo {

			stream := *si.stream

			err := stream.Send(status)
			if err != nil {
				r.log.Errorf("Failed to send node status to conduit server: %v", err)
				if errors.Is(err, io.EOF) {
					r.log.Errorf("Stream[%v] closed by conduit server. Removing from streams map", sid)
					delete(r.StreamsInfo, sid)
					r.log.Debugf("Current streams map: %+v", r.StreamsInfo)
				}
			}
		}

		r.StreamsLock.Unlock()
	}
}

// signalHandler will watch for unix signals to shutdown gracefully
func (r *Runner) signalHandler() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGHUP)

	sig := <-sigs
	r.log.Errorf("received unix signal: [%v]", sig)

	r.rsMutex.Lock()
	r.Shutdown = true
	r.rsMutex.Unlock()

	// tell clients this runner has no memory available
	r.AvailableMemory = 0
	r.mem_channel <- true

	// wait for all running jobs to complete
	jobsStopped := false
	jobCount := 0
	for !jobsStopped {
		r.JobsInfoLock.RLock()
		numJobs := len(r.JobsInfo)
		r.JobsInfoLock.RUnlock()

		if numJobs == 0 {
			jobsStopped = true
		}

		if !jobsStopped && jobCount != numJobs {
			r.log.Infof("waiting for %v jobs to complete", numJobs)
			jobCount = numJobs
		}
		if !jobsStopped {
			time.Sleep(1 * time.Second)
		}
	}

	r.log.Info("all running jobs have finished")

	r.log.Infof("shutting down")
	os.Exit(0)
}

func getCredentials(username string) (*syscall.Credential, error) {
	u, err := user.Lookup(username)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup user[%v]: %v", username, err)
	}

	uid64, err := strconv.ParseUint(u.Uid, 10, 32)
	if err != nil {
		return nil, fmt.Errorf("failed to parse uid %q: %w", u.Uid, err)
	}

	gid64, err := strconv.ParseUint(u.Gid, 10, 32)
	if err != nil {
		return nil, fmt.Errorf("failed to parse primary gid %q: %w", u.Gid, err)
	}

	groupIDs, err := u.GroupIds()
	if err != nil {
		return nil, err
	}

	groups := make([]uint32, 0, len(groupIDs))
	seen := map[uint32]bool{}

	for _, s := range groupIDs {
		g64, err := strconv.ParseUint(s, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("parse supplementary gid %q: %w", s, err)
		}

		g := uint32(g64)

		// Optional: don't duplicate the primary gid in supplementary groups.
		if g == uint32(gid64) || seen[g] {
			continue
		}

		groups = append(groups, g)
		seen[g] = true
	}

	return &syscall.Credential{
		Uid:    uint32(uid64),
		Gid:    uint32(gid64),
		Groups: groups,
	}, nil
}
