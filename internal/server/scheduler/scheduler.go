// Copyright 2026. Triad National Security, LLC. All rights reserved.

package scheduler

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"maps"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/defaults"
	"github.com/lanl/conduit/internal/etcd"
	"github.com/lanl/conduit/internal/logger"
	cert "github.com/lanl/conduit/internal/pki"
	schedutil "github.com/lanl/conduit/internal/server/scheduler/util"
	util "github.com/lanl/conduit/util"
	"github.com/spf13/viper"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	gcredentials "google.golang.org/grpc/credentials"
	goproto "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

/*
The CONDUIT scheduler reads jobs that come in from etcd and adds those jobs to the priority queue.
The scheduler checks if the highest priority job can be run with the available nodes.
If the job can be run, then the scheduler begins running them on the available nodes. If not
then the scheduler waits for an updated response from the nodes.
*/
type Scheduler struct {
	priorityQueue *PriorityQueue
	PriorityLock  sync.RWMutex
	nodeInfoLock  sync.RWMutex
	nodeInfo      map[string]*NodeInfo               // continually updated map with information on node memory and job count [key: node name]
	nodesConfig   map[string]*schedutil.NViperConfig // configuration information for each node [key: node name]
	id            uuid.UUID                          // unique identifier for the scheduler, primarily used for logging

	em  *etcd.ETCDManager
	cm  *cert.CertManager
	log *logger.ConduitLogger

	sMutex           sync.RWMutex
	state            proto.ServerState
	stopJobWatchChan chan bool

	activeJobs map[uuid.UUID]bool // the jobs map is only used for stopping conduit and keeps track of the events that the scheduler is actively handling
	jMutex     sync.RWMutex       // lock for active jobs map
}

// NodeInfo contains information regarding a node
type NodeInfo struct {
	Jobs   map[string]*proto.JobInfo    // current running jobs on the node [key: transferID]
	Memory uint64                       // current available memory(MB) on the node
	Name   string                       // name of node
	client proto.ConduitRunnerApiClient // API client that allows for streams for status updates
}

func (n *NodeInfo) Clone() *NodeInfo {
	return &NodeInfo{
		Jobs:   maps.Clone(n.Jobs),
		Memory: n.Memory,
		Name:   n.Name,
		client: n.client,
	}
}

// NewScheduler creates a new instance of a scheduler and starts the scheduler
func NewScheduler(log *logger.ConduitLogger, cm *cert.CertManager, em *etcd.ETCDManager) (*Scheduler, error) {

	id := uuid.New()
	l := logger.NewConduitLogger(log.GetLevel(), fmt.Sprintf("%sscheduler[%s]:", log.GetPrefix(), id))
	if log.GetPrefix() == "" {
		l = logger.NewConduitLogger(log.GetLevel(), fmt.Sprintf("scheduler[%s]:", id))
	}

	// initializing priority queue
	pq := make(PriorityQueue, 0)

	// initializing scheduler
	SC := &Scheduler{
		log:           l,
		cm:            cm,
		em:            em,
		priorityQueue: &pq,
		nodeInfo:      make(map[string]*NodeInfo),
		id:            id,
		activeJobs:    make(map[uuid.UUID]bool),
	}

	// adding nodes to scheduler's map from the CONDUIT config file
	nodes, err := schedutil.GetNodeConfigsFromViper()
	if err != nil {
		return nil, fmt.Errorf("failed to get nodes config from viper: %v", err)
	}

	SC.nodesConfig = nodes.Nodes

	return SC, nil
}

// startScheduler connects to client and begins monitoring node acitivity
func (s *Scheduler) StartScheduler() error {
	// creating grpc clients for each node
	for name, n := range s.nodesConfig {

		// initialing the internal certification manager to enable transport layer security (TLS)
		// protocol for communication between scheduler and runners
		certPool, err := s.cm.GetCertPool(cert.INTERNAL)
		if err != nil {
			return fmt.Errorf("failed to get cert pool: %v", err)
		}

		cert, err := s.cm.InternalCertManager.CreateSchedulerClientCert(time.Now().AddDate(10, 0, 0), s.GetSchedulerID())
		if err != nil {
			return fmt.Errorf("failed to create client certificate: %v", err)
		}

		tlsConfig := &tls.Config{
			RootCAs:      certPool,
			MinVersion:   tls.VersionTLS13,
			MaxVersion:   tls.VersionTLS13,
			Certificates: []tls.Certificate{*cert},
		}

		creds := gcredentials.NewTLS(tlsConfig)

		opts := []grpc.DialOption{
			grpc.WithTransportCredentials(creds),
		}

		addr := net.JoinHostPort(n.Address, strconv.Itoa(n.Port))

		con, err := grpc.NewClient(addr, opts...)
		if err != nil {
			s.log.Errorf("Error: %v", err)
			return fmt.Errorf("failed to create grpc client for node[%v]:%v", name, err)
		}
		client := proto.NewConduitRunnerApiClient(con)
		s.log.Debugf("Successfully dialed to: %v", addr)

		// retrieving information about the nodes so the scheduler can connect to the client
		node := new(NodeInfo)
		node.client = client
		node.Name = name
		node.Jobs = make(map[string]*proto.JobInfo)

		// adding the node to the scheduler's nodeInfo map
		s.nodeInfo[name] = node
	}
	s.log.Debugf("Nodes: %+v", s.nodeInfo)

	// start monitoring all nodes
	s.nodeMonitor()

	// start monitoring incoming jobs in etcd
	successChan := make(chan bool)
	stopChan := make(chan bool)
	go s.jobMonitor(successChan, stopChan)
	<-successChan

	// get existing jobs
	s.addExistingJobsToQueue()

	s.stopJobWatchChan = stopChan

	s.log.Infof("Started!")

	s.sMutex.Lock()
	s.state = proto.ServerState_SERVER_RUNNING
	s.sMutex.Unlock()

	return nil
}

func (s *Scheduler) StopScheduler() error {
	// check that the transfer worker is in a running state
	s.sMutex.Lock()
	state := s.state

	if state == proto.ServerState_SERVER_RUNNING {
		s.state = proto.ServerState_SERVER_STOPPING
	} else {
		s.sMutex.Unlock()
		return fmt.Errorf("could not stop scheduler[%v] because it is not in the running state: %v", s.id, state)
	}
	s.sMutex.Unlock()

	s.log.Info("stopping scheduler")

	// stop watching jobs from etcd
	s.stopJobWatchChan <- true

	// dump out the priority queue
	s.PriorityLock.Lock()
	s.log.Debugf("dumping out priority queue of %v jobs", s.priorityQueue.Len())
	pq := make(PriorityQueue, 0)
	s.priorityQueue = &pq
	s.PriorityLock.Unlock()

	// check to see if all the jobs are stopped
	jobsStopped := false
	jobCount := 0
	for !jobsStopped {
		s.PriorityLock.RLock()
		numJobs := s.priorityQueue.Len()
		s.PriorityLock.RUnlock()

		s.jMutex.RLock()
		numJobs += len(s.activeJobs)
		s.jMutex.RUnlock()

		if numJobs == 0 {
			jobsStopped = true
		}

		if !jobsStopped && jobCount != numJobs {
			s.log.Debugf("waiting for %v jobs to complete", numJobs)
			jobCount = numJobs
		}
		if !jobsStopped {
			time.Sleep(100 * time.Millisecond)
		}
	}

	s.log.Info("all scheduler jobs are complete")

	s.sMutex.Lock()
	s.state = proto.ServerState_SERVER_STOPPED
	s.sMutex.Unlock()

	return nil
}

func (s *Scheduler) RemoveTransfer(id uuid.UUID) error {
	s.PriorityLock.Lock()
	newQueue := s.priorityQueue.RemoveJob(id)
	s.priorityQueue = newQueue
	s.PriorityLock.Unlock()

	return nil
}

// jobMonitor watches for job updates from etcd
func (s *Scheduler) jobMonitor(successChan chan bool, stopChan chan bool) {
	// get all jobs from etcd starting with revision 0
	// this works because the job will not be executed unless it exists in etcd.
	watch, wcancel := s.em.GetWatchChannelPrefix(proto.JobsPrefix, 0)
	defer wcancel()

	s.log.Debugf("Listening for jobs %v", proto.JobsPrefix)

	monitorID := uuid.New()
	s.jMutex.Lock()
	s.activeJobs[monitorID] = true
	s.jMutex.Unlock()

	defer s.removeActiveJob(monitorID)

	successChan <- true

	for {
		select {
		case wresp, ok := <-watch:
			if !ok {
				s.log.Errorf("jobs watch channel closed unexpectedly")
				return
			}
			s.handleJobEvent(wresp.Events)
			if wresp.Canceled {
				s.log.Errorf("received cancel message from watch stream: %+v", wresp)
			}
		case <-stopChan:
			s.log.Infof("stopped watching job events")
			return
		}
	}
}

func (s *Scheduler) handleJobEvent(events []*clientv3.Event) {
	for _, event := range events {
		if event.Type == clientv3.EventTypePut {

			s.log.Debugf("Event's key and value: %q : %q", event.Kv.Key, event.Kv.Value)

			uid, err := ETCDKeytoUUID(string(event.Kv.Key))
			if err != nil {
				s.log.Errorf("Error when converting etcd key to uuid string: %v", err)
				continue
			}

			// getting job's priority from etcd
			it := proto.IncompleteTransfer(&proto.TransferDetails{TransferID: uid.String()})
			priority, err := s.em.GetPriority(it)
			if err != nil {
				s.log.Errorf("transfer %s has issues getting priority from etcd: %v", uid, err)
				continue
			}

			// getting transfer's created time from etcd
			createdTime, err := s.em.GetCreatedTime(it)
			if err != nil {
				s.log.Errorf("transfer %s has issues getting created time from etcd: %v", uid, err)
				continue
			}

			// converting the scheduler command action from bytes to a string
			b := event.Kv.Value
			action := string(b)

			// converts teh string recieved from etcd and returns the corresponding scheduler command
			sci, err := ETCDValuestoSchedulerCommand(action)
			if err != nil {
				s.log.Errorf("failed to get scheduler command from etcd")
				continue
			}

			s.log.Debugf("transfer %s recieved scheduler command: %v", uid, sci)

			s.PriorityLock.Lock()

			// adding the job's uuid, job status, priority, and the time it was created
			// to the scheduler's priority queue
			s.priorityQueue.AddJob(uid, sci, createdTime.AsTime(), priority, s.em)

			s.PriorityLock.Unlock()

			s.log.Debugf("Transfer[%v] was added to priority queue", uid)

			// triggering jobRequest to decide which nodes, based on avaliability,
			// will run the job that has just been sent
			go s.jobRequest()
		}
	}
}

// jobRequest grabs the top job from the priority queue and sends it to the most available runner(s)
func (s *Scheduler) jobRequest() {
	// add job for shutdown purposes
	eventID := uuid.New()
	s.jMutex.Lock()
	s.activeJobs[eventID] = true
	s.jMutex.Unlock()

	// remove active job when done
	defer s.removeActiveJob(eventID)

	s.PriorityLock.Lock()

	if len(*s.priorityQueue) == 0 {
		s.PriorityLock.Unlock()
		return
	}

	// removing top job in the priority queue so we can send it to the runner(s)
	top, tErr := s.priorityQueue.PopJob()
	if tErr != nil {
		s.log.Errorf("failed to remove job[%v][%v] from top of the priority queue: %v", top.JobID, top.SchedulerCommand, tErr)
		s.PriorityLock.Unlock()
		return
	}

	it := proto.IncompleteTransfer(&proto.TransferDetails{TransferID: top.JobID.String()})

	// update the expiry for the transfer
	expiryStop := make(chan bool)
	go s.em.UpdateExpiryConstantly(it, expiryStop)
	defer func() { expiryStop <- true }()

	s.PriorityLock.Unlock()

	jobReq := &proto.JobRequest{TransferID: top.JobID.String(), Cmd: top.SchedulerCommand}

	availableNodes := []*NodeInfo{}

	s.nodeInfoLock.Lock()

	defer s.nodeInfoLock.Unlock()

	// getting the status of available nodes
	for nn, ns := range s.nodeInfo {

		// s.nodeInfoLock.Lock()

		// check to see if the job is a validation job
		// if it is then run only check ns.memory
		// and run availableNodes = append(availableNodes, ns)

		// reading in minMemory values
		nodesMinMemory, err := util.ProcessBytes(s.nodesConfig[ns.Name].MinMemory)
		if err != nil {
			s.log.Errorf("transfer[%s] error converting minMemory config to bytes: %v", top.JobID, err)
			return
		}

		s.log.Debugf("%v nodesMinMemory[%v], node currently has: %v", nn, nodesMinMemory, ns.Memory)
		s.log.Debugf("%v nodesmaxjobs[%v], node currently has: %v", nn, s.nodesConfig[ns.Name].MaxJobs, len(ns.Jobs))

		if top.SchedulerCommand == proto.SchedulerCommand_VALIDATION && ns.Memory > uint64(nodesMinMemory) {
			availableNodes = append(availableNodes, ns)

		} else if ns.Memory > uint64(nodesMinMemory) && len(ns.Jobs) < s.nodesConfig[ns.Name].MaxJobs {

			availableNodes = append(availableNodes, ns)
		}
	}

	availableNodes, err := sortNodes(availableNodes)
	if err != nil {
		s.log.Errorf("transfer[%s] unable to sort available nodes: %v", top.JobID, err)
		return
	}
	requiredNumNodes := 0

	switch top.SchedulerCommand {
	case proto.SchedulerCommand_VALIDATION:
		requiredNumNodes = viper.GetInt(defaults.ConfigNodeAllocationsValidationNodesKey)
	case proto.SchedulerCommand_SETUP:
		requiredNumNodes = viper.GetInt(defaults.ConfigNodeAllocationsSetupNodesKey)
	case proto.SchedulerCommand_TEARDOWN:
		requiredNumNodes = viper.GetInt(defaults.ConfigNodeAllocationsTeardownNodesKey)
	case proto.SchedulerCommand_TRANSFER:
		requiredNumNodes = viper.GetInt(defaults.ConfigNodeAllocationsTransferNodesKey)
	default:
		s.log.Errorf("transfer[%s] could not find scheduler command: %v", top.JobID, top.SchedulerCommand)
		return
	}

	if len(availableNodes) >= requiredNumNodes {

		if requiredNumNodes == 0 {
			s.log.Warnf("transfer's [%s] scheduler command [%s] requires 0 nodes", top.JobID.String(), top.SchedulerCommand.String())
		}

		// get only the nodes we're going to use for this job
		nodes := []*NodeInfo{}
		nodeNames := []string{}
		for i := 0; i < requiredNumNodes; i++ {
			nodes = append(nodes, availableNodes[i])
			nodeNames = append(nodeNames, availableNodes[i].Name)
		}

		jobReq.Nodes = nodeNames

		// check the value of this transfers job key to make sure it exists and has a value that we expect
		compares := []clientv3.Cmp{
			clientv3.Compare(clientv3.CreateRevision(string(it.ETCDJobsKey())), ">", 0),
			clientv3.Compare(clientv3.Value(it.ETCDJobsKey()), "=", jobReq.Cmd.String()),
		}

		// delete job from etcd
		actions := []clientv3.Op{
			clientv3.OpDelete(string(it.ETCDJobsKey())),
		}

		res, err := s.em.RetryTxn(&compares, &actions, defaults.MaxRetries, defaults.RetryDelay)
		if err != nil {
			s.log.Errorf("failed to delete the transfer's %s job key [%s] [%s] from etcd: %v", top.JobID, it.ETCDJobsKey(), top.SchedulerCommand, err)
			return
		} else if !res.Succeeded {
			s.log.Warnf("the deletion of transfer's [%s] job key [%s] [%s] from etcd was unsuccessful. Another scheduler probably took care of it", top.JobID, it.ETCDJobsKey(), top.SchedulerCommand)
			return
		}

		s.log.Debugf("deleted job from etcd for transfer[%v]", it.GetTransferID())

		for i, n := range nodes {

			// Assigning node type
			if i == 0 {
				jobReq.Type = proto.JobType_HEAD
			} else {
				jobReq.Type = proto.JobType_ALLOCATE
			}

			// add what we think the node has for current jobs
			existingJobs := make(map[string]*proto.JobInfo)
			for tid, ji := range n.Jobs {
				existingJobs[tid] = goproto.Clone(ji).(*proto.JobInfo)
			}
			jobReq.ExistingJobs = existingJobs

			s.log.Debugf("transfer [%s] is sending %s job [%s] to runner [%v]", top.JobID.String(), jobReq.GetType(), top.SchedulerCommand.String(), n.Name)

			c, cancel := context.WithTimeout(context.Background(), defaults.DefaultRunnerTimeout)

			resp, submitErr := n.client.SubmitFTAJob(c, jobReq, grpc.WaitForReady(true))
			if submitErr != nil {
				s.log.Errorf("failed to submit job [%s] command [%s]: %v", top.SchedulerCommand.String(), top.JobID.String(), submitErr)
				cancel()

				succ, _, err := s.em.SafelyAddErr(it, proto.Error_ERROR_NETWORK, fmt.Errorf("failed to submit transfer to fta [%v]: %v", top.SchedulerCommand, submitErr))
				if !succ {
					s.log.Errorf("transfer[%s] failed to add error[%s] to etcd: transfer was already in error state", top.JobID.String())
				} else if err != nil {
					s.log.Errorf("transfer[%s] failed to add error[%s] to etcd: transfer was already in error state", top.JobID.String(), err)
				}
			}
			cancel()

			accepted := true

			// check if our requested job actually was accepted by the node
			if _, ok := resp.GetJobs()[top.JobID.String()]; !ok {
				// transfer id is not on node
				s.log.Errorf("transfer [%s] scheduler command [%s] was not accepted by runner [%s]", top.JobID.String(), top.SchedulerCommand.String(), n.Name)
				accepted = false
			} else if _, ok := resp.GetJobs()[top.JobID.String()].GetActions()[int32(top.SchedulerCommand)]; !ok {
				// transfer id is on node but not this scheduler command
				s.log.Errorf("transfer [%s] scheduler command [%s] was not accepted by runner [%s]", top.JobID.String(), top.SchedulerCommand.String(), n.Name)
				accepted = false
			}

			if !accepted {
				// put the job back into etcd
				// check to make sure there are no jobs already in etcd for this transfer
				compares := []clientv3.Cmp{
					clientv3.Compare(clientv3.CreateRevision(string(it.ETCDJobsKey())), "=", int64(0)),
					clientv3.Compare(clientv3.Value(it.ETCDErrorKey()), "=", proto.Error_ERROR_NONE.String()),
				}

				// add job back to etcd
				actions := []clientv3.Op{
					clientv3.OpPut(string(it.ETCDJobsKey()), top.SchedulerCommand.String()),
				}

				res, err := s.em.RetryTxn(&compares, &actions, defaults.MaxRetries, defaults.RetryDelay)
				if err != nil {
					s.log.Errorf("failed to re-add the transfer's %s job key [%s] [%s] to etcd: %v", top.JobID, it.ETCDJobsKey(), top.SchedulerCommand, err)
				} else if !res.Succeeded {
					s.log.Warnf("the re-addition of transfer's [%s] job key [%s] [%s] from etcd was unsuccessful. it is probably in error state", top.JobID, it.ETCDJobsKey(), top.SchedulerCommand)
				} else {
					s.log.Debugf("successfully re-added transfer[%s] job key [%s] to etcd", it.GetTransferID(), top.SchedulerCommand)
				}

			}

			// update the nodes current jobs with the value from the response
			newNI := s.nodeInfo[n.Name].Clone()
			newNI.Jobs = resp.GetJobs()
			s.nodeInfo[n.Name] = newNI

			// do not go on to the other nodes
			if !accepted {
				break
			}

		}

	} else {
		// If there are not enough available nodes for the job we wait until more nodes become available
		// When more nodes do become available, we add these jobs to the priority queue
		s.PriorityLock.Lock()

		s.priorityQueue.AddJob(top.JobID, top.SchedulerCommand, top.CreatedTime, top.Priority, s.em)

		s.PriorityLock.Unlock()

		switch top.SchedulerCommand {
		case proto.SchedulerCommand_VALIDATION:
			s.log.Warnf("transfer [%s] scheduler command [%s] Not enough nodes available to run this job. Number of nodes needed: %v memory required for this job %v", top.JobID.String(), top.SchedulerCommand.String(), viper.GetInt(defaults.ConfigNodeAllocationsValidationNodesKey), viper.GetInt(defaults.ConfigNodeAllocationsValidationMemoryKey))

		case proto.SchedulerCommand_SETUP:
			s.log.Warnf("transfer [%s] scheduler command [%s] Not enough nodes available to run this job. Number of nodes needed: %v memory required for this job %v", top.JobID.String(), top.SchedulerCommand.String(), viper.GetInt(defaults.ConfigNodeAllocationsSetupNodesKey), viper.GetInt(defaults.ConfigNodeAllocationsSetupMemoryKey))

		case proto.SchedulerCommand_TRANSFER:
			s.log.Warnf("transfer [%s] scheduler command [%s] Not enough nodes available to run this job. Number of nodes needed: %v memory required for this job %v", top.JobID.String(), top.SchedulerCommand.String(), viper.GetInt(defaults.ConfigNodeAllocationsTransferNodesKey), viper.GetInt(defaults.ConfigNodeAllocationsTransferMemoryKey))

		case proto.SchedulerCommand_TEARDOWN:
			s.log.Warnf("transfer [%s] scheduler command [%s] Not enough nodes available to run this job. Number of nodes needed: %v memory required for this job %v", top.JobID.String(), top.SchedulerCommand.String(), viper.GetInt(defaults.ConfigNodeAllocationsTeardownNodesKey), viper.GetInt(defaults.ConfigNodeAllocationsTeardownMemoryKey))
		}

		return
	}
}

// nodeMonitor monitors the nodes in the map
func (s *Scheduler) nodeMonitor() {

	s.nodeInfoLock.RLock()
	defer s.nodeInfoLock.RUnlock()

	for i, n := range s.nodeInfo {

		go func(ni *NodeInfo, nodeName string) {

			for {
				err := s.connectToNode(ni, nodeName)
				if err != nil {
					s.log.Errorf("failed to connect to node[%v]: %v", ni.Name, err)
				}
				time.Sleep(5 * time.Second)
			}
		}(n, i)
	}
}

func (s *Scheduler) connectToNode(ni *NodeInfo, nodeName string) error {
	// Base context for the RPC
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.nodeInfoLock.RLock()
	// listens for updates on stream regarding the current status of the nodes
	stream, err := ni.client.GetNodeStatusStream(ctx, &emptypb.Empty{}, grpc.WaitForReady(true))
	s.nodeInfoLock.RUnlock()

	if err != nil {
		return fmt.Errorf("failed to get node status stream: %v", err)
	}

	// Channels to get messages / errors from a single Recv goroutine.
	msgCh := make(chan *proto.NodeStatus)
	errCh := make(chan error, 1)

	// Goroutine that blocks on Recv and forwards results into channels.
	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				errCh <- err
				return
			}
			msgCh <- msg
		}
	}()

	// 1-minute inactivity timer
	const idleTimeout = time.Minute
	timer := time.NewTimer(idleTimeout)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			// Context cancelled (could be from outside or our own cancel)
			s.log.Error("node[%v] stream context cancelled", ni.Name)

			// error from the node stream,set available memory to zero, wait 5 seconds, then try reconnecting
			s.nodeInfoLock.Lock()
			s.nodeInfo[nodeName].Memory = 0
			s.nodeInfoLock.Unlock()

			return ctx.Err()

		case err := <-errCh:
			// Error or EOF from Recv
			if err == io.EOF {
				s.log.Error("node[%v] stream closed by server", ni.Name)
			} else {
				s.log.Errorf("Received error from node[%v] stream: %v", ni.Name, err)
			}

			// error from the node stream,set available memory to zero, wait 5 seconds, then try reconnecting
			s.nodeInfoLock.Lock()
			s.nodeInfo[nodeName].Memory = 0
			s.nodeInfoLock.Unlock()

			return fmt.Errorf("recieved error from node[%v] stream: %v", ni.Name, err)

		case res := <-msgCh:
			// Safely reset timer:
			if !timer.Stop() {
				// Drain timer channel if it already fired
				select {
				case <-timer.C:
				default:
				}
			}

			finalJobs := res.GetJobs()

			s.nodeInfoLock.Lock()

			if len(s.nodeInfo[nodeName].Jobs) != len(finalJobs) {
				s.log.Debugf("Updated job info for node[%s]: %+v vs %+v", nodeName, finalJobs, s.nodeInfo[nodeName].Jobs)
			}

			// getting the final jobs and available memory from the nodes
			s.nodeInfo[nodeName].Jobs = finalJobs
			s.nodeInfo[nodeName].Memory = res.GetAvailableMemory()

			s.nodeInfoLock.Unlock()

			go s.jobRequest()

			timer.Reset(idleTimeout)

		case <-timer.C:
			// No messages for idleTimeout
			cancel() // stop the Recv goroutine by cancelling the RPC
			return fmt.Errorf("no messages received for %s", idleTimeout)
		}
	}
}

// sortNodes sorts the memory of the nodes in order of nodes with most memory
func sortNodes(availableNodes []*NodeInfo) ([]*NodeInfo, error) {
	sort.Slice(availableNodes, func(i, j int) bool {

		if len(availableNodes[i].Jobs) == len(availableNodes[j].Jobs) {
			return availableNodes[i].Memory > availableNodes[j].Memory

		} else {
			return len(availableNodes[i].Jobs) < len(availableNodes[j].Jobs)
		}
	})

	return availableNodes, nil
}

// ETCDKeytoUUID assigns jobs in etcd as a uuid
func ETCDKeytoUUID(etcdKey string) (uuid.UUID, error) {

	uidString := strings.TrimPrefix(etcdKey, proto.JobsPrefix)

	u, err := uuid.Parse(uidString)
	if err != nil {
		return u, fmt.Errorf("error when parsing uuid string[%v]: %v", uidString, err)
	}

	return u, nil
}

// ETCDValuestoSchedulerCommand extracts the status of jobs (value) in the scheduler's command map
func ETCDValuestoSchedulerCommand(action string) (proto.SchedulerCommand, error) {

	// Setting the scheduler command value and action
	sci, ok := proto.SchedulerCommand_value[action]
	if !ok {
		return proto.SchedulerCommand_NONE, fmt.Errorf("transfer failed to find value %s in the scheduler command map", action)
	}
	sc := proto.SchedulerCommand(sci)

	return sc, nil
}

// GetNodeInfo returns a copy of the schedulers Nodeinfo map
func (s *Scheduler) GetNodeInfo() map[string]*NodeInfo {
	s.nodeInfoLock.RLock()
	defer s.nodeInfoLock.RUnlock()

	return maps.Clone(s.nodeInfo)
}

func (s *Scheduler) GetSchedulerID() uuid.UUID {
	return s.id
}

func (s *Scheduler) removeActiveJob(eventID uuid.UUID) {
	s.jMutex.Lock()
	delete(s.activeJobs, eventID)
	s.jMutex.Unlock()
}

// addedExistingJobsToQueue adds any existing jobs in etcd to the scheduler's priority queue
func (s *Scheduler) addExistingJobsToQueue() error {
	resp, err := s.em.GetPrefix(proto.JobsPrefix)
	if err != nil {
		return fmt.Errorf("failed to get existing jobs from etcd: %v", err)
	}
	events := []*clientv3.Event{}

	s.log.Debugf("adding existing %v jobs to priority queue", len(resp.Kvs))

	for _, kv := range resp.Kvs {
		events = append(events, &clientv3.Event{
			Type: mvccpb.PUT,
			Kv:   kv,
		})
	}

	s.handleJobEvent(events)

	return nil
}
