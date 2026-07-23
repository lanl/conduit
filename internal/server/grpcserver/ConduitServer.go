// Copyright 2026. Triad National Security, LLC. All rights reserved.

package grpcserver

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/jcmturner/gokrb5/v8/service"
	grpckrb "github.com/kpelzel/grpckrb"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/defaults"
	"github.com/lanl/conduit/internal/etcd"
	"github.com/lanl/conduit/internal/etcd/util"
	"github.com/lanl/conduit/internal/logger"
	cert "github.com/lanl/conduit/internal/pki"
	"github.com/lanl/conduit/internal/server/httpserver"
	"github.com/lanl/conduit/internal/server/rqlite"
	"github.com/lanl/conduit/internal/server/scheduler"
	"github.com/lanl/conduit/internal/server/transferworker"
	"github.com/lanl/conduit/internal/server/watchdog"
)

var (
	privilegedServices = []string{"conduit-service"}
	privilegedAdmins   = []string{"conduit-admin"}
	queryFields        = []string{}
	adminWarning       = "This transfer has been manipulated by an admin"
)

var _ proto.ConduitApiServer = (*ConduitServer)(nil)

type ConduitServer struct {
	proto.UnimplementedConduitApiServer
	si    *grpckrb.KRBServerInterceptor
	em    *etcd.ETCDManager
	cm    *cert.CertManager
	rm    *rqlite.RqliteManager
	tws   []*transferworker.TransferWorker
	lws   []*watchdog.Watchdog
	id    uuid.UUID
	sched []*scheduler.Scheduler

	grpcServer *grpc.Server
	httpServer *http.Server
	grpcAddr   string
	httpAddr   string

	usersTransfers map[string]map[uuid.UUID]bool     // key: username value: map of transfer IDs
	transfers      map[string]*proto.TransferDetails // key: TransferID value: Transfer
	tMutex         sync.RWMutex                      // lock for transfers map

	usersErrants map[string]map[string]*timestamppb.Timestamp // key: user value: map[trashPath] value: transferID OR PURGE
	eMutex       sync.RWMutex                                 // lock for errants map

	activeStreams map[uuid.UUID]map[uuid.UUID]chan bool // key: transferID value: (key: streamID value: stream)
	asMutex       sync.RWMutex                          // lock for activeStreams map

	log *logger.ConduitLogger

	ws        chan []byte
	wsRefresh chan bool

	serverState proto.ServerState
	Shutdown    bool           // shutdown is used to signal that we are trying to shutdown so prevent the api endpoints from responding
	ssMutex     sync.RWMutex   // lock for server state
	jobs        sync.WaitGroup // this waitgroup is to track ongoing requests with the conduit server
}

func init() {
	queryFields = createFields()
}

// makeGRPCServer creates the gRPC server that handles all requests through the gRPC api
func makeGRPCServer(cl *logger.ConduitLogger, cm *cert.CertManager) (*grpc.Server, *grpckrb.KRBServerInterceptor) {
	kt, err := keytab.Load(viper.GetString(defaults.ConfigAuthKeytabKey))
	if err != nil {
		cl.Errorf("error loading conduit keytab: %v", err)
	}

	// grpckrb only accepts a standard logger so create a standard logger and forward all logs to the logrus logger
	stdLogger := log.New(os.Stderr, "", log.Ldate|log.Ltime)
	stdLogger.SetOutput(cl.Writer())

	si := &grpckrb.KRBServerInterceptor{
		Settings:       service.NewSettings(kt, service.Logger(stdLogger)),
		AllowAnonymous: true,
	}

	serverCert, err := cm.ExternalCertManager.GetServerTLSCert()
	if err != nil {
		cl.Fatalf("failed to get server cert: %v", err)
	}
	certPool, err := cm.GetCertPool(cert.EXTERNAL)
	if err != nil {
		cl.Fatalf("Failed to get cert pool for server cert: %v", err)
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{*serverCert},
		RootCAs:      certPool,
		ClientCAs:    certPool,
		MinVersion:   tls.VersionTLS13,
		MaxVersion:   tls.VersionTLS13,
		ClientAuth:   tls.VerifyClientCertIfGiven,
	}

	creds := credentials.NewTLS(tlsConfig)
	opts := []grpc.ServerOption{
		grpc.Creds(creds),
		grpc.UnaryInterceptor(si.Unary()),
		grpc.StreamInterceptor(si.Stream()),
	}
	// opts := []grpc.ServerOption{grpc.Creds(creds)}

	grpcServer := grpc.NewServer(opts...)
	return grpcServer, si
}

// CreateConduitServer creates all necessary objects to start conduit. Creates the http and gRPC server objects
func CreateConduitServer(debug bool) (*ConduitServer, error) {
	id := uuid.New()

	// create a logger
	log := logger.NewConduitLogger(logrus.InfoLevel, fmt.Sprintf("conduit[%s]:", id))
	if debug {
		log = logger.NewConduitLogger(logrus.DebugLevel, fmt.Sprintf("conduit[%s]:", id))

		// used by pprof
		runtime.SetMutexProfileFraction(1)
		runtime.SetBlockProfileRate(1)

		log.Debugf("enabling grpc debugging")
		os.Setenv("GRPC_GO_LOG_SEVERITY_LEVEL", "warning")
		os.Setenv("GRPC_GO_LOG_VERBOSITY_LEVEL", "99")
	}

	log.Debugf("possible query fields: %+v", queryFields)

	// create cert manager
	internalCACertPath := viper.GetString(defaults.ConfigInternalCACertKey)
	internalCAKeyPath := viper.GetString(defaults.ConfigInternalCAKeyKey)
	externalCACertPath := viper.GetString(defaults.ConfigExternalCACertKey)
	externalCAKeyPath := viper.GetString(defaults.ConfigExternalCAKeyKey)
	serverIPStrings := viper.GetStringSlice(defaults.ConfigServerIPKey)
	serverIPs := []net.IP{}
	for _, sips := range serverIPStrings {
		sip := net.ParseIP(sips)
		if sip == nil {
			return nil, fmt.Errorf("failed to parse ip from string: %v", sips)
		}
		serverIPs = append(serverIPs, sip)
	}
	serverHostnames := viper.GetStringSlice(defaults.ConfigServerHostnameKey)

	icm, err := cert.NewInternalCertManager(log, internalCACertPath, internalCAKeyPath, nil, nil)
	if err != nil {
		log.Fatalf("failed to create internal cert manager: %v", err)
	}

	ecm, err := cert.NewExternalCertManager(log, externalCACertPath, externalCAKeyPath, serverIPs, serverHostnames)
	if err != nil {
		log.Fatalf("failed to create external cert manager: %v", err)
	}

	cm, err := cert.NewCertManager(log, icm, ecm)
	if err != nil {
		log.Fatalf("failed to create cert manager: %v", err)
	}

	log.Debug("getting etcd client tls cert")
	etcdTLSCert, err := cm.InternalCertManager.GetETCDClientTLSCert()
	if err != nil {
		return nil, fmt.Errorf("failed to get tls cert for etcd client: %v", err)
	}

	log.Debug("creating etcd cert pool")
	certPool, err := cm.GetCertPool(cert.INTERNAL)
	if err != nil {
		return nil, fmt.Errorf("failed to get cert pool for server cert: %v", err)
	}

	endpoints, err := util.GetEtcdEndpointsFromViper()
	if err != nil {
		log.Fatalf("failed to get etcd endpoints from viper: %v", endpoints)
	}

	em := etcd.NewETCDManager(log, etcdTLSCert, certPool, endpoints)

	log.Debug("getting rqlite client tls cert")
	rqliteTLSCert, err := cm.InternalCertManager.GetRqliteClientTLSCert()
	if err != nil {
		return nil, fmt.Errorf("failed to get tls cert for rqlite client: %v", err)
	}

	rm, err := rqlite.NewRqliteManager(log, rqliteTLSCert, certPool)
	if err != nil {
		return nil, fmt.Errorf("failed to create rqlite manager: %v", err)
	}
	err = rm.CreateTable()
	if err != nil {
		return nil, fmt.Errorf("failed to create conduit table in rqlite: %v", err)
	}

	numWorkers := viper.GetInt(defaults.ConfigConcurrentTransferWorkersKey)
	tws := []*transferworker.TransferWorker{}
	for i := 0; i < numWorkers; i++ {
		tws = append(tws, transferworker.NewTransferWorker(log, cm, em))
	}

	numSchedulers := viper.GetInt(defaults.ConfigConcurrentSchedulersKey)
	sched := []*scheduler.Scheduler{}
	for i := 0; i < numSchedulers; i++ {
		s, err := scheduler.NewScheduler(log, cm, em)
		if err != nil {
			return nil, fmt.Errorf("failed to create scheduler %v", err)
		}
		sched = append(sched, s)
	}

	numWatchdogs := viper.GetInt(defaults.ConfigConcurrentWatchdogsKey)
	lws := []*watchdog.Watchdog{}
	for i := 0; i < numWatchdogs; i++ {
		lws = append(lws, watchdog.NewWatchdog(log, cm, em, rm, sched))
	}

	// Create the main listener.
	port := viper.GetInt(defaults.ConfigServerPortKey)
	wsPort := viper.GetInt(defaults.ConfigServerWSPortKey)
	serverIP := net.ParseIP(serverIPStrings[0])
	if len(serverIPStrings) > 1 {
		serverIP = net.IPv4(0, 0, 0, 0)
	}

	grpcAddr := net.JoinHostPort(serverIP.String(), strconv.Itoa(port))
	httpAddr := net.JoinHostPort(serverIP.String(), strconv.Itoa(wsPort))

	wsRefresh := make(chan bool)
	httpServer, wschan := httpserver.CreateHTTPServer(log, httpAddr, wsRefresh)

	grpcServer, si := makeGRPCServer(log, cm)

	s := &ConduitServer{
		log:            log,
		si:             si,
		em:             em,
		cm:             cm,
		rm:             rm,
		tws:            tws,
		lws:            lws,
		ws:             wschan,
		wsRefresh:      wsRefresh,
		id:             id,
		sched:          sched,
		transfers:      make(map[string]*proto.TransferDetails),
		usersTransfers: make(map[string]map[uuid.UUID]bool),
		tMutex:         sync.RWMutex{},
		grpcServer:     grpcServer,
		httpServer:     httpServer,
		grpcAddr:       grpcAddr,
		httpAddr:       httpAddr,
		activeStreams:  make(map[uuid.UUID]map[uuid.UUID]chan bool),
		asMutex:        sync.RWMutex{},
		serverState:    proto.ServerState_SERVER_STARTING,
		usersErrants:   make(map[string]map[string]*timestamppb.Timestamp),
		eMutex:         sync.RWMutex{},
	}

	// add startup job to jobs wait group
	s.jobs.Add(1)

	successChan := make(chan bool)
	go s.cacheTransfers(successChan)
	<-successChan

	eSuccessChan := make(chan bool)
	go s.cacheErrors(eSuccessChan)
	<-eSuccessChan

	return s, nil
}

// StartConduitServer is the main entrypoint of conduit
func (s *ConduitServer) StartConduitServer(clearEtcd bool) error {
	// monitor for linux sigterm
	go s.signalHandler()

	// add root user to etcd if it doesn't already exist
	s.em.AddRoot()

	endpoints, err := util.GetEtcdEndpointsFromViper()
	if err != nil {
		log.Fatalf("failed to get etcd endpoints from viper: %v", endpoints)
	}
	if len(endpoints) == 0 {
		log.Fatalf("no etcd endpoints provided, check config")
	}

	// get the status from etcd, if we get an error from the first endpoint, try the next one
	var status *clientv3.StatusResponse
	var cr int64
	for i, e := range endpoints {
		status, cr, err = s.em.GetStatus(e)
		if err != nil {
			tErr := fmt.Errorf("failed to get etcd status: %v", err)
			s.log.Warn(tErr)
			if i == len(endpoints)-1 {
				return tErr
			} else {
				continue
			}
		} else {
			break
		}
	}
	s.log.Debugf("etcd status: %+v", status)
	s.log.Debugf("etcd compact revision: %+v", cr)

	if status.Header.GetRevision() != cr {
		if clearEtcd {
			s.log.Debug("Clearing ETCD!")
			// delete transfer and lease areas in etcd
			// only for debugging
			_, err := s.em.DeletePrefix(proto.TransferPrefix)
			if err != nil {
				return fmt.Errorf("error removing all transfers currently in etcd: %v", err)
			}
			// only for debugging
			_, err = s.em.DeletePrefix(proto.LeasePrefix)
			if err != nil {
				return fmt.Errorf("error removing all leases currently in etcd: %v", err)
			}

			// compact everything in etcd
			_, err = s.em.CompactRevision(status.Header.GetRevision())
			if err != nil {
				return fmt.Errorf("failed to compact etcd: %v", err)
			}
		} else {
			transfers, err := s.em.GetAllTransfers(cr)
			if err != nil {
				return fmt.Errorf("failed to get all existing transfers from etcd: %v", err)
			}

			s.log.Debugf("found %v transfers already in etcd", len(transfers))

			// add transfers to transfers
			s.tMutex.Lock()
			s.transfers = transfers
			// add transfers to user transfers
			for _, td := range transfers {
				if len(s.usersTransfers[td.GetUser()]) == 0 {
					s.usersTransfers[td.GetUser()] = make(map[uuid.UUID]bool)
				}

				tid, err := uuid.Parse(td.GetTransferID())
				if err != nil {
					s.log.Errorf("failed to parse transfer id [%s]: %v", td.GetTransferID(), err)
				}
				s.usersTransfers[td.GetUser()][tid] = true
			}

			s.tMutex.Unlock()
		}
	} else {
		s.log.Debug("etcd current revision is the same as the compact revision")
	}

	go s.updateNewWSConnections()

	// have etcd mangager start watching the transfer and lease prefixes
	wctx, wCancel := context.WithCancelCause(context.Background())
	go s.em.StartWatchChannels(status.Header.GetRevision(), wCancel)
	defer s.em.CloseClient()

	grpcLis, err := net.Listen("tcp", s.grpcAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on address[%s]: %v", s.grpcAddr, err)
	}

	proto.RegisterConduitApiServer(s.grpcServer, s)

	// TODO: monitor these go routines to watch if they crash
	// go httpserver.StartHTTPServer(s.httpServer, s.log)

	for _, sch := range s.sched {
		err := sch.StartScheduler()
		if err != nil {
			log.Fatalf("failed to start scheduler: %v", err)
		}
	}

	for _, tw := range s.tws {
		err := tw.StartTransferWorker()
		if err != nil {
			log.Fatalf("failed to start transfer worker: %v", err)
		}
	}

	// check if any transfers are stuck and need to be triggered again
	go s.rePutTransfers()

	// check if any transfers in etcd need to be archived
	go s.archiveTransfers()

	for _, wd := range s.lws {
		err := wd.StartWatchdog()
		if err != nil {
			log.Fatalf("failed to start watchdog: %v", err)
		}
	}

	s.ssMutex.Lock()
	s.serverState = proto.ServerState_SERVER_RUNNING
	s.ssMutex.Unlock()

	// done with the initial startup job
	s.jobs.Done()

	serveErr := make(chan error, 1)
	go func() {
		s.log.Infof("GRPC Listening on %v", grpcLis.Addr())
		serveErr <- s.grpcServer.Serve(grpcLis)
	}()

	select {
	case <-wctx.Done():
		return fmt.Errorf("failure while watching etcd: %v", context.Cause(wctx))
	case err := <-serveErr:
		if err != nil {
			return fmt.Errorf("failed to serve grpc server: %v", err)
		}
	}

	return nil
}

// stopConduitServer is meant to be run after conduit has been paused. This just makes sure that there are no transfer submissions in flight during shutdown
func (s *ConduitServer) stopConduitServer() error {
	// set shutdown
	s.ssMutex.Lock()
	s.Shutdown = true
	s.ssMutex.Unlock()

	// wait for jobs to stop
	s.jobs.Wait()

	s.log.Info("all server jobs are complete")

	return nil
}

// archiveTransfers goes through the conduit servers transfers and sets archive to ready for any that are finalized
func (s *ConduitServer) archiveTransfers() {
	// find finalized transfers
	s.tMutex.RLock()
	for _, t := range s.transfers {
		if t.GetState() == proto.TransferState_TRANSFER_FINALIZED && t.GetError() == proto.Error_ERROR_NONE && t.GetArchiveState() == proto.ArchiveState_ARCHIVE_NONE {
			succeeded, _, err := s.em.SafelySetTransferArchiveState(t, proto.ArchiveState_ARCHIVE_NONE, proto.ArchiveState_ARCHIVE_READY)
			if err != nil {
				tErr := fmt.Errorf("error committing new transfer archive state to etcd for transfer[%s]: %v", t.GetTransferID(), err)
				s.log.Error(tErr)
			} else if !succeeded {
				s.log.Warnf("failed to set transfer[%s] archive state to %v. Another worker probably took care of it", t.GetTransferID(), proto.ArchiveState_ARCHIVE_READY.String())
			} else {
				s.log.Infof("successfully set transfer[%s] state to %v", t.GetTransferID(), proto.ArchiveState_ARCHIVE_READY.String())
			}
		}
	}
	s.tMutex.RUnlock()
}

// rePutTransfers goes through the conduit server transfers and re-puts them to etcd to ensure that any necessary watches are triggered
func (s *ConduitServer) rePutTransfers() {
	s.tMutex.RLock()
	defer s.tMutex.RUnlock()

	for _, t := range s.transfers {
		comparisons := []clientv3.Cmp{}
		actions := []clientv3.Op{}
		comparisons = append(comparisons, clientv3.Compare(clientv3.Value(t.ETCDStateKey()), "=", t.GetState().String()))
		actions = append(actions, clientv3.OpPut(t.ETCDStateKey(), t.GetState().String()))

		resp, err := s.em.RetryTxn(&comparisons, &actions, defaults.MaxRetries, defaults.RetryDelay)
		if err != nil || !resp.Succeeded {
			s.log.Errorf("failed to set transfer[%s] state to current state: %s", t.GetTransferID(), err)
		} else {
			s.log.Infof("successfully set transfer[%s] state to its current state", t.GetTransferID())
		}
	}
}

func (s *ConduitServer) updateNewWSConnections() {
	for range s.wsRefresh {
		s.tMutex.Lock()
		mtd := &proto.MultiTransferDetails{Details: s.transfers}
		json, err := protojson.Marshal(mtd)
		if err != nil {
			s.log.Errorf("Failed to marshal json for websocket connection: %v", err)
		}
		s.ws <- json
		s.tMutex.Unlock()
	}
}

func (s *ConduitServer) cacheTransfers(successChan chan bool) {
	s.log.Infof("conduit server[%s]: subscribing to transfers", s.id)
	wc := s.em.SubscribeToTransfers(s.id)
	successChan <- true

	for wresp := range wc {
		// if we use a go routine to handle the transfer events, we may end up handling them out of order.
		// go s.handleTransferEvents(wresp.Events)
		// s.log.Debugf("conduit server[%s]: received transfer event from etcd", s.id)
		s.handleTransferEvents(wresp.Events)
		if wresp.Canceled {
			s.log.Error(wresp)
		}
	}
	s.log.Errorf("conduit server[%s]: transfer watch channel closed unexpectedly", s.id)
	s.em.UnsubscribeFromTransfers(s.id)

}

func (s *ConduitServer) cacheErrors(successChan chan bool) {
	s.log.Infof("conduit server[%s]: subscribing to errors", s.id)
	wc := s.em.SubscribeToErrant(s.id)
	successChan <- true

	for wresp := range wc {
		// if we use a go routine to handle the transfer events, we may end up handling them out of order.
		// go s.handleTransferEvents(wresp.Events)
		// s.log.Debugf("conduit server[%s]: received transfer event from etcd", s.id)
		s.handleErrantEvents(wresp.Events)
		if wresp.Canceled {
			s.log.Error(wresp)
		}
	}
	s.log.Errorf("conduit server[%s]: transfer watch channel closed unexpectedly", s.id)
	s.em.UnsubscribeFromErrant(s.id)

}

// handleTransferEvents gets called whenever an event is passed to the transfer watch channel
func (s *ConduitServer) handleTransferEvents(evs []*clientv3.Event) {
	eventTransfers := make(map[uuid.UUID]bool)
	s.tMutex.Lock()
	defer s.tMutex.Unlock()
	for _, ev := range evs {
		id, _, err := proto.ParseETCDTransfersKey(string(ev.Kv.Key))
		if err != nil {
			s.log.Errorf("failed to parse etcd key [%v]: %v", string(ev.Kv.Key), err)
			continue
		}

		switch ev.Type {
		case mvccpb.PUT:
			// if this transfer doesn't exist, create it
			if _, ok := s.transfers[id.String()]; !ok {
				t := &proto.TransferDetails{TransferID: id.String()}
				s.transfers[id.String()] = t
			}

			td, err := etcd.ParseETCDTransfer(id, []*mvccpb.KeyValue{ev.Kv}, s.transfers[id.String()])
			if err != nil {
				s.log.Errorf("failed to parse etcd transfer[%v] event: %v", id, err)
				continue
			}

			s.transfers[id.String()] = td

			// add transfer to userstransfers
			if _, ok := s.usersTransfers[td.GetUser()][id]; !ok {
				if len(s.usersTransfers[td.GetUser()]) == 0 {
					s.usersTransfers[td.GetUser()] = make(map[uuid.UUID]bool)
				}
				s.usersTransfers[td.GetUser()][id] = true
			}

		case mvccpb.DELETE:
			// if this transfer exists in s.transfers, delete it from s.transfers and s.userstransfers
			if td, ok := s.transfers[id.String()]; ok {

				// if transfer in s.userstransfers, delete it
				delete(s.usersTransfers[td.GetUser()], id)

				delete(s.transfers, id.String())

				s.log.Debugf("deleted transfer[%v] from server cache", id.String())
			}
		default:
			s.log.Errorf("found unknown type of etcd event: %s", ev.Type.String())
		}

		// add this transfer id to the eventTransfers map to send an update to any streams watching this transfer
		eventTransfers[id] = true
	}

	// // tell any clients that are listening on streams that there was an update
	// s.log.Debugf("sending updates for transfers: %+v", eventTransfers)
	for tid := range eventTransfers {
		go s.updateStreams(tid)
	}
}

// handleErrorEvents gets called whenever an event is passed to the error watch channel
func (s *ConduitServer) handleErrantEvents(evs []*clientv3.Event) {
	s.eMutex.Lock()
	defer s.eMutex.Unlock()
	for _, ev := range evs {
		user, trashPath, err := proto.ParseETCDErrorsKey(string(ev.Kv.Key))
		if err != nil {
			s.log.Errorf("failed to parse etcd key [%v]: %v", string(ev.Kv.Key), err)
			continue
		}

		switch ev.Type {
		case mvccpb.PUT:
			// if this user doesn't exist, create it
			if _, ok := s.usersErrants[user]; !ok {
				s.usersErrants[user] = make(map[string]*timestamppb.Timestamp)
				s.log.Debugf("creating error key for user %v", user)
			}

			timestamp, err := time.Parse(time.RFC3339, string(ev.Kv.Value))
			if err != nil {
				s.log.Errorf("failed to parse timestamp for errant path [%s]=[%s]: %v", ev.Kv.Key, ev.Kv.Value, err)
				continue
			}

			s.usersErrants[user][trashPath] = timestamppb.New(timestamp)
			s.log.Debugf("added errant path[%v] to user[%v]", trashPath, user)

		case mvccpb.DELETE:
			// if this errant exists in s.usersErrants, delete it from s.usersErrants
			if _, ok := s.usersErrants[user][trashPath]; ok {
				delete(s.usersErrants[user], trashPath)

				// delete user if its empty
				if len(s.usersErrants[user]) == 0 {
					delete(s.usersErrants, user)
				}
			}
		default:
			s.log.Errorf("found unknown type of etcd event: %s", ev.Type.String())
		}
	}
}

func (s *ConduitServer) updateStreams(transferID uuid.UUID) {
	s.asMutex.RLock()
	for tID, sm := range s.activeStreams {
		if tID == transferID {
			for _, sc := range sm {
				// NOTE: this will block if nobody is listening to the channel
				sc <- true
			}
		}
	}
	s.asMutex.RUnlock()
}

// pauseConduit will stop all transfer workers and watchdogs for this instance of conduit.
func (s *ConduitServer) pauseConduit() error {
	s.ssMutex.Lock()
	state := s.serverState
	if state == proto.ServerState_SERVER_RUNNING || state == proto.ServerState_SERVER_DRAINING {
		s.serverState = proto.ServerState_SERVER_STOPPING
	} else {
		s.ssMutex.Unlock()
		return fmt.Errorf("cannot pause server because it is not in a running or drained state: %v", state)
	}
	s.ssMutex.Unlock()

	var wdwg sync.WaitGroup
	var wdErr error
	for _, wd := range s.lws {
		wdwg.Add(1)
		go func(twd *watchdog.Watchdog) {
			defer wdwg.Done()
			err := twd.StopWatchdog()
			if err != nil {
				wdErr = err
			}
		}(wd)
	}
	wdwg.Wait()

	s.log.Info("all watchdogs stopped")

	var twwg sync.WaitGroup
	var twErr error
	for _, tw := range s.tws {
		twwg.Add(1)
		go func(ttw *transferworker.TransferWorker) {
			defer twwg.Done()
			err := ttw.StopTransferWorker()
			if err != nil {
				twErr = err
			}
		}(tw)
	}
	twwg.Wait()

	s.log.Info("all transfer workers stopped")

	var swg sync.WaitGroup
	var sErr error
	for _, s := range s.sched {
		swg.Add(1)
		go func(ts *scheduler.Scheduler) {
			defer swg.Done()
			err := ts.StopScheduler()
			if err != nil {
				sErr = err
			}
		}(s)
	}
	swg.Wait()

	s.log.Info("all schedulers stopped")

	if wdErr != nil {
		s.ssMutex.Lock()
		s.serverState = proto.ServerState_SERVER_ERROR
		s.ssMutex.Unlock()
		return fmt.Errorf("failed to stop watchdog: %v", wdErr)
	}

	if twErr != nil {
		s.ssMutex.Lock()
		s.serverState = proto.ServerState_SERVER_ERROR
		s.ssMutex.Unlock()
		return fmt.Errorf("failed to stop transfer worker: %v", twErr)
	}

	if sErr != nil {
		s.ssMutex.Lock()
		s.serverState = proto.ServerState_SERVER_ERROR
		s.ssMutex.Unlock()
		return fmt.Errorf("failed to stop scheduler: %v", sErr)
	}

	s.ssMutex.Lock()
	s.serverState = proto.ServerState_SERVER_STOPPED
	s.ssMutex.Unlock()

	return nil
}

// drainConduit will continue all currently submitted conduit jobs but not progress any new ones for this instance of conduit
func (s *ConduitServer) drainConduit() error {
	s.ssMutex.Lock()
	state := s.serverState
	if state == proto.ServerState_SERVER_RUNNING {
		s.serverState = proto.ServerState_SERVER_DRAIN_INIT
	} else {
		s.ssMutex.Unlock()
		return fmt.Errorf("cannot drain server because it is not in a running state: %v", state)
	}
	s.ssMutex.Unlock()

	// tell all transfer workers to drain
	var twwg sync.WaitGroup
	var twErr error
	for _, tw := range s.tws {
		twwg.Add(1)
		go func(ttw *transferworker.TransferWorker) {
			defer twwg.Done()
			err := ttw.DrainTransferWorker()
			if err != nil {
				twErr = err
			}
		}(tw)
	}
	twwg.Wait()

	if twErr != nil {
		s.ssMutex.Lock()
		s.serverState = proto.ServerState_SERVER_ERROR
		s.ssMutex.Unlock()
		return fmt.Errorf("failed to drain transfer worker: %v", twErr)
	}

	s.log.Info("all transfer workers are draining")

	s.ssMutex.Lock()
	s.serverState = proto.ServerState_SERVER_DRAINING
	s.ssMutex.Unlock()

	return nil
}

// resumeConduit will resume all transfer workers and watchdogs for this instance of conduit.
func (s *ConduitServer) resumeConduit() error {
	state := s.serverState

	if state == proto.ServerState_SERVER_DRAINING {
		err := s.pauseConduit()
		if err != nil {
			return fmt.Errorf("failed to pause conduit before starting again: %v", err)
		}

		state = s.serverState
	}

	if state == proto.ServerState_SERVER_STOPPED {
		s.serverState = proto.ServerState_SERVER_STARTING
	} else {
		return fmt.Errorf("cannot start server because it is not in a stopped state: %v", state)
	}

	numSchedulers := viper.GetInt(defaults.ConfigConcurrentSchedulersKey)
	sched := []*scheduler.Scheduler{}
	for i := 0; i < numSchedulers; i++ {
		s, err := scheduler.NewScheduler(s.log, s.cm, s.em)
		if err != nil {
			return fmt.Errorf("failed to create scheduler %v", err)
		}
		sched = append(sched, s)
	}

	s.sched = sched

	for _, sch := range sched {
		err := sch.StartScheduler()
		if err != nil {
			return fmt.Errorf("failed to start scheduler: %v", err)
		}
	}

	numWorkers := viper.GetInt(defaults.ConfigConcurrentTransferWorkersKey)
	tws := []*transferworker.TransferWorker{}
	for i := 0; i < numWorkers; i++ {
		ntw := transferworker.NewTransferWorker(s.log, s.cm, s.em)
		tws = append(tws, ntw)
	}

	s.tws = tws

	for _, tw := range tws {
		err := tw.StartTransferWorker()
		if err != nil {
			s.ssMutex.Lock()
			s.serverState = proto.ServerState_SERVER_ERROR
			s.ssMutex.Unlock()
			return fmt.Errorf("failed to start transfer worker: %v", err)
		}
	}

	s.rePutTransfers()

	s.archiveTransfers()

	numWatchdogs := viper.GetInt(defaults.ConfigConcurrentWatchdogsKey)
	lws := []*watchdog.Watchdog{}
	for i := 0; i < numWatchdogs; i++ {
		lws = append(lws, watchdog.NewWatchdog(s.log, s.cm, s.em, s.rm, sched))
	}

	s.lws = lws

	for _, wd := range lws {
		err := wd.StartWatchdog()
		if err != nil {
			s.ssMutex.Lock()
			s.serverState = proto.ServerState_SERVER_ERROR
			s.ssMutex.Unlock()
			return fmt.Errorf("failed to start watchdog: %v", err)
		}
	}

	s.ssMutex.Lock()
	s.serverState = proto.ServerState_SERVER_RUNNING
	s.ssMutex.Unlock()

	return nil
}

// signalHandler will watch for unix signals to shutdown gracefully
func (s *ConduitServer) signalHandler() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGHUP)

	sig := <-sigs
	s.log.Errorf("received unix signal: [%v]", sig)

	// attempt to pause conduit if not already
	err := s.pauseConduit()
	if err != nil {
		s.log.Errorf("failed to pause conduit: %v", err)
	}

	err = s.stopConduitServer()
	if err != nil {
		s.log.Errorf("failed to stop conduit server: %v", err)
	}

	s.em.CloseClient()

	s.log.Infof("shutting down")
	os.Exit(0)
}
