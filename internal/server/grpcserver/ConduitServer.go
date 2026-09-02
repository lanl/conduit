// Copyright 2026. Triad National Security, LLC. All rights reserved.

package grpcserver

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/coreos/go-systemd/daemon"
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
	"google.golang.org/grpc/health"
	"google.golang.org/protobuf/types/known/timestamppb"

	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/defaults"
	"github.com/lanl/conduit/internal/etcd"
	"github.com/lanl/conduit/internal/etcd/util"
	"github.com/lanl/conduit/internal/logger"
	cert "github.com/lanl/conduit/internal/pki"
	"github.com/lanl/conduit/internal/server/archive"
	"github.com/lanl/conduit/internal/server/httpserver"
	"github.com/lanl/conduit/internal/server/rqlite"
	"github.com/lanl/conduit/internal/server/scheduler"
	"github.com/lanl/conduit/internal/server/transferworker"
	"github.com/lanl/conduit/internal/server/watchdog"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

const (
	HTTP_CERT_NAME = "conduit-http"
)

var (
	privilegedServices = []string{"conduit-service", "conduit-http"}
	privilegedAdmins   = []string{"conduit-admin"}
	queryFields        = []string{}
	adminWarning       = "This transfer has been manipulated by an admin"
)

var _ proto.ConduitApiServer = (*ConduitServer)(nil)

type ConduitServer struct {
	proto.UnimplementedConduitApiServer
	si              *grpckrb.KRBServerInterceptor
	em              *etcd.ETCDManager
	cm              *cert.CertManager
	rm              *rqlite.RqliteManager
	transferWorkers []*transferworker.TransferWorker
	watchdogs       []*watchdog.Watchdog
	archivers       []*archive.Archiver
	id              uuid.UUID
	schdulers       []*scheduler.Scheduler

	grpcServer   *grpc.Server
	httpServer   *httpserver.HTTPServer
	healthServer *health.Server
	grpcAddr     string

	usersTransfers map[string]map[uuid.UUID]bool     // key: username value: map of transfer IDs
	transfers      map[string]*proto.TransferDetails // key: TransferID value: Transfer
	tMutex         sync.RWMutex                      // lock for transfers map

	usersErrants map[string]map[string]*timestamppb.Timestamp // key: user value: map[trashPath] value: transferID OR PURGE
	eMutex       sync.RWMutex                                 // lock for errants map

	activeStreams map[uuid.UUID]map[uuid.UUID]chan bool // key: transferID value: (key: streamID value: stream)
	asMutex       sync.RWMutex                          // lock for activeStreams map

	userStreams        map[string]map[uuid.UUID]*userStream // key: username value: (key: streamID value: userStream)
	userStreamsWorkers map[string]*userNotificationWorker
	usMutex            sync.RWMutex // lock for userStreams map

	log *logger.ConduitLogger

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

	log.Info("getting etcd client tls cert")
	etcdTLSCert, err := cm.InternalCertManager.GetETCDClientTLSCert()
	if err != nil {
		return nil, fmt.Errorf("failed to get tls cert for etcd client: %v", err)
	}

	log.Info("creating etcd cert pool")
	certPool, err := cm.GetCertPool(cert.INTERNAL)
	if err != nil {
		return nil, fmt.Errorf("failed to get cert pool for server cert: %v", err)
	}

	endpoints, err := util.GetEtcdEndpointsFromViper()
	if err != nil {
		log.Fatalf("failed to get etcd endpoints from viper: %v", endpoints)
	}

	em := etcd.NewETCDManager(log, etcdTLSCert, certPool, endpoints)

	log.Info("getting rqlite client tls cert")
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
	aws := []*archive.Archiver{}
	for i := 0; i < numWatchdogs; i++ {
		lws = append(lws, watchdog.NewWatchdog(log, cm, em, sched))
		aws = append(aws, archive.NewArchiver(log, cm, em, rm))
	}

	// Create the main listener.
	port := viper.GetInt(defaults.ConfigServerPortKey)

	grpcListenIP := net.ParseIP(serverIPStrings[0])
	if len(serverIPStrings) > 1 {
		grpcListenIP = net.IPv4zero
	}

	grpcListenAddr := net.JoinHostPort(grpcListenIP.String(), strconv.Itoa(port))
	grpcDialAddr := net.JoinHostPort(serverHostnames[0], strconv.Itoa(port))

	var httpServer *httpserver.HTTPServer

	httpEnabled := viper.GetBool(defaults.ConfigServerHTTPEnabledKey)
	if httpEnabled {
		httpPort := viper.GetInt(defaults.ConfigServerHTTPPortKey)
		httpAddr := net.JoinHostPort(grpcListenIP.String(), strconv.Itoa(httpPort))

		_, httpCreds, err := cm.ExternalCertManager.GetClientCreds(HTTP_CERT_NAME, time.Now().AddDate(10, 0, 0))
		if err != nil {
			return nil, fmt.Errorf("failed to generate http cert: %v", err)
		}

		exCertPool, err := cm.GetCertPool(cert.EXTERNAL)
		if err != nil {
			return nil, fmt.Errorf("failed to get external cert pool: %v", err)
		}

		httpServer, err = httpserver.CreateHTTPServer(log, httpAddr, httpCreds, exCertPool, grpcDialAddr)
		if err != nil {
			return nil, fmt.Errorf("failed to create http server: %v", err)
		}
	}

	grpcServer, si := makeGRPCServer(log, cm)

	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	healthpb.RegisterHealthServer(grpcServer, healthServer)

	s := &ConduitServer{
		log:                log,
		si:                 si,
		em:                 em,
		cm:                 cm,
		rm:                 rm,
		transferWorkers:    tws,
		watchdogs:          lws,
		archivers:          aws,
		id:                 id,
		schdulers:          sched,
		transfers:          make(map[string]*proto.TransferDetails),
		usersTransfers:     make(map[string]map[uuid.UUID]bool),
		tMutex:             sync.RWMutex{},
		grpcServer:         grpcServer,
		httpServer:         httpServer,
		healthServer:       healthServer,
		grpcAddr:           grpcListenAddr,
		activeStreams:      make(map[uuid.UUID]map[uuid.UUID]chan bool),
		asMutex:            sync.RWMutex{},
		userStreams:        make(map[string]map[uuid.UUID]*userStream),
		userStreamsWorkers: make(map[string]*userNotificationWorker),
		usMutex:            sync.RWMutex{},
		serverState:        proto.ServerState_SERVER_STARTING,
		usersErrants:       make(map[string]map[string]*timestamppb.Timestamp),
		eMutex:             sync.RWMutex{},
	}

	// add root user to etcd if it doesn't already exist
	s.em.AddRoot()

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
func (s *ConduitServer) StartConduitServer() error {
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

	// err = s.lws[0].CleanupETCD()
	// if err != nil {
	// 	s.log.Fatalf("failed to cleanup etcd: %v", err)
	// }

	transfers, rev, err := s.em.GetAllTransfers()
	if err != nil {
		return fmt.Errorf("failed to get all existing transfers from etcd: %v", err)
	}

	s.log.Infof("found %v transfers already in etcd", len(transfers))

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

	// have etcd mangager start watching the transfer and lease prefixes
	wctx, wCancel := context.WithCancelCause(context.Background())
	go s.em.StartWatchChannels(rev, wCancel)
	go s.em.StartUpdatingExpiries(wctx)
	defer s.em.CloseClient()

	grpcLis, err := net.Listen("tcp", s.grpcAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on address[%s]: %v", s.grpcAddr, err)
	}

	proto.RegisterConduitApiServer(s.grpcServer, s)

	// TODO: monitor these go routines to watch if they crash
	if s.httpServer != nil {
		authMode := viper.GetString(defaults.ConfigServerHTTPAuthModeKey)
		exCertPool, err := s.cm.GetCertPool(cert.EXTERNAL)
		if err != nil {
			return fmt.Errorf("failed to get external cert pool for HTTP server: %v", err)
		}

		// Get server certificate for TLS
		serverCert, err := s.cm.ExternalCertManager.GetServerTLSCert()
		if err != nil {
			return fmt.Errorf("failed to get server TLS cert for HTTP server: %v", err)
		}

		go func() {
			err := s.httpServer.StartHTTPServer(authMode, exCertPool, serverCert)
			if err != nil {
				s.log.Errorf("failed to start http server: %v", err)
			}
		}()
	}

	for _, sch := range s.schdulers {
		err := sch.StartScheduler()
		if err != nil {
			log.Fatalf("failed to start scheduler: %v", err)
		}
	}

	for _, tw := range s.transferWorkers {
		err := tw.StartTransferWorker()
		if err != nil {
			log.Fatalf("failed to start transfer worker: %v", err)
		}
	}

	// check if any transfers in etcd need to be archived
	go s.archiveTransfers()

	for _, wd := range s.watchdogs {
		err := wd.StartWatchdog()
		if err != nil {
			log.Fatalf("failed to start watchdog: %v", err)
		}
	}

	for _, a := range s.archivers {
		err := a.StartArchiver()
		if err != nil {
			log.Fatalf("failed to start archiver: %v", err)
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

	s.markConduitReady()

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
	eventUsers := make(map[string][]*proto.NotifyMessage)

	s.tMutex.Lock()

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
				s.transfers[id.String()] = &proto.TransferDetails{TransferID: id.String()}
			}

			td, err := etcd.ParseETCDTransfer(id, []*mvccpb.KeyValue{ev.Kv}, s.transfers[id.String()])
			if err != nil {
				s.log.Errorf("failed to parse etcd transfer[%v] event: %v", id, err)
				continue
			}

			s.transfers[id.String()] = td

			// add transfer to userstransfers
			if _, ok := s.usersTransfers[td.GetUser()]; !ok {
				s.usersTransfers[td.GetUser()] = make(map[uuid.UUID]bool)
			}

			if _, ok := s.usersTransfers[td.GetUser()][id]; !ok {
				s.usersTransfers[td.GetUser()][id] = true

				eventUsers[td.GetUser()] = append(
					eventUsers[td.GetUser()],
					&proto.NotifyMessage{
						TransferID: id.String(),
						Created:    true,
					},
				)
			}

		case mvccpb.DELETE:
			// Delete the transfer from both caches if it exists.
			if td, ok := s.transfers[id.String()]; ok {
				user := td.GetUser()

				delete(s.usersTransfers[user], id)

				if len(s.usersTransfers[user]) == 0 {
					delete(s.usersTransfers, user)
				}

				eventUsers[user] = append(
					eventUsers[user],
					&proto.NotifyMessage{
						TransferID: id.String(),
						Created:    false,
					},
				)

				delete(s.transfers, id.String())

				s.log.Debugf("deleted transfer[%v] from server cache", id.String())
			}

		default:
			s.log.Errorf("found unknown type of etcd event: %s", ev.Type.String())
		}

		// Notify streams watching this specific transfer.
		eventTransfers[id] = true
	}

	s.tMutex.Unlock()

	for tid := range eventTransfers {
		go s.updateTransferStreams(tid)
	}

	for user, messages := range eventUsers {
		s.enqueueUserNotifications(user, messages)
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

// pauseConduit will stop all transfer workers and watchdogs for this instance of conduit.
func (s *ConduitServer) pauseConduit() error {
	s.ssMutex.Lock()
	state := s.serverState
	if state == proto.ServerState_SERVER_RUNNING || state == proto.ServerState_SERVER_DRAINING {
		s.serverState = proto.ServerState_SERVER_STOPPING
		s.healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	} else {
		s.ssMutex.Unlock()
		return fmt.Errorf("cannot pause server because it is not in a running or drained state: %v", state)
	}
	s.ssMutex.Unlock()

	var wdwg sync.WaitGroup
	var wdErr error
	for _, wd := range s.watchdogs {
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

	var awg sync.WaitGroup
	var aErr error
	for _, a := range s.archivers {
		awg.Add(1)
		go func(ta *archive.Archiver) {
			defer awg.Done()
			err := ta.StopArchiver()
			if err != nil {
				aErr = err
			}
		}(a)
	}
	awg.Wait()

	s.log.Info("all archivers stopped")

	var twwg sync.WaitGroup
	var twErr error
	for _, tw := range s.transferWorkers {
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
	for _, s := range s.schdulers {
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

	if aErr != nil {
		s.ssMutex.Lock()
		s.serverState = proto.ServerState_SERVER_ERROR
		s.ssMutex.Unlock()
		return fmt.Errorf("failed to stop archiver: %v", aErr)
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
		s.healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	} else {
		s.ssMutex.Unlock()
		return fmt.Errorf("cannot drain server because it is not in a running state: %v", state)
	}
	s.ssMutex.Unlock()

	// tell all transfer workers to drain
	var twwg sync.WaitGroup
	var twErr error
	for _, tw := range s.transferWorkers {
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

	s.schdulers = sched

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

	s.transferWorkers = tws

	for _, tw := range tws {
		err := tw.StartTransferWorker()
		if err != nil {
			s.ssMutex.Lock()
			s.serverState = proto.ServerState_SERVER_ERROR
			s.ssMutex.Unlock()
			return fmt.Errorf("failed to start transfer worker: %v", err)
		}
	}

	s.archiveTransfers()

	numWatchdogs := viper.GetInt(defaults.ConfigConcurrentWatchdogsKey)
	lws := []*watchdog.Watchdog{}
	aws := []*archive.Archiver{}
	for i := 0; i < numWatchdogs; i++ {
		lws = append(lws, watchdog.NewWatchdog(s.log, s.cm, s.em, sched))
		aws = append(aws, archive.NewArchiver(s.log, s.cm, s.em, s.rm))
	}

	s.watchdogs = lws
	s.archivers = aws

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

	s.healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

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

func (s *ConduitServer) markConduitReady() {
	s.healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	// systemd-only; harmless/no-op when NOTIFY_SOCKET is absent.
	daemon.SdNotify(false, daemon.SdNotifyReady)

	s.log.Infof("CONDUIT_READY grpc_addr=%s server_id=%s", s.grpcAddr, s.id)
}
