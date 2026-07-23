// Copyright 2026. Triad National Security, LLC. All rights reserved.

package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/credentials"
	grpckrb "github.com/kpelzel/grpckrb"
	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/defaults"
	"github.com/lanl/conduit/internal/cli/util"
	"github.com/lanl/conduit/internal/fta/actions"
	"github.com/lanl/conduit/internal/pki"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	gcredentials "google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const Krb5CCNameEnvVar = "KRB5CCNAME"

type ConduitClient struct {
	log    *logrus.Logger
	client proto.ConduitApiClient
	quiet  bool
}

// NewClient creates a new ConduitClient
func NewClient(logger *logrus.Logger, quiet bool, certPath string, keyPath string) (*ConduitClient, error) {
	client, err := getGRPCClient(logger, quiet, certPath, keyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create grpc client: %v", err)
	}

	cc := &ConduitClient{
		log:    logger,
		client: client,
		quiet:  quiet,
	}
	return cc, nil
}

// getGRPCClient authenticates with kerberos, dials into the conduit server, and returns a ConduitApiClient
func getGRPCClient(log *logrus.Logger, quiet bool, certPath string, keyPath string) (proto.ConduitApiClient, error) {
	var ci *grpckrb.KRBClientInterceptor
	var tlsCert *tls.Certificate

	// use tls cert/key if one is provided, otherwise try kerberos
	if certPath != "" && keyPath != "" {
		log.Debugf("using cert[%s] and key[%s] for authentication", certPath, keyPath)
		if log.GetLevel() != logrus.DebugLevel && !quiet {
			fmt.Println("using cert/key for authentication")
		}
		var err error
		tlsCert, err = pki.GetKeyPairFromFile(certPath, keyPath)
		if err != nil {
			return nil, fmt.Errorf("failed to load cert[%s] and key[%s] from file: %v", certPath, keyPath, err)
		}
	}

	if tlsCert != nil && time.Now().After(tlsCert.Leaf.NotAfter) {
		log.Warnf("provided mtls cert has expired, attempting kerberos")
	}

	// use kerberos if we didn't get a tls cert or if its expired
	if tlsCert == nil || time.Now().After(tlsCert.Leaf.NotAfter) {
		spn := viper.GetString(defaults.ConfigKrbSpnKey)
		log.Debugf("using kerberos principle: [%v]", spn)

		krbClient, err := getKrbClient(log, spn, quiet)
		if err != nil {
			return nil, fmt.Errorf("failed to get kerberos client: %v", err)
		}

		ci = &grpckrb.KRBClientInterceptor{
			KRBClient:  krbClient,
			DefaultSPN: spn,
		}

	}

	var certPool *x509.CertPool
	caPath := viper.GetString(defaults.ConfigConduitCAKey)
	if caPath != "" {
		certPool = x509.NewCertPool()
		caCert, err := loadCAFromFile(caPath)
		if err != nil {
			return nil, fmt.Errorf("failed to CA cert from [%v]: %v", caPath, err)
		}
		certPool.AddCert(caCert)
	}

	tlsConfig := &tls.Config{
		// Certificates: []tls.Certificate{cert},
		RootCAs:    certPool,
		MinVersion: tls.VersionTLS13,
		MaxVersion: tls.VersionTLS13,
	}

	if tlsCert != nil {
		tlsConfig.Certificates = []tls.Certificate{*tlsCert}
	}

	creds := gcredentials.NewTLS(tlsConfig)

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
	}

	if ci != nil {
		opts = append(opts, grpc.WithUnaryInterceptor(ci.Unary()))
		opts = append(opts, grpc.WithStreamInterceptor(ci.Stream()))
	}

	conduitIP := viper.GetString(defaults.ConfigConduitIPKey)
	conduitPort := strconv.Itoa(viper.GetInt(defaults.ConfigConduitPortKey))
	conduitAddr := net.JoinHostPort(conduitIP, conduitPort)

	log.Debugf("dialing to grpc server at: %v\n", conduitAddr)
	conn, err := grpc.NewClient(conduitAddr, opts...)
	// conn, err := grpc.DialContext(ctx, conduitAddr, opts...)
	if err != nil {
		// ctxCancel()
		return nil, fmt.Errorf("failed to dial into conduit server: %v", err)
	}

	log.Debugf("creating grpc client")
	client := proto.NewConduitApiClient(conn)

	return client, nil
}

func loadCAFromFile(CAPath string) (*x509.Certificate, error) {
	certBytes, err := os.ReadFile(CAPath)
	if err != nil {
		return nil, fmt.Errorf("error reading certificate from file: %v", err)
	}
	certBlock, _ := pem.Decode(certBytes)
	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("error parsing certificate from file: %v", err)
	}

	return cert, nil
}

// getKrbClient creates a kerberos client using the users cached kerberos ticket defined in the config
// note that this will also login to the kdc
func getKrbClient(logger *logrus.Logger, servicePrincipalName string, quiet bool) (*client.Client, error) {
	// Load the client krb5 config
	conf, err := config.Load(viper.GetString(defaults.ConfigKrbConfigKey))
	if err != nil {
		return nil, fmt.Errorf("could not load krb5.conf: %v", err)
	}

	logger.Debugf("loaded kerberos conf: %+v", conf)

	// run kinit script if defined
	kinitPath := viper.GetString(defaults.ConfigKrbKinitPathKey)
	if kinitPath != "" {
		kinitErr := runKinit(kinitPath)
		if kinitErr != nil && !quiet {
			fmt.Printf("kinit script failed: %s\n", kinitErr)
		}
	}

	// get the current user information
	u, err := user.Current()
	if err != nil {
		return nil, fmt.Errorf("failed to get get the current user from os: %v", err)
	}

	// load the kerberos ticket cache
	cacheLocation := os.Getenv(Krb5CCNameEnvVar)
	if cacheLocation == "" {
		cacheLocation = fmt.Sprintf("%s/%s%s", viper.GetString(defaults.ConfigKrbCacheKey), viper.GetString(defaults.ConfigKrbCachePrefixKey), u.Uid)
	}

	// clean the kerberos cache ticket path
	cacheType, cleanCachePath, err := util.CleanKrbCache(cacheLocation)
	if err != nil {
		return nil, fmt.Errorf("failed to clean kerberos cache path: %v", err)
	}

	// warn about unsupported cache types
	if !(cacheType == proto.KrbCacheType_KRB_NONE || cacheType == proto.KrbCacheType_FILE || cacheType == proto.KrbCacheType_DIR) {
		if !quiet {
			logger.Warnf("kerberos cache type not supported: %s", cacheType)
		}
	}

	cleanCachePaths := []string{}

	if cacheType == proto.KrbCacheType_DIR {
		// go into the cache directory and find all ticket caches that the user has access to
		entries, err := os.ReadDir(cleanCachePath)
		if err != nil {
			return nil, fmt.Errorf("failed to read kerberos cache dir[%v]: %v", cleanCachePath, err)
		}

		for _, e := range entries {
			if !e.IsDir() {
				entryFullPath := filepath.Join(cleanCachePath, e.Name())

				err = unix.Access(entryFullPath, unix.R_OK)
				if err == nil {
					cleanCachePaths = append(cleanCachePaths, entryFullPath)
				} else {
					logger.Debugf("failed to access ticket cache [%v]: %v", entryFullPath, err)
				}
			}
		}

	} else {
		cleanCachePaths = append(cleanCachePaths, cleanCachePath)
	}

	if len(cleanCachePaths) == 0 {
		return nil, fmt.Errorf("failed to find any valid kerberos ticket cache at [%v]", cleanCachePath)
	}

	var cl *client.Client

	for i, ccp := range cleanCachePaths {
		ccache, loadCacheErr := credentials.LoadCCache(ccp)
		if loadCacheErr != nil {
			return nil, fmt.Errorf("could not load kerberos ticket cache at [%v]: %v", ccp, loadCacheErr)
		}

		logger.Debugf("using ticket kerberos ticket cache: %v", ccp)

		// grpckrb only accepts a standard logger so create a standard logger and forward all logs to the logrus logger
		stdLogger := log.New(os.Stderr, "gokrb5", log.Ldate|log.Ltime)
		stdLogger.SetOutput(logger.WriterLevel(logrus.DebugLevel))

		// Create a client from the loaded CCache
		cl, err = client.NewFromCCache(ccache, conf, client.Logger(stdLogger), client.DisablePAFXFAST(true))
		if err != nil {
			return nil, fmt.Errorf("failed to create client from kerberos ticket cache: %v", err)
		}

		realm := cl.Config.ResolveRealm(servicePrincipalName)
		logger.Debugf("using realm for principle[%v]: [%v]", servicePrincipalName, realm)

		// Log in the kerberos client
		err = cl.Login()
		if err != nil {
			// if this is the last available kerberos ticket cache, error out
			if i == len(cleanCachePaths)-1 {
				return nil, fmt.Errorf("failed to login kerberos client: %v", err)
			} else {
				logger.Debugf("failed to login kerberos client (trying next available kerberos cache): %v", err)
			}
		}
	}

	return cl, nil
}

// StartTransfer sends a GRPC transfer request to the conduit server
func (cc *ConduitClient) StartTransfer(action string, options map[string]*anypb.Any, src []string, dst string, skipValidation bool, pauseState proto.TransferState, validationOnly bool, user string, comment string, workdir string) (*proto.TransferDetails, error) {
	notifyOfChangedPath := false
	cleanSrc := []string{}
	for _, s := range src {
		// if the source isn't already an absolute path, prepend the custom working directory if one was provided
		if workdir != "" && !filepath.IsAbs(s) {
			s = filepath.Join(workdir, s)
		}
		as, err := filepath.Abs(s)
		if err != nil {
			return nil, fmt.Errorf("failed to get absolute path for source[%v]: %v", s, err)
		}
		if as != s {
			notifyOfChangedPath = true
		}
		cleanSrc = append(cleanSrc, as)
	}
	if len(cleanSrc) == 0 {
		// fmt.Printf("no valid sources have been provided, check earlier error messages\n")
		return nil, fmt.Errorf("no valid sources have been provided, check earlier error messages")
	}
	// if the destination isn't already an absolute path, prepend the custom working directory if one was provided
	if workdir != "" && !filepath.IsAbs(dst) {
		dst = filepath.Join(workdir, dst)
	}
	cleanDst, err := filepath.Abs(dst)
	if err != nil {
		return nil, fmt.Errorf("failed to get absolute path for destination[%v]: %v", dst, err)
	}
	// check if destination ends in a slash and if it does, add it back on
	if strings.HasSuffix(dst, string(os.PathSeparator)) {
		cleanDst += string(os.PathSeparator)
	}
	if dst != cleanDst {
		notifyOfChangedPath = true
	}
	if notifyOfChangedPath && !cc.quiet {
		fmt.Printf("Using source: %v\n", strings.Join(cleanSrc, ", "))
		fmt.Printf("Using destination: %v\n", cleanDst)
	}

	deprecatedAction := GetOldAction(action, options)

	tr := &proto.TransferRequest{
		User:             user,
		DeprecatedAction: &deprecatedAction,
		Action:           action,
		Options:          options,
		Source:           cleanSrc,
		Destination:      cleanDst,
		PausedState:      pauseState,
		Comment:          comment,
	}

	cc.log.Debugf("request to %s src:%v to dst:%v", action, tr.Source, tr.Destination)
	timeout := viper.GetDuration(defaults.ConfigConduitTimeoutKey)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var response *proto.TransferDetails

	if validationOnly {
		response, err = cc.client.ValidateTransfer(ctx, tr, grpc.WaitForReady(true))
		if err != nil {
			return nil, fmt.Errorf("failed to send transfer request: %v", err)
		}
	} else {
		response, err = cc.client.StartTransfer(ctx, tr, grpc.WaitForReady(true))
		if err != nil {
			return nil, fmt.Errorf("failed to send transfer request: %v", err)
		}
	}

	if skipValidation {
		return response, nil
	} else {
		quitAnimation := make(chan bool)
		if !cc.quiet {
			go printWaitingMessage(quitAnimation)
		}
		defer func() {
			if !cc.quiet {
				quitAnimation <- true
			}
		}()

		// setup a status stream until validation succeeds
		ctx, wCancel := context.WithCancel(context.Background())
		wc, err := cc.client.WatchStatus(ctx, &proto.TransferIds{Value: []string{response.GetTransferID()}})
		if err != nil {
			wCancel()
			return nil, fmt.Errorf("failed to watch transfer[%v]: %v", response.GetTransferID(), err)
		}

		go func() {
			<-wc.Context().Done()
			cc.log.Debugf("transfer[%v] grpc stream closed", response.GetTransferID())
		}()
		defer func() {
			cc.log.Debugf("transfer[%v] closing grpc stream", response.GetTransferID())
			wCancel()
		}()
		// this just closes the send side of grpc stream since we are only receiving
		err = wc.CloseSend()
		if err != nil {
			wErr := fmt.Errorf("transfer[%v] failed to close the send side of the stream: %v", response.GetTransferID(), err)
			return nil, wErr
		}

		if cc.log.GetLevel() == logrus.DebugLevel {
			if !cc.quiet {
				quitAnimation <- true
				cc.log.Debugf("transfer[%v] starting to listen to the watch stream", response.GetTransferID())
				go printWaitingMessage(quitAnimation)
			} else {
				cc.log.Debugf("transfer[%v] starting to listen to the watch stream", response.GetTransferID())
			}
		}

		for {
			mtd, err := wc.Recv()
			if cc.log.GetLevel() == logrus.DebugLevel {
				if !cc.quiet {
					quitAnimation <- true
					cc.log.Debugf("transfer[%v] received status message: %+v", response.GetTransferID(), mtd)
					go printWaitingMessage(quitAnimation)
				} else {
					cc.log.Debugf("transfer[%v] received status message: %+v", response.GetTransferID(), mtd)
				}
			}
			if err != nil {
				wErr := fmt.Errorf("transfer[%v] error while watching status: %v", response.GetTransferID(), err)
				return nil, wErr
			}
			for _, wtd := range mtd.Details {
				if wtd.GetTransferID() == response.GetTransferID() {
					s := wtd.GetState()
					switch {
					case s >= 1 && s <= 9:
						// these states are all problematic (abort, error, etc)
						// do error stuff
						wErr := fmt.Errorf("%v: %v", wtd.GetError(), wtd.GetErrorMessage())
						return wtd, wErr
					case s == 0 || (s >= 10 && s <= 14):
						// ignore. These states are normal starting states
					case s >= 15:
						// success
						return wtd, nil
					default:
						return nil, fmt.Errorf("received unrecognized state: %v", wtd.GetState())
					}
				}
			}
		}
	}
}

func (cc *ConduitClient) GetVersion() (*proto.VersionInfo, error) {
	timeout := viper.GetDuration(defaults.ConfigConduitTimeoutKey)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	response, err := cc.client.Version(ctx, &emptypb.Empty{}, grpc.WaitForReady(true))
	if err != nil {
		return nil, fmt.Errorf("failed to get conduit version: %v", err)
	}
	// cc.log.Debugf("got respnse: \nsrc: %v \ndst: %v \nuser: %v\nstatus: %v\nstartTime: %v\n", response.GetSource(), response.GetDestination(), response.GetUser(), response.GetStatus(), response.GetStartTime().AsTime().Local())
	return response, nil
}

// PauseTransfer sends a GRPC pause request to the conduit server for the specified TransferID
func (cc *ConduitClient) PauseTransfer(id uuid.UUID, pauseState proto.TransferState) (*proto.TransferDetails, error) {
	pr := &proto.PauseRequest{
		TransferID:  id.String(),
		PausedState: pauseState,
	}

	timeout := viper.GetDuration(defaults.ConfigConduitTimeoutKey)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	response, err := cc.client.PauseTransfer(ctx, pr, grpc.WaitForReady(true))
	if err != nil {
		return nil, fmt.Errorf("failed to send pause request: %v", err)
	}
	// cc.log.Debugf("got respnse: \nsrc: %v \ndst: %v \nuser: %v\nstatus: %v\nstartTime: %v\n", response.GetSource(), response.GetDestination(), response.GetUser(), response.GetStatus(), response.GetStartTime().AsTime().Local())
	return response, nil
}

// StopTransfer sends a GRPC abort request to the conduit server for the specified TransferID(s)
func (cc *ConduitClient) StopTransfer(tids []string) (*proto.MultiTransferDetails, error) {
	pTids := &proto.TransferIds{
		Value: tids,
	}

	timeout := viper.GetDuration(defaults.ConfigConduitTimeoutKey)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	response, err := cc.client.StopTransfer(ctx, pTids, grpc.WaitForReady(true))
	if err != nil {
		return nil, fmt.Errorf("failed to send abort request: %v", err)
	}
	// cc.log.Debugf("got respnse: \nsrc: %v \ndst: %v \nuser: %v\nstatus: %v\nstartTime: %v\n", response.GetSource(), response.GetDestination(), response.GetUser(), response.GetStatus(), response.GetStartTime().AsTime().Local())
	return response, nil
}

func printWaitingMessage(stop chan bool) {
	fmt.Printf("waiting for validation...")
	i := 0
	for {
		select {
		case <-stop:
			fmt.Printf("\rwaiting for validation...  \n")
			return
		default:
			chars := []string{" ", " ", ".", ".", "."}
			fmt.Printf("\rwaiting for validation%s%s%s%s%s", chars[(i+4)%5], chars[(i+3)%5], chars[(i+2)%5], chars[(i+1)%5], chars[i%5])
			time.Sleep(100 * time.Millisecond)
			i++
		}
	}
}

// Query sends a GRPC query request to the conduit server for the specified query options
func (cc *ConduitClient) Query(qo *proto.QueryOptions) (*proto.MultiTransferDetails, error) {
	timeout := viper.GetDuration(defaults.ConfigConduitTimeoutKey)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	grpcLimit := viper.GetInt(defaults.ConfigClientGrpcLimitKey)

	response, err := cc.client.Query(ctx, qo, grpc.MaxCallRecvMsgSize(grpcLimit), grpc.WaitForReady(true))
	if err != nil {
		return nil, fmt.Errorf("failed to send query request: %v", err)
	}

	return response, nil
}

// WatchStatus sets up a stream for the client to watch the status of transfers
// in close to real time
func (cc *ConduitClient) WatchStatus(tids []string, user string) (proto.ConduitApi_WatchStatusClient, context.CancelFunc, error) {
	ctx, wCancel := context.WithCancel(context.Background())
	wc, err := cc.client.WatchStatus(ctx, &proto.TransferIds{Value: tids, User: user}, grpc.WaitForReady(true))
	if err != nil {
		wCancel()
		return nil, nil, fmt.Errorf("failed to watch transfer[%v]: %v", tids, err)
	}

	return wc, wCancel, err
}

// runKinit will execute the kinit script at the provided location. It will block until the kinit script returns
func runKinit(kinitPath string) error {
	kinitCMD := exec.Command(kinitPath)

	kinitCMD.Stdin = os.Stdin
	kinitCMD.Stdout = os.Stdout
	kinitCMD.Stderr = os.Stderr

	err := kinitCMD.Start()
	if err != nil {
		return fmt.Errorf("failed to start reticket command: %s", err)
	}

	err = kinitCMD.Wait()
	if err != nil {
		return fmt.Errorf("error during reticket command: %s", err)
	}

	return nil
}

func (cc *ConduitClient) ServerControl(action proto.ServerControlAction) (*proto.ServerControlResponse, error) {
	cr := &proto.ServerControlRequest{
		Action: action,
	}

	timeout := viper.GetDuration(defaults.ConfigConduitTimeoutKey)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	response, err := cc.client.ServerControl(ctx, cr, grpc.WaitForReady(true))
	if err != nil {
		return nil, fmt.Errorf("failed to send server control request: %v", err)
	}
	// cc.log.Debugf("got respnse: \nsrc: %v \ndst: %v \nuser: %v\nstatus: %v\nstartTime: %v\n", response.GetSource(), response.GetDestination(), response.GetUser(), response.GetStatus(), response.GetStartTime().AsTime().Local())
	return response, nil
}

// GetCert retreives a PEM for a specified user to use for authentication
func (cc *ConduitClient) GetCert(user string) (*proto.CertResponse, error) {
	cr := &proto.CertRequest{
		User: user,
	}

	timeout := viper.GetDuration(defaults.ConfigConduitTimeoutKey)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	response, err := cc.client.GetCert(ctx, cr, grpc.WaitForReady(true))
	if err != nil {
		return nil, fmt.Errorf("failed to send cert request: %v", err)
	}

	return response, nil
}

func (cc *ConduitClient) SchedulerInfo() (*proto.SchedulerInfoResponse, error) {
	timeout := viper.GetDuration(defaults.ConfigConduitTimeoutKey)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	response, err := cc.client.SchedulerInfo(ctx, &emptypb.Empty{}, grpc.WaitForReady(true))
	if err != nil {
		return nil, fmt.Errorf("failed to get conduit scheduler info: %v", err)
	}
	// cc.log.Debugf("got respnse: \nsrc: %v \ndst: %v \nuser: %v\nstatus: %v\nstartTime: %v\n", response.GetSource(), response.GetDestination(), response.GetUser(), response.GetStatus(), response.GetStartTime().AsTime().Local())
	return response, nil
}

func (cc *ConduitClient) ErrantPaths() (*proto.ErrantPathsResponse, error) {
	timeout := viper.GetDuration(defaults.ConfigConduitTimeoutKey)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req := &proto.ErrantPathsRequest{
		User: "",
	}

	response, err := cc.client.ErrantPaths(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to get errant paths: %v", err)
	}
	// cc.log.Debugf("got respnse: \nsrc: %v \ndst: %v \nuser: %v\nstatus: %v\nstartTime: %v\n", response.GetSource(), response.GetDestination(), response.GetUser(), response.GetStatus(), response.GetStartTime().AsTime().Local())
	return response, nil
}

func (cc *ConduitClient) PurgeErrantPaths(paths []string, user string) (*proto.ErrantPathsResponse, error) {
	timeout := viper.GetDuration(defaults.ConfigConduitTimeoutKey)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req := &proto.PurgeErrantPathRequest{
		Paths: paths,
		User:  user,
	}

	response, err := cc.client.PurgeErrantPath(ctx, req, grpc.WaitForReady(true))
	if err != nil {
		return nil, fmt.Errorf("failed to purge paths: %v", err)
	}
	// cc.log.Debugf("got respnse: \nsrc: %v \ndst: %v \nuser: %v\nstatus: %v\nstartTime: %v\n", response.GetSource(), response.GetDestination(), response.GetUser(), response.GetStatus(), response.GetStartTime().AsTime().Local())
	return response, nil
}

// simple function used to convert a new action into a depreciated version
// it will return COPY if an action is not recognized
func GetOldAction(action string, options map[string]*anypb.Any) proto.DeprecatedAction {
	// determine the depreciated action
	recursive := false

	// add recursive flag if it was provided by the user
	if _, ok := options[actions.RecursiveFlag]; ok {
		var rec wrapperspb.BoolValue
		if err := options[actions.RecursiveFlag].UnmarshalTo(&rec); err != nil {
			// silently fail
			recursive = false
		}

		recursive = rec.GetValue()
	}

	switch action {
	case actions.Action_COPY:
		if recursive {
			return proto.DeprecatedAction_RECURSIVE_COPY
		} else {
			return proto.DeprecatedAction_COPY
		}
	case actions.Action_MOVE:
		if recursive {
			return proto.DeprecatedAction_RECURSIVE_MOVE
		} else {
			return proto.DeprecatedAction_MOVE
		}
	}

	return proto.DeprecatedAction_COPY
}
