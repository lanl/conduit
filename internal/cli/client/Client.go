// Copyright 2026. Triad National Security, LLC. All rights reserved.

package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
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
	conn   *grpc.ClientConn
	quiet  bool
}

// DialOptions selects the same backend authentication used by normal CLI commands.
// NonInteractive is required for MCP stdio: a credential helper must never read
// the JSON-RPC stream or write non-protocol data to stdout.
type DialOptions struct {
	CertPath       string
	KeyPath        string
	Quiet          bool
	NonInteractive bool
}

// NewClient creates a new ConduitClient
func NewClient(logger *logrus.Logger, quiet bool, certPath string, keyPath string) (*ConduitClient, error) {
	conn, grpcClient, err := DialClient(logger, DialOptions{
		CertPath: certPath,
		KeyPath:  keyPath,
		Quiet:    quiet,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create grpc client: %w", err)
	}

	return &ConduitClient{
		log:    logger,
		client: grpcClient,
		conn:   conn,
		quiet:  quiet,
	}, nil
}

// Close releases the gRPC connection owned by a regular ConduitClient.
func (cc *ConduitClient) Close() error {
	if cc == nil || cc.conn == nil {
		return nil
	}
	return cc.conn.Close()
}

// DialClient creates the Conduit backend used by both the CLI and local MCP.
// The caller owns the returned connection and must close it. NewClient takes
// ownership for regular CLI commands; the MCP server takes ownership for MCP.
// grpc.NewClient is lazy: the first RPC performs the actual network dial.
func DialClient(log *logrus.Logger, opts DialOptions) (*grpc.ClientConn, proto.ConduitApiClient, error) {
	if log == nil {
		return nil, nil, fmt.Errorf("logger is required")
	}
	if (opts.CertPath == "") != (opts.KeyPath == "") {
		return nil, nil, fmt.Errorf("both the client certificate and key must be provided")
	}

	var ci *grpckrb.KRBClientInterceptor
	var tlsCert *tls.Certificate

	// use tls cert/key if one is provided, otherwise try kerberos
	if opts.CertPath != "" {
		log.Debugf("using cert[%s] and key[%s] for authentication", opts.CertPath, opts.KeyPath)
		if log.GetLevel() != logrus.DebugLevel && !opts.Quiet {
			fmt.Println("using cert/key for authentication")
		}
		var err error
		tlsCert, err = pki.GetKeyPairFromFile(opts.CertPath, opts.KeyPath)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to load cert[%s] and key[%s] from file: %v", opts.CertPath, opts.KeyPath, err)
		}
	}

	if tlsCert != nil {
		// A tls.Certificate may not have Leaf set, depending on how it was loaded.
		if tlsCert.Leaf == nil {
			if len(tlsCert.Certificate) == 0 {
				return nil, nil, fmt.Errorf("client certificate chain is empty")
			}
			leaf, err := x509.ParseCertificate(tlsCert.Certificate[0])
			if err != nil {
				return nil, nil, fmt.Errorf("parse client certificate: %w", err)
			}
			tlsCert.Leaf = leaf
		}
		if time.Now().After(tlsCert.Leaf.NotAfter) {
			log.Warnf("provided mTLS certificate has expired; attempting Kerberos")
			// Do not also present the expired certificate during TLS handshake.
			tlsCert = nil
		}
	}

	// Use Kerberos if no valid TLS client certificate is available.
	if tlsCert == nil {
		spn := viper.GetString(defaults.ConfigKrbSpnKey)
		log.Debugf("using kerberos principle: [%v]", spn)

		krbClient, err := getKrbClient(log, spn, opts.Quiet, opts.NonInteractive)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get kerberos client: %v", err)
		}

		ci = &grpckrb.KRBClientInterceptor{
			KRBClient:  krbClient,
			DefaultSPN: spn,
		}

	}

	certPool, err := util.GetCertPoolFromViper(defaults.ConfigConduitCAKey)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get CA cert from config: %v", err)
	}

	tlsConfig := &tls.Config{
		RootCAs:    certPool,
		MinVersion: tls.VersionTLS13,
		MaxVersion: tls.VersionTLS13,
	}

	if tlsCert != nil {
		tlsConfig.Certificates = []tls.Certificate{*tlsCert}
	}

	creds := gcredentials.NewTLS(tlsConfig)

	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
	}

	if ci != nil {
		dialOpts = append(dialOpts, grpc.WithUnaryInterceptor(ci.Unary()))
		dialOpts = append(dialOpts, grpc.WithStreamInterceptor(ci.Stream()))
	}

	conduitIP := viper.GetString(defaults.ConfigConduitIPKey)
	conduitPort := strconv.Itoa(viper.GetInt(defaults.ConfigConduitPortKey))
	conduitAddr := net.JoinHostPort(conduitIP, conduitPort)

	log.Debugf("dialing to grpc server at: %v\n", conduitAddr)
	conn, err := grpc.NewClient(conduitAddr, dialOpts...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to dial into conduit server: %v", err)
	}

	log.Debugf("creating grpc client")
	client := proto.NewConduitApiClient(conn)

	return conn, client, nil
}

// getKrbClient creates a kerberos client using the users cached kerberos ticket defined in the config
// note that this will also login to the kdc
func getKrbClient(logger *logrus.Logger, servicePrincipalName string, quiet bool, nonInteractive bool) (*client.Client, error) {
	// Load the client krb5 config
	conf, err := config.Load(viper.GetString(defaults.ConfigKrbConfigKey))
	if err != nil {
		return nil, fmt.Errorf("could not load krb5.conf: %v", err)
	}

	logger.Debugf("loaded kerberos conf: %+v", conf)

	// run kinit script if defined
	kinitPath := viper.GetString(defaults.ConfigKrbKinitPathKey)
	if kinitPath != "" && !nonInteractive {
		kinitErr := runKinit(kinitPath)
		if kinitErr != nil && !quiet {
			fmt.Printf("kinit script failed: %s\n", kinitErr)
		}
	} else if kinitPath != "" {
		logger.Debug("skipping configured kinit helper in noninteractive mode; using existing ticket cache")
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

	var lastErr error
	for _, ccp := range cleanCachePaths {
		ccache, err := credentials.LoadCCache(ccp)
		if err != nil {
			lastErr = fmt.Errorf("load Kerberos ticket cache [%s]: %w", ccp, err)
			logger.Debugf("%v", lastErr)
			continue
		}

		logger.Debugf("using Kerberos ticket cache: %v", ccp)

		// grpckrb uses a standard logger; forward its output to logrus.
		stdLogger := log.New(os.Stderr, "gokrb5", log.Ldate|log.Ltime)
		stdLogger.SetOutput(logger.WriterLevel(logrus.DebugLevel))

		cl, err := client.NewFromCCache(ccache, conf, client.Logger(stdLogger), client.DisablePAFXFAST(true))
		if err != nil {
			lastErr = fmt.Errorf("create Kerberos client from cache [%s]: %w", ccp, err)
			logger.Debugf("%v", lastErr)
			continue
		}

		logger.Debugf("using realm for principal[%v]: [%v]", servicePrincipalName, cl.Config.ResolveRealm(servicePrincipalName))
		// Log in the kerberos client
		err = cl.Login()
		if err != nil {
			lastErr = fmt.Errorf("login with Kerberos ticket cache [%s]: %w", ccp, err)
			logger.Debugf("%v", lastErr)
			continue
		}
		return cl, nil
	}

	return nil, fmt.Errorf("no usable Kerberos credential cache found: %w", lastErr)
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

		grpcLimit := viper.GetInt(defaults.ConfigClientGrpcLimitKey)

		// setup a status stream until validation succeeds
		ctx, wCancel := context.WithCancel(context.Background())
		wc, err := cc.client.WatchStatus(ctx, &proto.TransferIds{Value: []string{response.GetTransferID()}}, grpc.MaxCallRecvMsgSize(grpcLimit))
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
	grpcLimit := viper.GetInt(defaults.ConfigClientGrpcLimitKey)

	ctx, wCancel := context.WithCancel(context.Background())
	wc, err := cc.client.WatchStatus(ctx, &proto.TransferIds{Value: tids, User: user}, grpc.MaxCallRecvMsgSize(grpcLimit), grpc.WaitForReady(true))
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

// ReRunTransfers sends a GRPC rerun request to the conduit server for the specified TransferIDs
func (cc *ConduitClient) ReRunTransfers(ids []string) (*proto.MultiTransferDetails, error) {
	rr := &proto.ReRunRequest{
		TransferIDs: ids,
	}

	timeout := viper.GetDuration(defaults.ConfigConduitTimeoutKey)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	response, err := cc.client.ReRunTransfers(ctx, rr, grpc.WaitForReady(true))
	if err != nil {
		return nil, fmt.Errorf("failed to send rerun request: %v", err)
	}

	return response, nil
}

// simple function used to convert a new action into a deprecated version
// it will return COPY if an action is not recognized
func GetOldAction(action string, options map[string]*anypb.Any) proto.DeprecatedAction {
	// determine the deprecated action
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
