// Copyright 2026. Triad National Security, LLC. All rights reserved.

package httpserver

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/handlers"
	"github.com/lanl/conduit/api"
	"github.com/lanl/conduit/defaults"
	cliutil "github.com/lanl/conduit/internal/cli/util"
	"github.com/lanl/conduit/internal/logger"
	"github.com/lanl/conduit/internal/server/httpserver/auth"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type HTTPServer struct {
	log *logger.ConduitLogger

	addr      string
	server    *http.Server
	router    http.Handler
	validator *auth.Introspector

	conduitClient     api.ConduitApiClient
	conduitClientConn *grpc.ClientConn

	originPolicy *OriginPolicy
}

func CreateHTTPServer(log *logger.ConduitLogger, addr string, clientCert *tls.Certificate, certPool *x509.CertPool, grpcAddr string) (*HTTPServer, error) {
	l := logger.NewConduitLogger(log.GetLevel(), fmt.Sprintf("%sHTTP server:", log.GetPrefix()))
	if log.GetPrefix() == "" {
		l = logger.NewConduitLogger(log.GetLevel(), "HTTP server:")
	}

	rawAllowedOrigins := viper.GetStringSlice(defaults.ConfigServerHTTPAllowedOriginsKey)

	originPolicy, err := NewOriginPolicy(l, rawAllowedOrigins)
	if err != nil {
		return nil, err
	}

	router := http.NewServeMux()

	// Get auth mode configuration
	authMode := viper.GetString(defaults.ConfigServerHTTPAuthModeKey)
	l.Infof("HTTP authentication mode: %s", authMode)

	// Validate auth mode
	validAuthModes := map[string]bool{
		"oauth":         true,
		"mtls":          true,
		"oauth-or-mtls": true,
	}
	if !validAuthModes[authMode] {
		return nil, fmt.Errorf("invalid auth-mode '%s': must be 'oauth', 'mtls', or 'oauth-or-mtls'", authMode)
	}

	var validator *auth.Introspector

	// Only initialize OAuth validator if OAuth is being used
	if authMode == "oauth" || authMode == "oauth-or-mtls" {
		discoveryURL := viper.GetString(defaults.ConfigOAuthDiscoveryKey)
		clientID := viper.GetString(defaults.ConfigOAuthclientIDKey)
		clientSecret := viper.GetString(defaults.ConfigOAuthclientSecretKey)
		userInfoFallback := viper.GetBool(defaults.ConfigOAuthUserFallbackKey)
		usernameClaims := viper.GetStringSlice(defaults.ConfigOAuthUserClaimsKey)
		introspectionAuthMethod := viper.GetString(defaults.ConfigOAuthIntrospectionAuthMethodKey)
		expectedAudience := viper.GetString(defaults.ConfigOAuthAudienceKey)

		if discoveryURL == "" {
			return nil, fmt.Errorf("OAuth discovery URL is required when using OAuth authentication")
		}
		if clientID == "" {
			return nil, fmt.Errorf("OAuth client ID is required when using OAuth authentication")
		}
		if clientSecret == "" {
			return nil, fmt.Errorf("OAuth client secret is required when using OAuth authentication")
		}

		authCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		oauthCertPool, err := cliutil.GetCertPoolFromViper(
			defaults.ConfigOAuthCAKey,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to load OAuth CA certificate: %w",
				err,
			)
		}

		oauthTLSConfig := &tls.Config{
			RootCAs:    oauthCertPool,
			MinVersion: tls.VersionTLS12,
		}

		validator, err = auth.NewIntrospector(authCtx, auth.Config{
			DiscoveryURL:                 discoveryURL,
			ClientID:                     clientID,
			ClientSecret:                 clientSecret,
			UsernameClaims:               usernameClaims,
			UseUserInfoFallback:          userInfoFallback,
			TLSConfig:                    oauthTLSConfig,
			ViperIntrospectionAuthMethod: introspectionAuthMethod,
			ExpectedAudience:             expectedAudience,
		}, l)
		if err != nil {
			return nil, fmt.Errorf("failed to create http validator: %v", err)
		}
	}

	conn, conduitClient, err := GetGRPCClient(l, clientCert, certPool, grpcAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to get grpc client: %v", err)
	}

	h := &HTTPServer{
		addr:              addr,
		log:               l,
		conduitClient:     conduitClient,
		conduitClientConn: conn,
		router:            router,
		validator:         validator,
		originPolicy:      originPolicy,
	}

	h.registerHTTPRoutes(router, authMode)

	corsHandler := handlers.CORS(
		handlers.AllowedOrigins(originPolicy.CORSOrigins()),
		handlers.AllowedHeaders([]string{
			"Authorization",
			"Content-Type",
		}),
		handlers.AllowedMethods([]string{
			http.MethodGet,
			http.MethodPost,
			http.MethodOptions,
		}),
	)(router)

	h.server = &http.Server{
		Handler: corsHandler,
		Addr:    addr,

		ReadHeaderTimeout: 15 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	certFile := viper.GetString(defaults.ConfigServerHTTPTLSCertKey)
	keyFile := viper.GetString(defaults.ConfigServerHTTPTLSKeyKey)
	if certFile != "" || keyFile != "" {
		if certFile == "" || keyFile == "" {
			return nil, fmt.Errorf("both TLS certificate and key must be configured")
		}

		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, err
		}

		h.server.TLSConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}
	}

	return h, nil
}

func (h *HTTPServer) StartHTTPServer(authMode string, certPool *x509.CertPool, serverCert *tls.Certificate) error {
	defer h.conduitClientConn.Close()
	h.log.Infof("HTTP server listening on %s with TLS enabled", h.addr)

	// Configure TLS for all modes
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{*serverCert},
		MinVersion:   tls.VersionTLS12,
	}

	// Configure client certificate requirements based on auth mode
	switch authMode {
	case "mtls":
		h.log.Infof("mTLS mode: requiring client certificates")
		tlsConfig.ClientCAs = certPool
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	case "oauth-or-mtls":
		h.log.Infof("OAuth-or-mTLS mode: client certificates optional")
		tlsConfig.ClientCAs = certPool
		tlsConfig.ClientAuth = tls.VerifyClientCertIfGiven
	case "oauth":
		h.log.Infof("OAuth mode: client certificates not required")
		tlsConfig.ClientAuth = tls.NoClientCert
	default:
		return fmt.Errorf("unsupported http auth mode: %v", authMode)
	}

	h.server.TLSConfig = tlsConfig

	// Always use TLS
	if err := h.server.ListenAndServeTLS("", ""); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("server error: %w", err)
	}

	return nil
}

func (h *HTTPServer) registerHTTPRoutes(router *http.ServeMux, authMode string) {
	router.Handle("GET /ws", h.authedHandler(h.serveWs, authMode))

	router.Handle("GET /transfers", h.authedHandler(h.getTransfers, authMode))
	router.Handle("POST /transfers", h.authedHandler(h.startTransfer, authMode))

	router.Handle("GET /transfers/{transferID}", h.authedHandler(h.getTransferByID, authMode))
	router.Handle("POST /transfers/query", h.authedHandler(h.queryTransfers, authMode))
	router.Handle("POST /transfers/abort", h.authedHandler(h.abortTransfers, authMode))
	router.Handle("POST /transfers/{transferID}/abort", h.authedHandler(h.abortTransfer, authMode))
}

func (h *HTTPServer) authedHandler(next func(http.ResponseWriter, *http.Request, string), authMode string) http.Handler {
	var middleware http.Handler

	handler := http.HandlerFunc(func(wr http.ResponseWriter, req *http.Request) {
		req.Body = http.MaxBytesReader(wr, req.Body, 1<<20) // 1 MiB
		defer req.Body.Close()

		username, ok := auth.UsernameFromRequest(req)
		if !ok {
			http.Error(wr, "missing authenticated user", http.StatusInternalServerError)
			return
		}

		next(wr, req, username)
	})

	// Apply the appropriate authentication middleware based on auth mode
	switch authMode {
	case "oauth":
		middleware = auth.RequireBearer(h.validator, auth.ValidateOptions{}, handler)
	case "mtls":
		middleware = auth.RequireMTLS(handler)
	case "oauth-or-mtls":
		middleware = auth.RequireBearerOrMTLS(h.validator, auth.ValidateOptions{}, handler)
	default:
		// Should not reach here due to validation in CreateHTTPServer
		middleware = handler
	}

	return middleware
}

// getGRPCClient authenticates with kerberos, dials into the conduit server, and returns a ConduitApiClient
func GetGRPCClient(log *logger.ConduitLogger, clientCert *tls.Certificate, certPool *x509.CertPool, conduitAddr string) (*grpc.ClientConn, api.ConduitApiClient, error) {
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{*clientCert},
		RootCAs:      certPool,
		MinVersion:   tls.VersionTLS13,
		MaxVersion:   tls.VersionTLS13,
	}

	creds := credentials.NewTLS(tlsConfig)

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
	}

	log.Debugf("dialing to grpc server at: %v\n", conduitAddr)
	conn, err := grpc.NewClient(conduitAddr, opts...)
	// conn, err := grpc.DialContext(ctx, conduitAddr, opts...)
	if err != nil {
		// ctxCancel()
		return nil, nil, fmt.Errorf("failed to dial into conduit server: %v", err)
	}

	log.Debugf("creating grpc client")
	client := api.NewConduitApiClient(conn)

	return conn, client, nil
}
