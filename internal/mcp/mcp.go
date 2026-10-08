// Copyright 2026. Triad National Security, LLC. All rights reserved.

package mcp

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/handlers"
	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/defaults"
	cliutil "github.com/lanl/conduit/internal/cli/util"
	"github.com/lanl/conduit/internal/logger"
	"github.com/lanl/conduit/internal/server/httpserver"
	conduitauth "github.com/lanl/conduit/internal/server/httpserver/auth"
	mcpauth "github.com/modelcontextprotocol/go-sdk/auth"
	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/modelcontextprotocol/go-sdk/oauthex"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

const (
	DefaultResourcePath     = "/mcp"
	DefaultMetadataPath     = "/.well-known/oauth-protected-resource"
	DefaultTokenFallbackTTL = time.Minute
)

type MCPServer struct {
	log  *logger.ConduitLogger
	addr string

	mcpServer  *mcpsdk.Server
	httpServer *http.Server
	mux        *http.ServeMux

	validator *conduitauth.Introspector

	requiredScopes       []string
	supportedScopes      []string
	resourcePath         string
	resourceMetadataPath string
	resourceURL          string
	resourceMetadataURL  string

	tokenExpirationFallback time.Duration

	conduitClient     proto.ConduitApiClient
	conduitClientConn *grpc.ClientConn

	originPolicy *httpserver.OriginPolicy
}

func CreateMCPServer(log *logger.ConduitLogger, mcpAddr string, clientCert *tls.Certificate, certPool *x509.CertPool, conduitAddr string) (*MCPServer, error) {
	l := logger.NewConduitLogger(log.GetLevel(), fmt.Sprintf("%sMCP server:", log.GetPrefix()))
	if log.GetPrefix() == "" {
		l = logger.NewConduitLogger(log.GetLevel(), "MCP server:")
	}

	rawAllowedOrigins := viper.GetStringSlice(defaults.ConfigServerHTTPAllowedOriginsKey)

	originPolicy, err := httpserver.NewOriginPolicy(l, rawAllowedOrigins)
	if err != nil {
		return nil, err
	}

	discoveryURL := viper.GetString(defaults.ConfigOAuthDiscoveryKey)
	clientID := viper.GetString(defaults.ConfigOAuthclientIDKey)
	clientSecret := viper.GetString(defaults.ConfigOAuthclientSecretKey)
	userInfoFallback := viper.GetBool(defaults.ConfigOAuthUserFallbackKey)
	usernameClaims := viper.GetStringSlice(defaults.ConfigOAuthUserClaimsKey)
	introspectionAuthMethod := viper.GetString(defaults.ConfigOAuthIntrospectionAuthMethodKey)

	if discoveryURL == "" {
		return nil, fmt.Errorf("OAuth discovery URL is required")
	}
	if clientID == "" {
		return nil, fmt.Errorf("OAuth client ID is required")
	}
	if clientSecret == "" {
		return nil, fmt.Errorf("OAuth client secret is required")
	}

	authCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Load OAuth CA certificate pool (starts with system pool, adds custom CA if configured)
	oauthCertPool, err := cliutil.GetCertPoolFromViper(defaults.ConfigOAuthCAKey)
	if err != nil {
		return nil, fmt.Errorf("failed to load OAuth CA certificate: %w", err)
	}

	// Configure TLS with the OAuth CA certificate pool for OAuth discovery
	oauthTLSConfig := &tls.Config{
		RootCAs:    oauthCertPool,
		MinVersion: tls.VersionTLS12,
	}

	validator, err := conduitauth.NewIntrospector(authCtx, conduitauth.Config{
		DiscoveryURL:                 discoveryURL,
		ClientID:                     clientID,
		ClientSecret:                 clientSecret,
		UsernameClaims:               usernameClaims,
		UseUserInfoFallback:          userInfoFallback,
		TLSConfig:                    oauthTLSConfig,
		ViperIntrospectionAuthMethod: introspectionAuthMethod,
		ExpectedAudience:             viper.GetString(defaults.ConfigOAuthAudienceKey),
	}, l)
	if err != nil {
		return nil, fmt.Errorf("failed to create MCP OAuth validator: %w", err)
	}

	conn, conduitClient, err := httpserver.GetGRPCClient(l, clientCert, certPool, conduitAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to get grpc client: %w", err)
	}

	publicBaseURL := strings.TrimRight(viper.GetString(defaults.ConfigMCPPublicBaseURLKey), "/")
	if publicBaseURL == "" {
		return nil, fmt.Errorf("MCP public base URL is required")
	}

	resourcePath := viper.GetString(defaults.ConfigMCPResourcePathKey)
	if resourcePath == "" {
		resourcePath = DefaultResourcePath
	}

	resourceMetadataPath := viper.GetString(defaults.ConfigMCPResourceMetadataPathKey)
	if resourceMetadataPath == "" {
		resourceMetadataPath = DefaultMetadataPath
	}

	resourceURL := publicBaseURL + "/" + strings.TrimLeft(resourcePath, "/")
	resourceMetadataURL := publicBaseURL + "/" + strings.TrimLeft(resourceMetadataPath, "/")

	requiredScopes := viper.GetStringSlice(defaults.ConfigOAuthRequiredScopesKey)
	supportedScopes := viper.GetStringSlice(defaults.ConfigOAuthSupportedScopesKey)

	fallbackTTL := viper.GetDuration(defaults.ConfigOAuthTokenFallbackTTLKey)
	if fallbackTTL <= 0 {
		fallbackTTL = DefaultTokenFallbackTTL
	}

	srv := mcpsdk.NewServer(&mcpsdk.Implementation{
		Name:    "conduit-mcp-server",
		Version: "0.1.0",
	}, nil)

	mux := http.NewServeMux()

	m := &MCPServer{
		addr:                    mcpAddr,
		log:                     l,
		mcpServer:               srv,
		mux:                     mux,
		validator:               validator,
		requiredScopes:          requiredScopes,
		supportedScopes:         supportedScopes,
		resourceURL:             resourceURL,
		resourceMetadataURL:     resourceMetadataURL,
		resourcePath:            resourcePath,
		resourceMetadataPath:    resourceMetadataPath,
		tokenExpirationFallback: fallbackTTL,
		conduitClient:           conduitClient,
		conduitClientConn:       conn,
		originPolicy:            originPolicy,
	}

	m.registerMCPRoutes()
	if err := m.registerTools(); err != nil {
		_ = conn.Close()
		return nil, err
	}

	corsHandler := handlers.CORS(
		handlers.AllowedOrigins(originPolicy.CORSOrigins()),
		handlers.AllowedHeaders([]string{
			"Authorization",
			"Content-Type",
			"MCP-Protocol-Version",
			"Mcp-Session-Id",
		}),
		handlers.ExposedHeaders([]string{
			"Mcp-Session-Id",
		}),
		handlers.AllowedMethods([]string{
			http.MethodGet,
			http.MethodPost,
			http.MethodOptions,
			http.MethodDelete,
		}),
	)(mux)

	m.httpServer = &http.Server{
		Addr:              mcpAddr,
		Handler:           corsHandler,
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

		m.httpServer.TLSConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}
	}

	return m, nil
}

func (m *MCPServer) registerMCPRoutes() {
	metadata := &oauthex.ProtectedResourceMetadata{
		Resource: m.resourceURL,

		// Best value here is the OAuth issuer / authorization server base URL.
		// If you add an Issuer() method to your Introspector, use that.
		AuthorizationServers: []string{m.validator.Issuer()},

		ScopesSupported: m.supportedScopes,
	}

	m.mux.Handle(
		m.resourceMetadataPath,
		mcpauth.ProtectedResourceMetadataHandler(metadata),
	)

	handler := mcpsdk.NewStreamableHTTPHandler(func(req *http.Request) *mcpsdk.Server {
		return m.mcpServer
	}, &mcpsdk.StreamableHTTPOptions{
		SessionTimeout: 30 * time.Minute,
	})

	authMiddleware := mcpauth.RequireBearerToken(
		m.verifyToken,
		&mcpauth.RequireBearerTokenOptions{
			ResourceMetadataURL: m.resourceMetadataURL,
			Scopes:              m.requiredScopes,
		},
	)

	loggedHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m.log.Infof("=== Incoming MCP Request ===")
		m.log.Infof("Method: %s %s", r.Method, r.URL.Path)

		if !m.originPolicy.CheckRequestOrigin(r) {
			http.Error(w, "forbidden origin", http.StatusForbidden)
			return
		}

		for k, v := range r.Header {
			if strings.EqualFold(k, "Authorization") {
				m.log.Debugf("Header: %s=%v", k, []string{fmt.Sprintf("Bearer <redacted> (%v)", len(v))})
				continue
			}
		}

		authMiddleware(handler).ServeHTTP(w, r)
	})

	m.mux.Handle(m.resourcePath, loggedHandler)
}

func (m *MCPServer) StartMCPServer() error {
	defer m.conduitClientConn.Close()

	m.log.Infof("MCP server listening on %s", m.addr)
	m.log.Infof("Protected resource metadata: %s", m.resourceMetadataURL)
	m.log.Infof("MCP endpoint: %s", m.resourceURL)

	var err error
	if m.httpServer.TLSConfig != nil {
		err = m.httpServer.ListenAndServeTLS("", "")
	} else {
		err = m.httpServer.ListenAndServe()
	}

	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("MCP server error: %w", err)
	}

	return nil
}
