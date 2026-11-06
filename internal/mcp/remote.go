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
	"github.com/lanl/conduit/defaults"
	cliutil "github.com/lanl/conduit/internal/cli/util"
	"github.com/lanl/conduit/internal/logger"
	"github.com/lanl/conduit/internal/server/httpserver"
	conduitauth "github.com/lanl/conduit/internal/server/httpserver/auth"
	mcpauth "github.com/modelcontextprotocol/go-sdk/auth"
	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/modelcontextprotocol/go-sdk/oauthex"
	"github.com/spf13/viper"
)

const (
	DefaultResourcePath     = "/mcp"
	DefaultMetadataPath     = "/.well-known/oauth-protected-resource"
	DefaultTokenFallbackTTL = time.Minute
)

type remoteConfig struct {
	addr                    string
	httpServer              *http.Server
	mux                     *http.ServeMux
	validator               *conduitauth.Introspector
	originPolicy            *httpserver.OriginPolicy
	requiredScopes          []string
	supportedScopes         []string
	resourcePath            string
	resourceMetadataPath    string
	resourceURL             string
	resourceMetadataURL     string
	tokenExpirationFallback time.Duration
}

// CreateMCPServer preserves the original remote constructor and its OAuth
// frontend / mTLS backend configuration. No CLI Kerberos path is used here.
func CreateMCPServer(log *logger.ConduitLogger, mcpAddr string, clientCert *tls.Certificate, certPool *x509.CertPool, conduitAddr string) (*MCPServer, error) {
	if log == nil {
		return nil, fmt.Errorf("MCP logger is required")
	}

	l := logger.NewConduitLogger(log.GetLevel(), log.GetPrefix()+"MCP server:")
	rawAllowedOrigins := viper.GetStringSlice(defaults.ConfigServerHTTPAllowedOriginsKey)
	originPolicy, err := httpserver.NewOriginPolicy(l, rawAllowedOrigins)
	if err != nil {
		return nil, err
	}

	discoveryURL := viper.GetString(defaults.ConfigOAuthDiscoveryKey)
	clientID := viper.GetString(defaults.ConfigOAuthclientIDKey)
	clientSecret := viper.GetString(defaults.ConfigOAuthclientSecretKey)
	if discoveryURL == "" || clientID == "" || clientSecret == "" {
		return nil, fmt.Errorf("OAuth discovery URL, client ID, and client secret are required")
	}

	authCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	oauthCertPool, err := cliutil.GetCertPoolFromViper(defaults.ConfigOAuthCAKey)
	if err != nil {
		return nil, fmt.Errorf("failed to load OAuth CA certificate: %w", err)
	}
	validator, err := conduitauth.NewIntrospector(authCtx, conduitauth.Config{
		DiscoveryURL:                 discoveryURL,
		ClientID:                     clientID,
		ClientSecret:                 clientSecret,
		UsernameClaims:               viper.GetStringSlice(defaults.ConfigOAuthUserClaimsKey),
		UseUserInfoFallback:          viper.GetBool(defaults.ConfigOAuthUserFallbackKey),
		TLSConfig:                    &tls.Config{RootCAs: oauthCertPool, MinVersion: tls.VersionTLS12},
		ViperIntrospectionAuthMethod: viper.GetString(defaults.ConfigOAuthIntrospectionAuthMethodKey),
		ExpectedAudience:             viper.GetString(defaults.ConfigOAuthAudienceKey),
	}, l)
	if err != nil {
		return nil, fmt.Errorf("failed to create MCP OAuth validator: %w", err)
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
	if !strings.HasPrefix(resourcePath, "/") || !strings.HasPrefix(resourceMetadataPath, "/") {
		return nil, fmt.Errorf("MCP resource paths must start with /")
	}

	fallbackTTL := viper.GetDuration(defaults.ConfigOAuthTokenFallbackTTLKey)
	if fallbackTTL <= 0 {
		fallbackTTL = DefaultTokenFallbackTTL
	}

	// This is deliberately the original remote service-certificate dialer.
	conn, conduitClient, err := httpserver.GetGRPCClient(l, clientCert, certPool, conduitAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to get remote Conduit gRPC client: %w", err)
	}

	m, err := newMCPServer(log, conn, conduitClient, "")
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	m.remote = &remoteConfig{
		addr:                    mcpAddr,
		mux:                     http.NewServeMux(),
		validator:               validator,
		originPolicy:            originPolicy,
		requiredScopes:          viper.GetStringSlice(defaults.ConfigOAuthRequiredScopesKey),
		supportedScopes:         viper.GetStringSlice(defaults.ConfigOAuthSupportedScopesKey),
		resourcePath:            resourcePath,
		resourceMetadataPath:    resourceMetadataPath,
		resourceURL:             publicBaseURL + resourcePath,
		resourceMetadataURL:     publicBaseURL + resourceMetadataPath,
		tokenExpirationFallback: fallbackTTL,
	}
	m.registerRemoteMCPRoutes()

	corsHandler := handlers.CORS(
		handlers.AllowedOrigins(originPolicy.CORSOrigins()),
		handlers.AllowedHeaders([]string{"Authorization", "Content-Type", "MCP-Protocol-Version", "Mcp-Session-Id"}),
		handlers.ExposedHeaders([]string{"Mcp-Session-Id"}),
		handlers.AllowedMethods([]string{http.MethodGet, http.MethodPost, http.MethodOptions, http.MethodDelete}),
	)(m.remote.mux)

	m.remote.httpServer = &http.Server{
		Addr:              mcpAddr,
		Handler:           corsHandler,
		ReadHeaderTimeout: 15 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	certFile := viper.GetString(defaults.ConfigServerHTTPTLSCertKey)
	keyFile := viper.GetString(defaults.ConfigServerHTTPTLSKeyKey)
	if certFile != "" || keyFile != "" {
		if certFile == "" || keyFile == "" {
			_ = m.Close()
			return nil, fmt.Errorf("both TLS certificate and key must be configured")
		}
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			_ = m.Close()
			return nil, err
		}
		m.remote.httpServer.TLSConfig = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	}

	return m, nil
}

func (m *MCPServer) registerRemoteMCPRoutes() {
	r := m.remote
	metadata := &oauthex.ProtectedResourceMetadata{
		Resource:             r.resourceURL,
		AuthorizationServers: []string{r.validator.Issuer()},
		ScopesSupported:      r.supportedScopes,
	}
	r.mux.Handle(r.resourceMetadataPath, mcpauth.ProtectedResourceMetadataHandler(metadata))

	handler := mcpsdk.NewStreamableHTTPHandler(func(*http.Request) *mcpsdk.Server {
		return m.mcpServer
	}, &mcpsdk.StreamableHTTPOptions{SessionTimeout: 30 * time.Minute})

	protected := mcpauth.RequireBearerToken(m.verifyToken, &mcpauth.RequireBearerTokenOptions{
		ResourceMetadataURL: r.resourceMetadataURL,
		Scopes:              r.requiredScopes,
	})(handler)

	r.mux.Handle(r.resourcePath, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if !r.originPolicy.CheckRequestOrigin(req) {
			http.Error(w, "forbidden origin", http.StatusForbidden)
			return
		}
		m.log.Infof("Incoming MCP request: %s %s", req.Method, req.URL.Path)
		protected.ServeHTTP(w, req)
	}))
}

func (m *MCPServer) StartMCPServer() error {
	if m.remote == nil || m.remote.httpServer == nil {
		return fmt.Errorf("MCP server is not configured for remote HTTP")
	}
	defer m.Close()
	r := m.remote
	m.log.Infof("MCP server listening on %s", r.addr)
	m.log.Infof("Protected resource metadata: %s", r.resourceMetadataURL)
	m.log.Infof("MCP endpoint: %s", r.resourceURL)

	var err error
	if r.httpServer.TLSConfig != nil {
		err = r.httpServer.ListenAndServeTLS("", "")
	} else {
		err = r.httpServer.ListenAndServe()
	}
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("MCP server error: %w", err)
	}
	return nil
}
