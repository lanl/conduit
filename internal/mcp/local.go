// Copyright 2026. Triad National Security, LLC. All rights reserved.

package mcp

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os/user"
	"strings"
	"time"

	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/internal/logger"
	mcpauth "github.com/modelcontextprotocol/go-sdk/auth"
	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/grpc"
)

// CreateLocalMCPServer shares all tool handlers with CreateMCPServer, but uses
// the exact gRPC client/connection prepared by the Conduit CLI (Kerberos by
// default, or a user-configured mTLS client). It takes ownership of conn on
// success; the caller must close conn if this constructor returns an error.
func CreateLocalMCPServer(log *logger.ConduitLogger, conn *grpc.ClientConn, client proto.ConduitApiClient) (*MCPServer, error) {
	account, err := user.Current()
	if err != nil {
		return nil, fmt.Errorf("failed to determine current OS user: %w", err)
	}
	username := strings.TrimSpace(account.Username)
	if username == "" {
		return nil, fmt.Errorf("current OS username is empty")
	}
	return newMCPServer(log, conn, client, username)
}

// StartLocalMCPStdio runs exactly one MCP session on stdin/stdout. It performs
// no MCP bearer-token authentication: SSH/process ownership is the frontend
// trust boundary. Nothing except MCP protocol messages may be written to stdout.
func (m *MCPServer) StartLocalMCPStdio(ctx context.Context) error {
	if m.localUser == "" || m.remote != nil {
		return fmt.Errorf("MCP server is not configured as local")
	}
	defer m.Close()
	return m.mcpServer.Run(ctx, &mcpsdk.StdioTransport{})
}

// StartLocalMCPHTTP exposes the same local tools over a loopback-only HTTP
// endpoint with a random per-process bearer token. It prints the endpoint and
// token to infoOut before blocking in Serve.
//
// Unlike stdio, HTTP does not use stdout for MCP protocol messages, so normal
// CLI output is permitted.
//
// The CLI process and its Conduit backend credentials must remain alive while
// clients use the HTTP endpoint. This mode is not intended for remote exposure.
func (m *MCPServer) StartLocalMCPHTTP(ctx context.Context, addr string, infoOut io.Writer) error {
	if m.localUser == "" || m.remote != nil {
		return fmt.Errorf("MCP server is not configured as local")
	}
	defer m.Close()
	if infoOut == nil {
		return fmt.Errorf("an output writer is required for local MCP connection information")
	}
	if addr == "" {
		addr = "127.0.0.1:0"
	}
	// Do not allow --addr :8080, 0.0.0.0:8080, or LAN addresses.
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("invalid MCP listen address: %w", err)
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("local MCP HTTP may only listen on a numeric loopback address, got %q", host)
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen for local MCP HTTP: %w", err)
	}
	defer listener.Close()

	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return fmt.Errorf("failed to generate local MCP bearer token: %w", err)
	}
	token := base64.RawURLEncoding.EncodeToString(secret)

	verifier := func(_ context.Context, presented string, _ *http.Request) (*mcpauth.TokenInfo, error) {
		if subtle.ConstantTimeCompare([]byte(presented), []byte(token)) != 1 {
			return nil, mcpauth.ErrInvalidToken
		}
		// The SDK requires a nonzero expiration. The random token is scoped
		// to this process and is revoked when the listener exits.
		return &mcpauth.TokenInfo{
			UserID:     m.localUser,
			Expiration: time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC),
		}, nil
	}

	mcpHandler := mcpsdk.NewStreamableHTTPHandler(func(*http.Request) *mcpsdk.Server {
		return m.mcpServer
	}, &mcpsdk.StreamableHTTPOptions{SessionTimeout: 30 * time.Minute})
	protected := mcpauth.RequireBearerToken(verifier, &mcpauth.RequireBearerTokenOptions{})(mcpHandler)

	mux := http.NewServeMux()
	mux.Handle(DefaultResourcePath, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		// Browser callers must originate from this same HTTP origin. Native MCP
		// clients normally omit Origin; SDK Host checks also guard rebinding.
		if origin := req.Header.Get("Origin"); origin != "" {
			u, err := url.Parse(origin)
			if err != nil || u.Scheme != "http" || u.Host != req.Host || u.User != nil || u.Path != "" {
				http.Error(w, "forbidden origin", http.StatusForbidden)
				return
			}
		}
		protected.ServeHTTP(w, req)
	}))

	httpServer := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 15 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	endpoint := "http://" + listener.Addr().String() + DefaultResourcePath
	if _, err := fmt.Fprintf(infoOut, "Local MCP endpoint: %s\nBearer token: %s\n", endpoint, token); err != nil {
		return fmt.Errorf("failed to print local MCP connection information: %w", err)
	}

	finished := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = httpServer.Shutdown(shutdownCtx)
		case <-finished:
		}
	}()

	err = httpServer.Serve(listener)
	close(finished)
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("local MCP HTTP server: %w", err)
	}
	return nil
}
