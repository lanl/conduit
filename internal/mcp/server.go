// Copyright 2026. Triad National Security, LLC. All rights reserved.

package mcp

import (
	"context"
	"fmt"
	"strings"

	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/internal/logger"
	mcpauth "github.com/modelcontextprotocol/go-sdk/auth"
	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/grpc"
)

// MCPServer contains the shared MCP tools and their Conduit backend.
// The remote and local constructors configure authentication and transport.
type MCPServer struct {
	log       *logger.ConduitLogger
	mcpServer *mcpsdk.Server

	conduitClient     proto.ConduitApiClient
	conduitClientConn *grpc.ClientConn

	// Empty for remote OAuth; populated from the operating-system account
	// for a locally started MCP server (including stdio over SSH).
	localUser string

	// Transport-specific state is declared in remote.go.
	remote *remoteConfig
}

// newMCPServer is shared by the remote and local constructors.
// Ownership of conn is transferred to the MCPServer on success.
func newMCPServer(log *logger.ConduitLogger, conn *grpc.ClientConn, client proto.ConduitApiClient, localUser string) (*MCPServer, error) {
	if log == nil {
		return nil, fmt.Errorf("MCP logger is required")
	}
	if conn == nil || client == nil {
		return nil, fmt.Errorf("MCP Conduit backend connection and client are required")
	}

	prefix := log.GetPrefix()
	if prefix == "" {
		prefix = "MCP server:"
	} else {
		prefix += "MCP server:"
	}

	m := &MCPServer{
		log:               logger.NewConduitLogger(log.GetLevel(), prefix),
		mcpServer:         mcpsdk.NewServer(&mcpsdk.Implementation{Name: "conduit-mcp-server", Version: "0.1.0"}, nil),
		conduitClient:     client,
		conduitClientConn: conn,
		localUser:         localUser,
	}
	if err := m.registerTools(); err != nil {
		return nil, err
	}
	return m, nil
}

// requestUsername resolves the identity used in Conduit API requests.
// Locally it is fixed to the operating-system account, never supplied by
// an MCP tool parameter. Remotely it is the OAuth-authenticated user.
func (m *MCPServer) requestUsername(ctx context.Context, req *mcpsdk.CallToolRequest) (string, error) {
	if m.localUser != "" {
		return m.localUser, nil
	}

	// The MCP SDK attaches token information to each tool request.
	if req != nil && req.Extra != nil && req.Extra.TokenInfo != nil {
		if username := strings.TrimSpace(req.Extra.TokenInfo.UserID); username != "" {
			return username, nil
		}
	}

	// Also support the context populated by the OAuth HTTP middleware.
	if info := mcpauth.TokenInfoFromContext(ctx); info != nil {
		if username := strings.TrimSpace(info.UserID); username != "" {
			return username, nil
		}
	}
	return "", fmt.Errorf("no authenticated user provided")
}

// Close releases the backend gRPC connection owned by this MCP server.
func (m *MCPServer) Close() error {
	if m.conduitClientConn == nil {
		return nil
	}
	return m.conduitClientConn.Close()
}
