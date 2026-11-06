// Copyright 2026. Triad National Security, LLC. All rights reserved.

package mcp

import (
	"context"
	"fmt"
	"net/http"
	"time"

	conduitauth "github.com/lanl/conduit/internal/server/httpserver/auth"
	mcpauth "github.com/modelcontextprotocol/go-sdk/auth"
)

func (m *MCPServer) verifyToken(ctx context.Context, token string, _ *http.Request) (*mcpauth.TokenInfo, error) {
	if m.remote == nil || m.remote.validator == nil {
		return nil, mcpauth.ErrInvalidToken
	}

	p, err := m.remote.validator.ValidateBearerToken(ctx, token, conduitauth.ValidateOptions{})
	if err != nil {
		m.log.Errorf("MCP token validation failed: %v", err)
		return nil, fmt.Errorf("%w: %v", mcpauth.ErrInvalidToken, err)
	}
	if p.Username == "" {
		return nil, fmt.Errorf("%w: missing username", mcpauth.ErrInvalidToken)
	}

	exp := p.ExpiresAt
	if exp.IsZero() {
		exp = time.Now().Add(m.remote.tokenExpirationFallback)
	}

	return &mcpauth.TokenInfo{
		UserID:     p.Username,
		Scopes:     p.Scopes,
		Expiration: exp,
		Extra: map[string]any{
			"subject":   p.Subject,
			"client_id": p.ClientID,
			"audiences": p.Audiences,
			"claims":    p.Claims,
		},
	}, nil
}
