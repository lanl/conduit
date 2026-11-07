# Conduit MCP Server Operations Guide

Conduit exposes data-transfer tools through the [Model Context Protocol (MCP)](https://modelcontextprotocol.io/). This guide covers the **shared/remote MCP server**, deployed as the standalone `conduit-mcp` service for multiple users. MCP clients can connect directly to this service or through a LiteLLM MCP gateway.

For an MCP server started by an individual user with the Conduit CLI, see [Local MCP in the CLI guide](../usage/conduit-cli-usage.md#mcp---run-a-local-mcp-server).

## Deployment Models

| | Local MCP | Shared/remote MCP |
| --- | --- | --- |
| Command | `conduit mcp` | `conduit-mcp` |
| Intended use | Individual user or SSH-accessible host | Centrally deployed, multi-user service |
| MCP transport | stdio or loopback-only Streamable HTTP | Streamable HTTP |
| MCP client authentication | Process/SSH access (stdio) or a generated bearer token (HTTP) | OAuth/OIDC access token |
| Authentication to the Conduit gRPC server | User Kerberos ticket or user mTLS certificate | Service mTLS certificate |
| User identity | Determined by the Conduit backend from the user's credentials | Extracted from the validated OAuth token and forwarded to Conduit |

The local and shared servers expose the same MCP tools. **A local MCP server does not require OAuth, LiteLLM, or the standalone `conduit-mcp` service.**

The shared MCP server supports two client connection paths:

1. **Direct access:** The MCP client connects to `conduit-mcp` and authenticates with the configured OAuth/OIDC provider.
2. **LiteLLM gateway:** The MCP client connects to LiteLLM, which applies its own access controls and obtains or forwards a user OAuth token to `conduit-mcp`.

In both cases, **Conduit MCP validates the upstream OAuth access token**; LiteLLM does not replace this validation.

## Authentication and Identity

The shared MCP server uses two separate authentication connections:

```text
MCP client (direct or via LiteLLM)
    |
    | OAuth access token for Conduit MCP
    v
conduit-mcp
    | Validate token and extract username
    |
    | gRPC with service mTLS certificate
    | Request contains OAuth-authenticated username
    v
Conduit server
```

The service certificate must be authorized by the Conduit backend to make requests on behalf of users. Restrict access to this certificate and its private key.

### Required OAuth Audience

The shared MCP server requires `oauth.expected-audience`. It rejects access tokens that do not contain the configured audience in their validated audience claims. This prevents tokens issued for unrelated applications from being accepted by Conduit MCP.

Set `oauth.expected-audience` to the audience identifier that your OAuth/OIDC provider includes in access tokens intended for Conduit MCP. Depending on the provider, this may be an API identifier, resource URI, or application-specific identifier.

Configure MCP clients (or LiteLLM's upstream OAuth flow) to request an access token for that audience. Some providers use resource indicators or audience-specific scopes to do so. **The configured expected audience must match a value in the token's validated audience claims**; it is not necessarily the same as a scope string.

Audience validation is separate from user authorization. Configure appropriate permissions/scopes at the OAuth provider and enforce user-level access in Conduit. Existing deployments without an expected audience must configure one before upgrading to a release that requires it.

## Shared MCP Configuration

See the [full reference configuration](../configs/conduit-mcp-full-reference-config.yaml) for all supported settings.

```yaml
# Connection to the Conduit gRPC server
conduit:
  ip: conduit-server.example.com
  port: 23456
  ca: /path/to/conduit-ca.pem
  request-timeout: 30s

# Shared MCP HTTP listener and externally visible URL
server:
  ip: 0.0.0.0
  port: 8081
  public-url: "https://mcp.example.com"
  metadata-path: /.well-known/oauth-protected-resource
  http:
    allowed-origins:
      - "https://client1.example.com"
      - "https://client2.example.com"

# OAuth token validation
oauth:
  discovery-url: "https://auth.example.com/.well-known/openid-configuration"
  expected-audience: "conduit-mcp-audience"
  ca: /path/to/oauth-ca.pem
  username-claims:
    - preferred_username
    - email
  # Optional: introspection-auth-method: client_secret_basic

# Service identity used by conduit-mcp to connect to Conduit
client:
  cert: /path/to/service-client-cert.pem
  key: /path/to/service-client-key.pem

# Optional
debug: false
```

The OAuth provider's client credentials can be supplied through environment variables instead of embedding secrets in the YAML file:

```bash
CONDUIT_MCP_OAUTH_ISSUER=https://auth.example.com
CONDUIT_MCP_OAUTH_CLIENT_ID=your-client-id
CONDUIT_MCP_OAUTH_CLIENT_SECRET=your-client-secret
CONDUIT_MCP_OAUTH_INTROSPECTION_URL=https://auth.example.com/introspect
CONDUIT_MCP_OAUTH_USERINFO_URL=https://auth.example.com/userinfo
CONDUIT_MCP_OAUTH_EXPECTED_AUDIENCE=conduit-mcp-audience
CONDUIT_MCP_SERVER_PUBLIC_URL=https://mcp.example.com
```

Use your provider's actual issuer and endpoint URLs. With Conduit's Viper configuration, environment values override corresponding file settings.

Clients connect to the externally published endpoint:

```text
https://mcp.example.com/mcp
```

The configured `server.public-url` must match the public origin used for OAuth resource metadata. Serve the public endpoint over HTTPS, either with TLS configured on `conduit-mcp` or behind a trusted TLS-terminating reverse proxy. `allowed-origins` is a browser-origin restriction, **not** a replacement for authentication.

## Option 1: Direct MCP Access

In a direct deployment, the MCP client obtains an OAuth access token for Conduit MCP and sends it to the shared MCP endpoint.

![Direct MCP Access](../images/conduit-mcp.svg)

The MCP server validates that token using the configured identity provider, including the required audience, and uses the resulting username when calling the Conduit backend.

Use direct access when clients can reach `conduit-mcp` over HTTPS and can authenticate with its OAuth/OIDC provider without a gateway.

## Option 2: LiteLLM MCP Gateway

LiteLLM provides a centrally managed MCP entry point:

```text
MCP client -> LiteLLM -> conduit-mcp -> Conduit gRPC server
```

Clients connect to:

```text
https://litellm.example.com/conduit/mcp
```

LiteLLM controls which users, teams, or keys may access the Conduit MCP server. It also obtains or forwards the upstream OAuth token. The shared Conduit MCP service remains configured as described above and continues to validate the token and expected audience.

### LiteLLM Access Control

Require explicit permission for the Conduit MCP server:

```yaml
allow_all_keys: false
```

Grant access to the relevant LiteLLM users, teams, or keys. For example:

```json
{
  "object_permission": {
    "mcp_servers": ["conduit"]
  }
}
```

LiteLLM supports two relevant user-oriented OAuth approaches for this deployment.

### Delegated OAuth

Delegated OAuth is well suited to centrally managed deployments where clients support interactive OAuth and should not need to manage LiteLLM virtual keys themselves.

```yaml
mcp_servers:
  conduit:
    url: "https://mcp.internal.example.com/mcp"
    transport: "http"
    description: "Conduit MCP server"

    auth_type: oauth_delegate
    dcr_bridge: true

    allow_all_keys: false
```

With `dcr_bridge: true`, LiteLLM provides the OAuth discovery, registration, and login experience used by compatible MCP clients. Users authenticate with the configured LiteLLM login/SSO mechanism, and LiteLLM forwards a distinct upstream OAuth access token when calling Conduit MCP.

```text
MCP client
    |
    | OAuth discovery, registration, and login
    v
LiteLLM (user/team access control)
    |
    | Obtain/forward user's upstream OAuth token
    v
Conduit MCP (token + audience validation)
    |
    | Service mTLS + authenticated username
    v
Conduit server
```

LiteLLM's sign-in application and the OAuth client registration used to obtain a Conduit token can be separate registrations at the identity provider. The upstream token **must include Conduit's expected audience**. Configure the identity provider and LiteLLM's upstream OAuth flow to request a token for that audience.

For the bridge to work, LiteLLM must have browser login/SSO configured, and the client must support the required OAuth discovery/registration flow. See [LiteLLM MCP OAuth Passthrough](https://docs.litellm.ai/docs/mcp_oauth_passthrough) for the current delegated-flow and DCR bridge requirements.

#### Reverse Proxy Configuration

When LiteLLM is behind a reverse proxy, set its externally visible origin:

```bash
PROXY_BASE_URL=https://litellm.example.com
```

For permitted web-client callback origins outside that origin, configure:

```bash
MCP_TRUSTED_REDIRECT_ORIGINS=client.example.com
```

For example, a web-based client may use a callback like:

```text
https://client.example.com/oauth/clients/mcp:conduit/callback
```

Allow only known callback origins and configure trusted forwarded headers correctly at the reverse proxy.

### Authorization Code via LiteLLM

The authorization-code flow is useful when individual users have their own LiteLLM identities and virtual keys. LiteLLM manages the upstream OAuth consent flow and stores each user's token against their LiteLLM user identity.

```yaml
mcp_servers:
  conduit:
    url: "https://mcp.internal.example.com/mcp"
    transport: "http"
    description: "Conduit MCP server"

    auth_type: oauth2
    oauth2_flow: authorization_code
    per_server_oauth_discovery: true

    client_id: os.environ/CONDUIT_OAUTH_CLIENT_ID
    client_secret: os.environ/CONDUIT_OAUTH_CLIENT_SECRET

    authorization_url: "https://auth.example.com/authorize"
    token_url: "https://auth.example.com/token"

    scopes:
      - openid
      - profile
      - email
      - offline_access
      # Add any provider-specific audience scope if required to obtain
      # an access token containing Conduit's expected audience.

    allow_all_keys: false
```

Register LiteLLM's redirect URI with the OAuth provider:

```text
https://litellm.example.com/callback
```

When `per_server_oauth_discovery: true` is used, clients identify their LiteLLM user with a virtual key on the dedicated header:

```text
x-litellm-api-key: Bearer <LiteLLM virtual key>
```

Keep the standard `Authorization` header available for the upstream OAuth flow. In cases where the client explicitly supplies both credentials, use separate headers:

```text
x-litellm-api-key: Bearer <LiteLLM virtual key>
Authorization: Bearer <OAuth access token>
```

Do not share a user-owned LiteLLM virtual key among multiple users: stored OAuth credentials are associated with the LiteLLM user's identity. Ensure the upstream OAuth scopes request Conduit's expected audience.

For current gateway behavior and client requirements, see [LiteLLM MCP OAuth](https://docs.litellm.ai/docs/mcp_oauth).

## Running the Shared MCP Server

### Prerequisites

- A running, reachable Conduit gRPC server.
- An OAuth/OIDC provider that issues access tokens for Conduit MCP, with a configured expected audience.
- A service mTLS certificate authorized for the intended backend operations. See [Generating Certificates](cert-generation.md).
- An HTTPS endpoint available to MCP clients, directly or through a reverse proxy.

### Command Line

```bash
conduit-mcp --config /path/to/conduit-mcp-config.yaml

# Debug logging
conduit-mcp --config /path/to/conduit-mcp-config.yaml --debug
```

### systemd

```ini
[Unit]
Description=Conduit MCP Server
Documentation=https://github.com/lanl/conduit
After=network-online.target local-fs.target remote-fs.target time-sync.target
Wants=network-online.target local-fs.target remote-fs.target time-sync.target

[Service]
Type=simple
ExecStart=/usr/local/bin/conduit-mcp --config /etc/conduit/conduit-mcp-config.yaml
Restart=on-failure
RestartSec=5s
KillMode=mixed
KillSignal=SIGTERM
TimeoutStopSec=5h
SendSIGKILL=yes

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now conduit-mcp
sudo systemctl status conduit-mcp
```

### Docker

Build the container image:

```bash
docker build -f docker/Dockerfile.mcp -t conduit-mcp:latest .
```

Run it with the configuration and certificates mounted read-only:

```bash
docker run -d \
  --name conduit-mcp \
  -p 8081:8081 \
  -v /path/to/conduit-mcp-config.yaml:/etc/conduit/conduit-mcp-config.yaml:ro \
  -v /path/to/certs:/etc/conduit/keys:ro \
  conduit-mcp:latest
```
