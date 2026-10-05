#!/bin/sh
set -eu

MCP_DOCKER_PATH="/etc/conduit-mcp"
SCRIPT_DIR=$(cd -- "$(dirname -- "$0")" >/dev/null 2>&1 && pwd)

ZITADEL_URL="${ZITADEL_URL:-https://zitadel.home.arpa}"
API="${ZITADEL_URL}/management/v1"
PAT_FILE="$MCP_DOCKER_PATH/zitadel/bootstrap/admin.pat"
STATE_FILE="$MCP_DOCKER_PATH/generated/zitadel-bootstrap-state.env"
CADDY_ROOT_CA="${CADDY_ROOT_CA:-$MCP_DOCKER_PATH/caddy/caddy_data/caddy/pki/authorities/local/root.crt}"

DEMO_USERNAME="testuser"
DEMO_EMAIL="testuser@example.com"
DEMO_PASSWORD="password"

ADMIN_USERNAME="${ADMIN_USERNAME:-admin}"
ADMIN_EMAIL="${ADMIN_EMAIL:-admin@example.com}"
ADMIN_PASSWORD="${ADMIN_PASSWORD:-qMRW%g*qi3sZE65x6wiK}"

MCP_SETUP_FILE="$MCP_DOCKER_PATH/generated/openwebui-mcp-setup.txt"

MCP_CONNECTION_ID="conduit-mcp"
MCP_SERVER_URL="https://mcp.home.arpa/mcp"
OPENWEBUI_URL="https://openwebui.home.arpa"

if [ -f "$STATE_FILE" ]; then
	echo "ZITADEL already bootstrapped: $STATE_FILE"
	exit 0
fi

if [ ! -s "$PAT_FILE" ]; then
	echo "Missing ZITADEL bootstrap PAT at $PAT_FILE" >&2
	exit 1
fi

PAT="$(cat "$PAT_FILE")"

need() {
	command -v "$1" >/dev/null 2>&1 || {
		echo "Missing required command: $1" >&2
		exit 1
	}
}

need curl
need jq
need mktemp

mkdir -p "$MCP_DOCKER_PATH/generated"

curl_with_ca() {
	if [ -s "$CADDY_ROOT_CA" ]; then
		curl --cacert "$CADDY_ROOT_CA" "$@"
	else
		curl "$@"
	fi
}

api_request_url() {
	method="$1"
	url="$2"
	body="$3"

	tmp_body="$(mktemp)"
	http_code=""

	if ! http_code="$(curl_with_ca -sS -o "$tmp_body" -w "%{http_code}" \
		-X "$method" "$url" \
		-H "Authorization: Bearer ${PAT}" \
		-H "Content-Type: application/json" \
		-d "$body")"; then
		echo "ZITADEL API request failed: $method $url" >&2
		echo "Response body:" >&2
		cat "$tmp_body" >&2 || true
		rm -f "$tmp_body"
		exit 1
	fi

	case "$http_code" in
		2*)
			cat "$tmp_body"
			rm -f "$tmp_body"
			;;
		*)
			echo "ZITADEL API returned HTTP $http_code: $method $url" >&2
			echo "Request body:" >&2
			printf '%s\n' "$body" >&2
			echo "Response body:" >&2
			cat "$tmp_body" >&2 || true
			rm -f "$tmp_body"
			exit 1
			;;
	esac
}

api_request_url_optional() {
	method="$1"
	url="$2"
	body="$3"

	tmp_body="$(mktemp)"
	http_code=""

	if ! http_code="$(curl_with_ca -sS -o "$tmp_body" -w "%{http_code}" \
		-X "$method" "$url" \
		-H "Authorization: Bearer ${PAT}" \
		-H "Content-Type: application/json" \
		-d "$body")"; then
		rm -f "$tmp_body"
		return 1
	fi

	case "$http_code" in
		2*)
			cat "$tmp_body"
			rm -f "$tmp_body"
			return 0
			;;
		*)
			echo "Optional API call failed with HTTP $http_code: $method $url" >&2
			echo "Response body:" >&2
			cat "$tmp_body" >&2 || true
			rm -f "$tmp_body"
			return 1
			;;
	esac
}

api_post() {
	path="$1"
	body="$2"
	api_request_url "POST" "${API}${path}" "$body"
}

relax_demo_password_policy() {
	body="$(jq -n '{
	  minLength: 8,
	  hasUppercase: false,
	  hasLowercase: true,
	  hasNumber: false,
	  hasSymbol: false
	}')"

	echo "Relaxing demo password policy so testuser/password works..."

	if api_request_url_optional "PUT" "${ZITADEL_URL}/policies/password/complexity" "$body" >/dev/null; then
		return 0
	fi

	api_request_url "PUT" "${ZITADEL_URL}/admin/v1/policies/password/complexity" "$body" >/dev/null
}

json_field() {
	json="$1"
	field="$2"
	value="$(printf '%s' "$json" | jq -r "$field")"

	if [ -z "$value" ] || [ "$value" = "null" ]; then
		echo "Expected JSON field missing: $field" >&2
		echo "JSON was:" >&2
		printf '%s\n' "$json" >&2
		exit 1
	fi

	printf '%s\n' "$value"
}

echo "Using ZITADEL URL: $ZITADEL_URL"
if [ -s "$CADDY_ROOT_CA" ]; then
	echo "Using Caddy root CA: $CADDY_ROOT_CA"
else
	echo "Caddy root CA not found at $CADDY_ROOT_CA; relying on system trust store."
fi

relax_demo_password_policy

echo "Creating ZITADEL project..."
PROJECT_BODY="$(jq -n '{
  name: "conduit-lab",
  projectRoleAssertion: true,
  projectRoleCheck: false,
  hasProjectCheck: false,
  privateLabelingSetting: "PRIVATE_LABELING_SETTING_UNSPECIFIED"
}')"
PROJECT_JSON="$(api_post "/projects" "$PROJECT_BODY")"
PROJECT_ID="$(json_field "$PROJECT_JSON" '.id')"

echo "Creating Open WebUI OIDC app..."
OPENWEBUI_BODY="$(jq -n '{
  name: "openwebui",
  redirectUris: ["https://openwebui.home.arpa/oauth/oidc/callback"],
  responseTypes: ["OIDC_RESPONSE_TYPE_CODE"],
  grantTypes: [
    "OIDC_GRANT_TYPE_AUTHORIZATION_CODE",
    "OIDC_GRANT_TYPE_REFRESH_TOKEN"
  ],
  appType: "OIDC_APP_TYPE_WEB",
  authMethodType: "OIDC_AUTH_METHOD_TYPE_BASIC",
  version: "OIDC_VERSION_1_0",
  devMode: false,
  accessTokenType: "OIDC_TOKEN_TYPE_BEARER",
  accessTokenRoleAssertion: true,
  idTokenRoleAssertion: true,
  idTokenUserinfoAssertion: true,
  clockSkew: "0s"
}')"
OPENWEBUI_JSON="$(api_post "/projects/${PROJECT_ID}/apps/oidc" "$OPENWEBUI_BODY")"
OPENWEBUI_CLIENT_ID="$(json_field "$OPENWEBUI_JSON" '.clientId')"
OPENWEBUI_CLIENT_SECRET="$(json_field "$OPENWEBUI_JSON" '.clientSecret')"

MCP_CALLBACK_URI="${OPENWEBUI_URL}/oauth/clients/mcp:${MCP_CONNECTION_ID}/callback"

echo "Creating Open WebUI Conduit MCP OIDC app..."
MCP_OIDC_BODY="$(jq -n \
  --arg redirect_uri "$MCP_CALLBACK_URI" \
  '{
    name: "openwebui-conduit-mcp",
    redirectUris: [$redirect_uri],
    responseTypes: ["OIDC_RESPONSE_TYPE_CODE"],
    grantTypes: [
      "OIDC_GRANT_TYPE_AUTHORIZATION_CODE",
      "OIDC_GRANT_TYPE_REFRESH_TOKEN"
    ],
    appType: "OIDC_APP_TYPE_WEB",
    authMethodType: "OIDC_AUTH_METHOD_TYPE_BASIC",
    version: "OIDC_VERSION_1_0",
    devMode: false,
    accessTokenType: "OIDC_TOKEN_TYPE_BEARER",
    accessTokenRoleAssertion: true,
    idTokenRoleAssertion: false,
    idTokenUserinfoAssertion: true,
    clockSkew: "0s"
  }')"

MCP_OIDC_JSON="$(
  api_post "/projects/${PROJECT_ID}/apps/oidc" "$MCP_OIDC_BODY"
)"
MCP_OAUTH_CLIENT_ID="$(json_field "$MCP_OIDC_JSON" '.clientId')"
MCP_OAUTH_CLIENT_SECRET="$(json_field "$MCP_OIDC_JSON" '.clientSecret')"

echo "Creating Conduit MCP API app..."
CONDUIT_BODY="$(jq -n '{
  name: "conduit-mcp",
  authMethodType: "API_AUTH_METHOD_TYPE_BASIC"
}')"
CONDUIT_JSON="$(api_post "/projects/${PROJECT_ID}/apps/api" "$CONDUIT_BODY")"
CONDUIT_CLIENT_ID="$(json_field "$CONDUIT_JSON" '.clientId')"
CONDUIT_CLIENT_SECRET="$(json_field "$CONDUIT_JSON" '.clientSecret')"

echo "Creating demo user ${DEMO_USERNAME}..."
USER_BODY="$(jq -n \
  --arg username "$DEMO_USERNAME" \
  --arg email "$DEMO_EMAIL" \
  --arg password "$DEMO_PASSWORD" \
  '{
    userName: $username,
    profile: {
      firstName: "Test",
      lastName: "User",
      nickName: "testuser",
      displayName: "Test User",
      preferredLanguage: "en",
      gender: "GENDER_UNSPECIFIED"
    },
    email: {
      email: $email,
      isEmailVerified: true
    },
    initialPassword: $password
  }')"
USER_JSON="$(api_post "/users/human" "$USER_BODY")"
DEMO_USER_ID="$(json_field "$USER_JSON" '.userId')"

AUD_SCOPE="urn:zitadel:iam:org:project:id:${PROJECT_ID}:aud"

umask 077

cat > "$MCP_DOCKER_PATH/generated/openwebui.env" <<EOF
ENABLE_OAUTH_SIGNUP=true
OAUTH_CLIENT_ID=${OPENWEBUI_CLIENT_ID}
OAUTH_CLIENT_SECRET=${OPENWEBUI_CLIENT_SECRET}
OAUTH_PROVIDER_NAME=ZITADEL
OPENID_PROVIDER_URL=https://zitadel.home.arpa/.well-known/openid-configuration
OPENID_REDIRECT_URI=https://openwebui.home.arpa/oauth/oidc/callback
OAUTH_MERGE_ACCOUNTS_BY_EMAIL=true
OAUTH_SCOPES=openid email profile offline_access ${AUD_SCOPE}
EOF

cat > "$MCP_DOCKER_PATH/generated/conduit-mcp.env" <<EOF
CONDUIT_MCP_OAUTH_CLIENT_ID=${CONDUIT_CLIENT_ID}
CONDUIT_MCP_OAUTH_CLIENT_SECRET=${CONDUIT_CLIENT_SECRET}
CONDUIT_MCP_OAUTH_ISSUER=${ZITADEL_URL}
CONDUIT_MCP_OAUTH_INTROSPECTION_URL=${ZITADEL_URL}/oauth/v2/introspect
CONDUIT_MCP_OAUTH_USERINFO_URL=${ZITADEL_URL}/oidc/v1/userinfo
CONDUIT_MCP_OAUTH_EXPECTED_AUDIENCE=${PROJECT_ID}
EOF

cat > "$STATE_FILE" <<EOF
ZITADEL_PROJECT_ID=${PROJECT_ID}
ZITADEL_AUDIENCE_SCOPE=${AUD_SCOPE}
OPENWEBUI_CLIENT_ID=${OPENWEBUI_CLIENT_ID}
CONDUIT_CLIENT_ID=${CONDUIT_CLIENT_ID}
MCP_OAUTH_CLIENT_ID=${MCP_OAUTH_CLIENT_ID}
MCP_OAUTH_CLIENT_SECRET=${MCP_OAUTH_CLIENT_SECRET}
MCP_CONNECTION_ID=${MCP_CONNECTION_ID}
DEMO_USER_ID=${DEMO_USER_ID}
DEMO_USERNAME=${DEMO_USERNAME}
DEMO_EMAIL=${DEMO_EMAIL}
DEMO_PASSWORD=${DEMO_PASSWORD}
EOF

cat > "$MCP_SETUP_FILE" <<EOF
Open WebUI Conduit MCP configuration
=====================================

Open WebUI:

  URL:      ${OPENWEBUI_URL}
  Username: ${ADMIN_EMAIL}
  Password: ${ADMIN_PASSWORD}

Log in as the Open WebUI administrator, then add an external tool server using
these values:

  Type:                     MCP (Streamable HTTP)
  ID:                       ${MCP_CONNECTION_ID}
  Name:                     Conduit MCP
  URL:                      ${MCP_SERVER_URL}
  Authentication:           OAuth 2.1 (Static)
  OAuth Client ID:          ${MCP_OAUTH_CLIENT_ID}
  OAuth Client Secret:      ${MCP_OAUTH_CLIENT_SECRET}
  OAuth Server URL:         ${ZITADEL_URL}
  OAuth Scopes:             openid profile email offline_access ${AUD_SCOPE}
  OAuth Resource Parameter: Automatic

IMPORTANT:

  The ID must be exactly:

    ${MCP_CONNECTION_ID}

  Open WebUI uses the ID to construct this OAuth callback:

    ${MCP_CALLBACK_URI}

  After entering the values:

    1. Click Register Client.
    2. Save the MCP server.
    3. Sign out of the administrator account.
    4. Sign in as the test user.
    5. Enable the Conduit MCP server for the test user.

Test user login:

  Username: ${DEMO_USERNAME}
  Password: ${DEMO_PASSWORD}
EOF

chmod 600 \
	"$MCP_DOCKER_PATH/generated/"*.env \
	"$STATE_FILE" \
	"$MCP_SETUP_FILE"

echo
echo "ZITADEL bootstrap complete."
echo "Demo login:"
echo "  username: ${DEMO_USERNAME}"
echo "  password: ${DEMO_PASSWORD}"
echo
echo "State written to:"
echo "  $STATE_FILE"
