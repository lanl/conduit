#!/bin/sh
set -eu

if [ "$(id -u)" -ne 0 ]; then
	echo "Please run as root." >&2
	exit 1
fi

MCP_DOCKER_PATH="/etc/conduit-mcp"
SCRIPT_DIR=$(cd -- "$(dirname -- "$0")" >/dev/null 2>&1 && pwd)
echo "SCRIPT_DIR:$SCRIPT_DIR"

. "${SCRIPT_DIR}/vars.sh"

ROOT_CA="$MCP_DOCKER_PATH/caddy/caddy_data/caddy/pki/authorities/local/root.crt"
SYSTEM_CA="/usr/local/share/ca-certificates/conduit-caddy-root.crt"
CONTAINER_CA_BUNDLE="$MCP_DOCKER_PATH/openwebui-ca-bundle.pem"

wait_for_file() {
	file="$1"
	i=0
	while [ ! -s "$file" ]; do
		i=$((i + 1))
		if [ "$i" -gt 120 ]; then
			echo "Timed out waiting for $file" >&2
			exit 1
		fi
		sleep 1
	done
}

wait_for_url() {
	url="$1"
	i=0
	while ! curl -fsS "$url" >/dev/null 2>&1; do
		i=$((i + 1))
		if [ "$i" -gt 120 ]; then
			echo "Timed out waiting for $url" >&2
			exit 1
		fi
		sleep 1
	done
}

install_caddy_root_ca() {
	wait_for_file "$ROOT_CA"

	echo "Installing Caddy internal root CA into host trust store..."

	if command -v update-ca-certificates >/dev/null 2>&1; then
		cp "$ROOT_CA" "$SYSTEM_CA"
		chmod 0644 "$SYSTEM_CA"
		update-ca-certificates
	elif command -v update-ca-trust >/dev/null 2>&1; then
		cp "$ROOT_CA" /etc/pki/ca-trust/source/anchors/conduit-caddy-root.crt
		chmod 0644 /etc/pki/ca-trust/source/anchors/conduit-caddy-root.crt
		update-ca-trust extract
	else
		echo "No known CA update command found." >&2
		echo "Install ca-certificates, or configure bootstrap-zitadel.sh to use --cacert $ROOT_CA." >&2
		exit 1
	fi

	echo "Building container CA bundle..."
	cp /etc/ssl/certs/ca-certificates.crt "$CONTAINER_CA_BUNDLE"
	cat "$ROOT_CA" >> "$CONTAINER_CA_BUNDLE"
	chmod 0644 "$CONTAINER_CA_BUNDLE"
}

cd "$SCRIPT_DIR"

# Start Caddy first so its internal CA root exists.
$DOCKER_COMPOSE up -d --force-recreate caddy

install_caddy_root_ca

# Start only ZITADEL first. The other services need generated client IDs/secrets.
$DOCKER_COMPOSE up -d --force-recreate zitadel-db zitadel zitadel-login

wait_for_file "$MCP_DOCKER_PATH/zitadel/bootstrap/admin.pat"
wait_for_url "https://zitadel.home.arpa/.well-known/openid-configuration"

"${SCRIPT_DIR}/bootstrap-zitadel.sh"

wait_for_url "https://zitadel.home.arpa/.well-known/openid-configuration"

# Start downstream services. Do not recreate caddy/zitadel here.
$DOCKER_COMPOSE up -d --force-recreate conduit-mcp
$DOCKER_COMPOSE up -d --force-recreate openwebui

MCP_SETUP_FILE="/etc/conduit-mcp/generated/openwebui-mcp-setup.txt"

echo
if [ -s "$MCP_SETUP_FILE" ]; then
	cat "$MCP_SETUP_FILE"
else
	echo "Open WebUI MCP setup information was not generated." >&2
	echo "Expected file: $MCP_SETUP_FILE" >&2
fi
echo
