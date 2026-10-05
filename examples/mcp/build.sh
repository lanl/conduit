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

mkdir -p "$MCP_DOCKER_PATH"
mkdir -p "$MCP_DOCKER_PATH/zitadel/postgres"
mkdir -p "$MCP_DOCKER_PATH/zitadel/bootstrap"
mkdir -p "$MCP_DOCKER_PATH/generated"

# Compose env_file entries must exist before docker compose reads the file.
touch "$MCP_DOCKER_PATH/generated/openwebui.env"
touch "$MCP_DOCKER_PATH/generated/conduit-mcp.env"

chmod 700 "$MCP_DOCKER_PATH/generated"
chmod 600 "$MCP_DOCKER_PATH/generated/"*.env

# Generate a stable stack .env only once.
if [ ! -f "${SCRIPT_DIR}/.env" ]; then
	umask 077
	cat > "${SCRIPT_DIR}/.env" <<EOF
ZITADEL_VERSION=v4.15.1
ZITADEL_MASTERKEY=$(tr -dc A-Za-z0-9 </dev/urandom | head -c 32)
ZITADEL_POSTGRES_PASSWORD=$(tr -dc A-Za-z0-9 </dev/urandom | head -c 32)
ZITADEL_ADMIN_USERNAME=admin
ZITADEL_ADMIN_EMAIL=admin@example.com
ZITADEL_ADMIN_PASSWORD=$(tr -dc A-Za-z0-9 </dev/urandom | head -c 24)
EOF
fi

cd "$SCRIPT_DIR"
$DOCKER_COMPOSE build