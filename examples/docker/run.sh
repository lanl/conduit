#!/bin/sh
#set -e

CONDUIT_PATH="/etc/conduit"

# get path of script
SCRIPT_DIR=$( cd -- "$( dirname -- "$0" )" &> /dev/null && pwd )
echo SCRIPT_DIR:$SCRIPT_DIR

# Get "global" variables
. "${SCRIPT_DIR}"/vars.sh

# build and run conduit example images
cd $SCRIPT_DIR
$DOCKER_COMPOSE up -d --force-recreate kdc etcd-1 etcd-2 etcd-3 fta1 fta2 rqlite-1 rqlite-2 rqlite-3 conduit-server ldap
# Get shell in client
$DOCKER_COMPOSE run --rm --name conduit_client client
