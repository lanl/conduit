#!/bin/sh
set -e

if [ $(id -u) -ne 0 ]; then
	echo "Please run as root." >&2
	exit 1
fi

CONDUIT_PATH="/etc/conduit"

# get path of script
SCRIPT_DIR=$( cd -- "$( dirname -- "$0" )" &> /dev/null && pwd )
echo SCRIPT_DIR:$SCRIPT_DIR

# Get "global" variables
. "${SCRIPT_DIR}"/vars.sh

# create conduit area
mkdir -p $CONDUIT_PATH
mkdir -p $CONDUIT_PATH/keys
mkdir -p $CONDUIT_PATH/ssh
mkdir -p $CONDUIT_PATH/conduit_fs_1
mkdir -p $CONDUIT_PATH/conduit_fs_2
mkdir -p $CONDUIT_PATH/etcd-1
mkdir -p $CONDUIT_PATH/etcd-2
mkdir -p $CONDUIT_PATH/etcd-3
chmod 700 $CONDUIT_PATH/etcd-*
mkdir -p $CONDUIT_PATH/rqlite-1
mkdir -p $CONDUIT_PATH/rqlite-2
mkdir -p $CONDUIT_PATH/rqlite-3
chown 1000:1000 $CONDUIT_PATH/rqlite-*

# create example fs directories
mkdir -p $CONDUIT_PATH/conduit_fs_1/conduit-staging-area/stage-in
mkdir -p $CONDUIT_PATH/conduit_fs_1/conduit-staging-area/stage-out
mkdir -p $CONDUIT_PATH/conduit_fs_1/conduit-trash
mkdir -p $CONDUIT_PATH/conduit_fs_2/conduit-staging-area/stage-in
mkdir -p $CONDUIT_PATH/conduit_fs_2/conduit-staging-area/stage-out
mkdir -p $CONDUIT_PATH/conduit_fs_2/conduit-trash
mkdir -p $CONDUIT_PATH/conduit_fs_1/foo
mkdir -p $CONDUIT_PATH/conduit_fs_2/bar
echo 'hello' > $CONDUIT_PATH/conduit_fs_1/foo/hello.txt

# generate ssh keys for testuser
mkdir -p $CONDUIT_PATH/ssh/testuser
rm -f $CONDUIT_PATH/ssh/testuser/id_rsa
rm -f $CONDUIT_PATH/ssh/testuser/id_rsa.pub
ssh-keygen -f $CONDUIT_PATH/ssh/testuser/id_rsa -C testuser@conduit_example -q -N ""
cat $CONDUIT_PATH/ssh/testuser/id_rsa.pub > $CONDUIT_PATH/ssh/testuser/authorized_keys
chmod 600 $CONDUIT_PATH/ssh/testuser/authorized_keys
chown -R 1433:1433 $CONDUIT_PATH/ssh/testuser

# generate ssh host keys for the ftas
mkdir -p $CONDUIT_PATH/ssh/host
rm -f $CONDUIT_PATH/ssh/host/ssh_host_ecdsa_key
rm -f $CONDUIT_PATH/ssh/host/ssh_host_ecdsa_key.pub
ssh-keygen -t ecdsa -f $CONDUIT_PATH/ssh/host/ssh_host_ecdsa_key -C "" -q -N ""
printf "fta1,192.168.20.6 $(cat $CONDUIT_PATH/ssh/host/ssh_host_ecdsa_key.pub)\nfta2,192.168.20.7 $(cat $CONDUIT_PATH/ssh/host/ssh_host_ecdsa_key.pub)\n" > $CONDUIT_PATH/ssh/testuser/known_hosts
chmod 644 $CONDUIT_PATH/ssh/testuser/known_hosts

# Check for Docker
$DOCKER --version || echo "Docker not found: '$DOCKER'" >&2
# Check for go
go version || echo "Go not found" >&2

cd ../../
go build -buildvcs=false -o bin/conduit-server ./cmd/server
cd $SCRIPT_DIR

# generate CAs for conduit
echo internal-ca:
../../bin/conduit-server internal-ca -d \
    --internal-ca-cert $CONDUIT_PATH/keys/conduit_internal_ca.pem \
    --internal-ca-key $CONDUIT_PATH/keys/conduit_internal_key.pem \
    --external-ca-cert $CONDUIT_PATH/keys/conduit_external_ca.pem \
    --external-ca-key $CONDUIT_PATH/keys/conduit_external_key.pem \
    --keytab $CONDUIT_PATH/keys/conduit.keytab \
    --hostname conduit-server.example.com \
    --ip 192.168.20.3 \
    --port 23456 \
    --etcd-ip 192.168.20.21,192.168.20.22,192.168.20.23 \
    --etcd-port 2379,2379,2379 \
    --etcd-hostname etcd-1.example.com,etcd-2.example.com,etcd-3.example.com \

echo external-ca:
../../bin/conduit-server external-ca -d \
    --internal-ca-cert $CONDUIT_PATH/keys/conduit_internal_ca.pem \
    --internal-ca-key $CONDUIT_PATH/keys/conduit_internal_key.pem \
    --external-ca-cert $CONDUIT_PATH/keys/conduit_external_ca.pem \
    --external-ca-key $CONDUIT_PATH/keys/conduit_external_key.pem \
    --keytab $CONDUIT_PATH/keys/conduit.keytab \
    --hostname conduit-server.example.com \
    --ip 192.168.20.3,172.18.0.10 \
    --port 23456 \
    --etcd-ip 192.168.20.21,192.168.20.22,192.168.20.23 \
    --etcd-port 2379,2379,2379 \
    --etcd-hostname etcd-1.example.com,etcd-2.example.com,etcd-3.example.com \

echo internal-server-cert-etcd:
../../bin/conduit-server internal-server-cert -d \
    --separate-cert-key \
    --cert-name etcd_server_cert.pem \
    --key-name etcd_server_key.pem \
    --output $CONDUIT_PATH/keys/ \
    --server-ip 192.168.20.21,192.168.20.22,192.168.20.23 \
    --server-hostname etcd-1.example.com,etcd-2.example.com,etcd-3.example.com

echo internal-server-cert-rqlite:
../../bin/conduit-server internal-server-cert -d \
    --separate-cert-key \
    --cert-name rqlite_server_cert.pem \
    --key-name rqlite_server_key.pem \
    --output $CONDUIT_PATH/keys/ \
    --server-ip 192.168.20.31,192.168.20.32,192.168.20.33 \
    --server-hostname rqlite-1.example.com,rqlite-2.example.com,rqlite-3.example.com

chown 1000:1000 -R \
	"${CONDUIT_PATH}"/keys/rqlite_server_cert.pem \
	"${CONDUIT_PATH}"/keys/rqlite_server_key.pem \
	"${CONDUIT_PATH}"/keys/rqlite_server_key.pem \

echo external-client-cert-dws:
../../bin/conduit-server external-client-cert -d \
    --separate-cert-key \
    --cert-name conduit_dws_cert.pem \
    --key-name conduit_dws_key.pem \
    --output $CONDUIT_PATH/keys/ \
    --client-commonname conduit-dws \
    --expiration 365

echo external-client-cert-admin:
../../bin/conduit-server external-client-cert -d \
    --separate-cert-key \
    --cert-name conduit_admin_cert.pem \
    --key-name conduit_admin_key.pem \
    --output $CONDUIT_PATH/keys/ \
    --client-commonname conduit-admin \
    --expiration 365

echo etcd-client-cert:
../../bin/conduit-server internal-client-cert -d \
    --separate-cert-key \
    --cert-name etcd_client_cert.pem \
    --key-name etcd_client_key.pem \
    --output $CONDUIT_PATH/keys/ \
    --client-commonname root \
    --expiration 365

# build pftool image
$DOCKER build \
	-t pftool \
	-f $SCRIPT_DIR/Dockerfile.pftool \
	$SCRIPT_DIR

# build conduit images
cd $SCRIPT_DIR/../../
echo conduit-cli:
$DOCKER build \
	-f docker/Dockerfile.cli \
	-t conduit-cli:latest \
	.
echo conduit-fta:
$DOCKER build \
	-f docker/Dockerfile.fta \
	-t conduit-fta:latest \
	.

echo conduit-runner:
$DOCKER build \
	-f docker/Dockerfile.runner \
	-t conduit-runner:latest \
	.


# cd $SCRIPT_DIR/../../
# echo conduit:
# docker build -f Dockerfile -t conduit:latest .
# go build
# cd $SCRIPT_DIR

# build and run conduit example images
cd $SCRIPT_DIR
$DOCKER_COMPOSE build

# Modify conduit-staging-area ownership
chown 1433:1433 -R \
	"${CONDUIT_PATH}"/conduit_fs_1/foo \
	"${CONDUIT_PATH}"/conduit_fs_2/bar
chmod 777 \
	"${CONDUIT_PATH}"/conduit_fs_1/conduit-staging-area/stage-in \
	"${CONDUIT_PATH}"/conduit_fs_1/conduit-staging-area/stage-out \
	"${CONDUIT_PATH}"/conduit_fs_1/conduit-trash \
	"${CONDUIT_PATH}"/conduit_fs_2/conduit-staging-area/stage-in \
	"${CONDUIT_PATH}"/conduit_fs_2/conduit-staging-area/stage-out \
	"${CONDUIT_PATH}"/conduit_fs_2/conduit-trash

