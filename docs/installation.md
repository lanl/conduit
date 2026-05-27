# CONDUIT Installation

This installation document will guide you through installing conduit and it's dependent components onto a machine without the use of docker containers

## Build Conduit

### Dependencies

1. git
2. [go](https://go.dev/doc/install)

### Build Instructions

1. Clone the repository
2. Build the binaries
3. Add conduit to /usr/local/bin

```bash
git clone https://github.com/lanl/conduit
cd conduit
# build server
go build -o conduit-server ./cmd/server
# build cli
go build -o conduit ./cmd/cli

# build fta
go build -o conduit-fta ./cmd/fta

# build runner
go build -o conduit-runner ./cmd/runner
# add symlink to /usr/local/bin
ln -s $(pwd)/conduit /usr/local/bin/conduit
```

## Cert Generation

1. Create CONDUIT directory `/etc/conduit`

2. Populate CONDUIT config `/etc/conduit/config.yaml`

```yaml
auth:
  external-ca-cert: /etc/conduit/keys/conduit-external-ca.pem
  external-ca-key: /etc/conduit/keys/conduit-external-key.pem
  internal-ca-cert: /etc/conduit/keys/conduit-internal-ca.pem
  internal-ca-key: /etc/conduit/keys/conduit-internal-key.pem
  keytab: /etc/krb5.keytab
  requested-cert-lifetime: 24h
errant-lock: 336h
etcd:
  - hostname: db-node1.example.com
    ip: 192.168.0.18
    port: 2379
  - hostname: db-node2.example.com
    ip: 192.168.0.19
    port: 2379
  - hostname: db-node3.example.com
    ip: 192.168.0.20
    port: 2379
ldap:
  base-dn: []
  host: ""
  krb5-attributes: []
  port: 389
  uid-number-attributes:
    - uidNumber
  uname-attributes:
    - uid
node-allocations:
  setup:
    memory: 10MB
    nodes: 1
  teardown:
    memory: 10MB
    nodes: 1
  transfer:
    memory: 500MB
    nodes: 2
  validation:
    memory: 10MB
    nodes: 1
nodes:
  fta1:
    address: fta1.example.com
    port: 23457
    min-memory: 1GB
    max-jobs: 4
  fta2:
    # example of ip address instead of hostname
    address: 192.168.20.7
    port: 23457
    min-memory: 1GB
    max-jobs: 4
rqlite:
  - hostname: db-node1.example.com
    ip: 192.168.0.18
    port: 4001
  - hostname: db-node2.example.com
    ip: 192.168.0.19
    port: 4001
  - hostname: db-node3.example.com
    ip: 192.168.0.20
    port: 4001
server:
  hostname:
    - conduit-server.example.com
  ip:
    - 192.168.0.254
    - 204.121.70.130
  port: 23456
  ws-port: 8080
  concurrency:
    schedulers: 1
    watchdogs: 1
    transfer-workers: 1
test: false
transfer:
  expiry-advance: 60s
  max-source-bytes: 4000
```

3. Generate certs:

```bash
CONDUIT_PATH=/etc/conduit
mkdir -p $CONDUIT_PATH/keys

# generate internal CA
conduit-server internal-ca -d

conduit-server external-ca -d

# generate server certs
for i in {1..3}; do
ip=$((i+17))
fta=$(printf %02d $(($i+8)))
conduit-server internal-server-cert \
    -d \
    --separate-cert-key \
    --cert-name etcd_${i}_server_cert.pem \
    --key-name etcd_${i}_server_key.pem \
    --output /etc/conduit/keys/ \
    --server-ip 192.168.0.${ip} \
    --server-commonname etcd-${i} \
    --server-hostname fta${fta}.example.com

conduit-server internal-server-cert \
    -d \
    --separate-cert-key \
    --cert-name rqlite_${i}_server_cert.pem \
    --key-name rqlite_${i}_server_key.pem \
    --output /etc/conduit/keys/ \
    --server-ip 192.168.0.${ip} \
    --server-commonname rqlite-${i} \
    --server-hostname fta${fta}.example.com
done

# generate cert for the slurm plugin to use for authentication
conduit-server external-client-cert \
    -d \
    --separate-cert-key \
    --cert-name conduit-slurm-cert.pem \
    --key-name conduit-slurm-key.pem \
    --output /etc/conduit/keys/ \
    --client-commonname conduit-service \ // this CN is unique, do not change
    --expiration 365

# generate cert for admins to use for authentication
conduit-server external-client-cert \
    -d \
    --separate-cert-key \
    --cert-name conduit-admin-cert.pem \
    --key-name conduit-admin-key.pem \
    --output /etc/conduit/keys/ \
    --client-commonname conduit-admin \ // this CN is unique, do not change
    --expiration 365

# generate cert to communicate with etcd for debugging
conduit-server internal-client-cert \
    -d \
    --separate-cert-key \
    --cert-name etcd-client-cert.pem \
    --key-name etcd-client-key.pem \
    --output /etc/conduit/keys/ \
    --expiration 365 \
    --client-commonname root
```

## ETCD Setup

1. Create the `etcd` user

2. [Download etcd prebuilt binary](https://github.com/etcd-io/etcd/releases/latest)

3. Move binary to /usr/bin/etcd

4. Populate environment file at `/var/lib/etcd/environment` with correct values:

```bash
ETCD_DATA_DIR=/var/lib/etcd/data
# unique name of this etcd node
ETCD_NAME=etcd-01
# ip for conduit to reach etcd
ETCD_LISTEN_CLIENT_URLS=https://192.168.0.18:2379
ETCD_ADVERTISE_CLIENT_URLS=https://192.168.0.18:2379
# ip for other etcd nodes to communicate with this node
ETCD_LISTEN_PEER_URLS=https://192.168.0.18:2380
ETCD_INITIAL_ADVERTISE_PEER_URLS=https://192.168.0.18:2380
# addresses and names for all other etcd nodes
ETCD_INITIAL_CLUSTER=etcd-01=https://192.168.0.18:2380,etcd-02=https://192.168.0.19:2380,etcd-03=https://192.168.0.20:2380
# random token used during bootstrap. Should be the same across nodes
ETCD_INITIAL_CLUSTER_TOKEN=token-01
ETCD_INITIAL_CLUSTER_STATE=new
ETCD_LOG_LEVEL=debug
ETCD_LOGGER=zap
ETCD_LOG_OUTPUTS=default
# server cert and key used for communicating with conduit
ETCD_CERT_FILE=/var/lib/etcd/etcd-server-cert.pem
ETCD_KEY_FILE=/var/lib/etcd/etcd-server-key.pem
# the conduit internal ca cert
ETCD_TRUSTED_CA_FILE=/etc/conduit/keys/conduit-internal-ca.pem
# enable mTLS for conduit communication
ETCD_CLIENT_CERT_AUTH=1
# cert used for communicating with other etcd nodes
ETCD_PEER_CERT_FILE=/var/lib/etcd/etcd-server-cert.pem
ETCD_PEER_KEY_FILE=/var/lib/etcd/etcd-server-key.pem
ETCD_PEER_TRUSTED_CA_FILE=/etc/conduit/keys/conduit-internal-ca.pem
# enable mTLS for communication between etcd nodes
ETCD_PEER_CLIENT_CERT_AUTH=1
```

5. Install [service file](./service/etcd.service) at `/usr/lib/systemd/system/etcd.service`

6. Start ETCD

```bash
systemctl enable etcd
systemctl start etcd
```

## rqlite Setup

1. Create the `rqlite` user

2. [Download rqlite prebuilt binary](https://github.com/rqlite/rqlite/releases/latest)

3. Move binary to /usr/bin/rqlite

4. Populate environment file at `/var/lib/rqlite/environment` with correct values:

```bash
# unique id for this rqlite node
RQLITE_NODE_ID=1
# ip for conduit to reach rqlite (HTTP API)
RQLITE_HTTP_ADDR=192.168.0.18:4001
RQLITE_HTTP_ADV_ADDR=192.168.0.18:4001
# ip for other rqlite nodes to communicate with this node (Raft)
RQLITE_RAFT_ADDR=192.168.0.18:4002
RQLITE_RAFT_ADV_ADDR=192.168.0.18:4002
# Raft address that nodes should use to join cluster (note: uses Raft port 4002, not HTTP port)
RQLITE_JOIN=192.168.0.18:4002
# server cert and key used for communicating with conduit
RQLITE_CA_PATH=/etc/conduit/keys/conduit-internal-ca.pem
RQLITE_CERT_PATH=/var/lib/rqlite/rqlite-server-cert.pem
RQLITE_KEY_PATH=/var/lib/rqlite/rqlite-server-key.pem
# cert used for communicating with other rqlite nodes
RQLITE_PEER_CA_PATH=/etc/conduit/keys/conduit-internal-ca.pem
RQLITE_PEER_CERT_PATH=/var/lib/rqlite/rqlite-server-cert.pem
RQLITE_PEER_KEY_PATH=/var/lib/rqlite/rqlite-server-key.pem
```

5. Install [service file](./service/rqlite.service) in /usr/lib/systemd/system/rqlite.service

6. Start rqlite

```bash
systemctl enable rqlite
systemctl start rqlite
```

## Conduit Server Setup

1. Copy `conduit-server` binary to /usr/local/bin

2. Install [service file](./service/conduit-server.service) in /usr/lib/systemd/system/conduit-server.service

3. Start conduit-server

```bash
systemctl enable conduit-server
systemctl start conduit-server
```

## Runner Setup

1. Copy over `conduit-runner` binary to each fta node at /usr/local/bin

2. Add `/etc/conduit/conduit-runner-config.yaml` to all FTAs:

```yaml
auth:
  internal-ca-cert: /etc/conduit/keys/conduit-internal-ca.pem
  internal-ca-key: /etc/conduit/keys/conduit-internal-key.pem
etcd:
  - hostname: db-node1.example.com
    ip: 192.168.0.18
    port: 2379
  - hostname: db-node2.example.com
    ip: 192.168.0.19
    port: 2379
  - hostname: db-node3.example.com
    ip: 192.168.0.20
    port: 2379
fta:
  path: /opt/storage/conduit/src/conduit-fta/conduit-fta
  # options need to be single string with no spaces. Use '=' with flags
  options:
    - "--config=/opt/storage/conduit/configs/conduit-fta-config.yaml"
    - "-d" # debug mode
  environment:
    path: "/bin:/usr/bin/:/usr/local/bin/:/usr/local/sbin:/usr/sbin"
    ld-Library-Path: "/lib/:/lib64/:/usr/local/lib"
server:
  hostname:
    - fta1.example.com
  ip:
    - 192.168.20.6
  port: 23457
```

3. Install [service file](./service/conduit-runner.service) in /usr/lib/systemd/system/conduit-runner.service

4. Start conduit-runner

```bash
systemctl enable conduit-runner
systemctl start conduit-runner
```

## FTA Setup

1. Copy over `conduit-fta` binary to each fta node. This needs to be in the location specified in the conduit runner config created earlier

2. Add `/etc/conduit/conduit-fta-config.yaml` to all FTAs:

```yaml
auth:
  internal-ca-cert: /etc/conduit/keys/conduit-internal-ca.pem
conduit:
  expiry-advance: 5m
  expiry-interval: 30s
etcd:
  - ip: 192.168.0.18
    port: 2379
  - ip: 192.168.0.19
    port: 2379
  - ip: 192.168.0.20
    port: 2379
fta:
  verify-retry-count: 20
  verify-sleep-duration: 5s
plugins:
  pftool:
    pfcp-path: /opt/software/campaign/install/bin/pfcp
  rsync:
    rsync-path: rsync

filesystems:
  nfs-scratch:
    user-path: ^/nfs-scratch/scratch([0-9]+)/(.*)
    fta-root-fs-path: /nfs-scratch/
    fta-path: /nfs-scratch/scratch$1/$2
    plugin-stages:
      validation: posix
      setup-src: posix
      setup-dst: posix
      transfer-src:
        - pftool
      transfer-dst:
        - pftool
      teardown-src: posix
      teardown-dst: posix
    custom-plugin-config:
      posix-src-trash: ""
      pftool-no-progress-timeout-hours: 1
  marfs:
    user-path: ^(/marfs)?/(campaign)/([\w\-. ]+)(/.*)?
    fta-root-fs-path: /marfs/$2/
    fta-path: /marfs/$2/$3$4
    plugin-stages:
      validation: posix
      setup-src: posix
      setup-dst: posix
      transfer-src:
        - pftool
      transfer-dst:
        - pftool
      teardown-src: posix
      teardown-dst: posix
    custom-plugin-config:
      posix-src-trash: ""
      pftool-no-progress-timeout-hours: 1
  marchive:
    user-path: ^(/marfs)?/(archive)/([\w\-. ]+)(/.*)?
    fta-root-fs-path: /marfs/$2/
    fta-path: /marfs/$2/$3$4
    plugin-stages:
      validation: posix
      setup-src: marchive
      setup-dst: posix
      transfer-src:
        - pftool
      transfer-dst:
        - pftool
      teardown-src: marchive
      teardown-dst: posix
    custom-plugin-config:
      posix-src-trash: ""
      pftool-no-progress-timeout-hours: 1
```

## Kerberos Setup

Kerberos Instructions:

1. Get keytab from security

2. Move keytab to `/etc/krb5.keytab`

3. Create config file `/etc/krb5.conf`:

```conf
# Configuration snippets may be placed in this directory as well
includedir /etc/krb5.conf.d/

[logging]
default = FILE:/var/log/krb5libs.log
kdc = FILE:/var/log/krb5kdc.log
admin_server = FILE:/var/log/kadmind.log

[libdefaults]
dns_lookup_realm = false
ticket_lifetime = 10h
renew_lifetime = 7d
forwardable = true
rdns = false
pkinit_anchors = FILE:/etc/pki/tls/certs/ca-bundle.crt
default_realm = HPC.EXAMPLE.COM
default_ccache_name = FILE:/tmp/krb5cc_%{uid}

[realms]
HPC.EXAMPLE.COM = {
    kdc = hpc-auth1.example.com:88
    kdc = hpc-auth2.example.com:88
    admin_server = hpc-auth1.example.com:749
    default_domain = example.com
    pkinit_identities = PKCS11:opensc-pkcs11.so
    pkinit_anchors = FILE:/etc/krb5.anchors.hpc.pem
    pkinit_pool = FILE:/etc/krb5.pool.hpc.pem
}

[kdc]
profile = /var/kerberos/krb5kdc/kdc.conf

[appdefaults]
pam = {
    debug = true
}
```

4. Create reticket script in `/usr/sbin/reticket`:

```bash
#!/bin/bash

# Script to test if there is a Turquoise ticket, when running in the
# Turquoise RE. Nothing is done, if not in the RE. Make use of LOGNAME
# environment variable.

rc=0
# Need to figure out if we are in the RE
if [ -n "`grep default_realm /etc/krb5.conf | grep HPC.EXAMPLE.COM`" ]; then
TURQ_TICKET="$(klist | grep '^Default principal:' | grep '@HPC.EXAMPLE.COM')"
# If there is no Turquoise ticket, then need to create one ...
if [ -z "$TURQ_TICKET" ]; then
    echo "Need to generate Turquoise Kerberos ticket..."
    kinit -n @HPC.EXAMPLE.COM
    kinit -T "$(klist | grep '^Ticket cache:' | sed 's/^Ticket cache: //')" ${LOGNAME}@HPC.EXAMPLE.COM
#   echo "Return: $?"
    rc=$?
fi
fi

exit $rc
```

5. Download `krb5.anchors.hpc.pem` and `krb5.pool.hpc.pem` from organization. Put them in `/etc/`

6. Install packages:

```bash
yum install krb5-pkinit krb5-workstation pam_krb5 krb5-libs
```

7. Install organization specific certs:

```bash
yum install organization-krb-certs...rpm
```
