# ETCD Direct Query

This guide shows how to query ETCD directly using `etcdctl` for debugging and troubleshooting purposes.

## Prerequisites

You need a client certificate and key signed by the Conduit internal CA. See the [Generating Certificates](cert-generation.md) guide for details on creating these credentials.

## ETCD Key Organization

Conduit organizes data in ETCD using a hierarchical prefix structure:

- `transfers/` - Root prefix for all transfers
- `transfers/<transfer-id>/` - Individual transfer data
- `transfers/<transfer-id>/<field>` - Specific transfer fields (state, error, errorMessage, etc.)

## Common Queries

Set up environment variables for convenience:

```bash
export ETCD_CERT=/etc/conduit/keys/etcd-client-cert.pem
export ETCD_KEY=/etc/conduit/keys/etcd-client-key.pem
export ETCD_CA=/etc/conduit/keys/conduit-internal-ca.pem
export ETCD_ENDPOINTS=192.168.0.254:2379
```

### Get a Specific Transfer Field

```bash
# Get the state of a transfer
etcdctl get --cert $ETCD_CERT --key $ETCD_KEY --cacert $ETCD_CA \
  --endpoints=$ETCD_ENDPOINTS \
  transfers/ebb444e4-48d8-4fc1-bc31-c8c6816f2b0d/state
```

### Get All Data for a Transfer

```bash
# Get all key-value pairs for a specific transfer
etcdctl get --cert $ETCD_CERT --key $ETCD_KEY --cacert $ETCD_CA \
  --endpoints=$ETCD_ENDPOINTS \
  --prefix transfers/ebb444e4-48d8-4fc1-bc31-c8c6816f2b0d
```

### List All Transfers

```bash
# List all transfer IDs
etcdctl get --cert $ETCD_CERT --key $ETCD_KEY --cacert $ETCD_CA \
  --endpoints=$ETCD_ENDPOINTS \
  --prefix --keys-only transfers/
```
