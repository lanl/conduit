# Start & Stop Server

This guide covers how to control the Conduit server using both the CLI and systemd.

## Prerequisites

CLI control commands require an admin certificate and key. See the [Generating Certificates](cert-generation.md) guide for details.

## CLI Control

Use `conduit control` commands to manage server state. Target a specific instance with `--ip` and `--port` flags.

### Drain Mode

Put the server in drain mode to gracefully stop accepting new transfers:

```bash
conduit --cert /etc/conduit/keys/conduit-admin-cert.pem \
  --key /etc/conduit/keys/conduit-admin-key.pem \
  control drain
```

**What happens:**
- Stops accepting new transfers from the init state
- Continues processing all in-progress transfers
- Best used when draining all instances in a cluster

**Important:** Drain mode must be manually stopped with `systemctl stop conduit-server` as the server cannot detect when all transfers are complete.

### Resume from Drain

Resume normal operation after draining:

```bash
conduit --cert /etc/conduit/keys/conduit-admin-cert.pem \
  --key /etc/conduit/keys/conduit-admin-key.pem \
  control start
```

## systemd Control

### Stop the Server

```bash
systemctl stop conduit-server
```

**What happens:**
- Server begins graceful shutdown
- Waiting leases are reverted to "validation complete" for other instances to pick up
- In-progress transfers continue as long as runners are active
- New transfer requests are rejected

### Start the Server

```bash
systemctl start conduit-server
```

Starts the server from a fully stopped state.

**Note:** This is different from `conduit control start`, which only resumes from drain mode.

### Check Server Status

```bash
systemctl status conduit-server
```

### Enable Auto-start on Boot

```bash
systemctl enable conduit-server
```
