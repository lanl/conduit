# Python Client

The Conduit Python client provides a native Python interface for interacting with Conduit's data transfer service via gRPC. It wraps the generated gRPC stubs with a clean, Pythonic API while still allowing access to underlying protobuf types when needed.

## Installation

Clone the repository and install:

```bash
git clone https://github.com/lanl/conduit
cd conduit/clients/python
pip install .
```

## Configuration

### Configuration Object

Configure the client using `ConduitClientConfig`:

```python
from conduit import ConduitClient, ConduitClientConfig

cfg = ConduitClientConfig(
    addr="conduit-server.example.com:23456",
    timeout_s=10.0,
    ca_pem_path="/etc/conduit/keys/conduit-external-ca.pem",
    cert_key_bundle_path="/home/testuser/.conduit-cert-key-bundle.pem",
)

client = ConduitClient(cfg)
```

### `ConduitClientConfig` Options

| Option                 | Type        | Default                                | Description                                                                                                                                                            |
|------------------------|-------------|----------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `addr`                 | `str\|None` | system-dependent                       | gRPC server address in `host:port` form. May be derived from the local system or environment variables (`CONDUIT_CLI_CONDUIT_IP`) if not explicitly provided.          |
| `timeout_s`            | `float`     | `10.0`                                 | Default timeout (deadline) in seconds for unary RPCs and streaming calls.                                                                                              |
| `ca_pem_path`          | `str\|None` | system-dependent                       | Path to the PEM file containing the CA certificates used to verify the server. If `None`, the value of `CONDUIT_CLI_CONDUIT_CA` or the system trust store may be used. |
| `cert_key_bundle_path` | `str\|None` | value of `CONDUIT_CLI_CERT_KEY_BUNDLE` | Path to a combined PEM file containing both the client certificate and private key for mTLS authentication.                                                            |
| `client_cert_pem_path` | `str\|None` | `None`                                 | Path to the client certificate PEM file (used when cert and key are provided separately).                                                                              |
| `client_key_pem_path`  | `str\|None` | `None`                                 | Path to the client private key PEM file (used when cert and key are provided separately).                                                                              |
| `grpc_limit`           | `int`       | `100000000`                            | Maximum allowed size (in bytes) for received gRPC messages. Applied at the channel level.                                                                              |


### Configuration Precedence (if applicable)

When defaults are computed dynamically, the typical precedence order is:

1. Explicit arguments passed to `ConduitClientConfig`
2. Environment variable overrides (if enabled)
3. System-specific defaults
4. Library defaults


## Example

```python
from conduit import ConduitClient, ConduitClientConfig
from conduit._generated import api_pb2

cfg = ConduitClientConfig(
    addr="conduit-server.example.com:23456",
    timeout_s=10.0,
    ca_pem_path="/etc/conduit/keys/conduit-external-ca.pem",
    cert_key_bundle_path="/home/testuser/.conduit-cert-key-bundle.pem",
)

with ConduitClient(cfg) as client:
    # Start a copy transfer
    transfer: api_pb2.TransferDetails = client.start_transfer(
        sources=["/mnt/fs_1/foo/hello.txt"],
        destination="/mnt/fs_2/bar/hello.txt",
        action=api_pb2.RECURSIVE_COPY,
    )

    print(f"Transfer ID: {transfer.transferID}")

    state: api_pb2.TransferState = api_pb2.TRANSFER_NONE

    # Watch transfer until complete
    for details in client.watch_transfer(transfer.transferID):
        if api_pb2.TransferState.Name(details.state) != state:
            state = api_pb2.TransferState.Name(details.state)
            print(f"State: {state}")
        # Loop ends automatically when transfer is no longer active

    # Get final status
    final_status = client.status(transfer.transferID)
    print(f"Final status: {final_status}")

```

## API Reference

### ConduitClient

The main client class for interacting with Conduit.

#### Initialization

```python
client = ConduitClient(cfg: ConduitClientConfig)
```

Creates a new client with the given configuration. The client manages its own gRPC channel.

#### Context Manager Support

```python
with ConduitClient(cfg) as client:
    # Use client
    pass
# Channel is automatically closed
```

Or manually:

```python
client = ConduitClient(cfg)
try:
    # Use client
    pass
finally:
    client.close()
```

### Core Methods

#### start_transfer()

Start a new transfer.

```python
transfer: api_pb2.TransferDetails = client.start_transfer(
    sources: list[str],
    destination: str,
    action: api_pb2.Action,
    user: str = "",  # Only needed with service cert
)
```

**Parameters:**

- `sources`: List of source paths (files or directories)
- `destination`: Destination path
- `action`: Transfer action (see Actions below)
- `user`: User to run transfer as (service cert only)

**Returns:** `TransferDetails` protobuf message with transfer info including `transferID`

**Actions:**

```python
api_pb2.COPY            # Copy single file
api_pb2.MOVE            # Move files
api_pb2.RECURSIVE_COPY  # Copy files recursively
api_pb2.RECURSIVE_MOVE  # Move files recursively
```

#### status()

Get current status of a transfer.

```python
details: api_pb2.TransferDetails = client.status(
    transfer_id: str,
    user: str = "",  # Only needed with service cert
)
```

**Returns:** `TransferDetails` with current state, progress, errors, etc.

#### watch_transfer()

Stream status updates for a transfer.

```python
for details in client.watch_transfer(transfer_id: str):
    # Process each update
    print(details.state)
    # Loop ends automatically when transfer becomes inactive
```

This is a generator that yields `TransferDetails` messages as the transfer progresses. It automatically stops when `details.active == False`.


#### stop_transfer()

Abort an active transfer.

```python
client.stop_transfer(
    transfer_id: str,
    user: str = "",  # Only needed with service cert
)
```

## Working with Transfer States

Transfer states are defined in the protobuf:

```python
from conduit._generated import api_pb2

# Access state enum
state = details.state

# Get state name as string
state_name = api_pb2.TransferState.Name(state)

# Check specific states
if state == api_pb2.TRANSFER_RUNNING:
    print("Transfer is running")
elif state == api_pb2.TRANSFER_FINALIZED:
    print("Transfer complete")
elif state == api_pb2.TRANSFER_ERROR:
    print(f"Transfer failed: {details.errorMessage}")
```
