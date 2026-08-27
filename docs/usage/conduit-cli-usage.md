# Conduit CLI Usage

The `conduit` command-line interface provides tools for submitting, monitoring, and managing file transfers.

## Basic Usage

```bash
conduit <COMMAND> [ARGS] [FLAGS]
```

View help for any command:

```bash
conduit --help
conduit <command> --help
```

## Global Flags

These flags can be used with any command:

| Flag                 | Type   | Required | Description                                                                                        |
|----------------------|--------|----------|----------------------------------------------------------------------------------------------------|
| `--ca, -c`           | string | no       | Location of conduit root CA certificate                                                            |
| `--cert`             | string | no       | Path to the mTLS client certificate to use for the request                                         |
| `--cert-key-bundle`  | string | no       | Shorthand for setting --cert and --key to the same path (default "~/.conduit-cert-key-bundle.pem") |
| `--config`           | string | no       | Config file (default is /etc/conduit/conduit-cli-config.yaml)                                     |
| `--debug, -d`        | flag   | no       | Enable debugging output                                                                            |
| `--grpc-limit`       | int    | no       | Size limit (in bytes) of grpc messages received from conduit (default 100000000)                   |
| `--ip, -i`           | string | no       | Address of the conduit server                                                                      |
| `--key`              | string | no       | Path to the mTLS client key to use for the request                                                 |
| `--krb-cache`        | string | no       | Location of the krb5 ticket cache (default "/tmp")                                                 |
| `--krb-cache-prefix` | string | no       | Prefix before the UID of the tickets located in the krb5 cache (default "krb5cc\_")                |
| `--krb-config`       | string | no       | Location of the krb5 config file (default "/etc/krb5.conf")                                        |
| `--krb-spn`          | string | no       | The conduit service principal name (spn) (default "conduit/conduit-server.example.com")            |
| `--port, -p`         | int    | no       | Port of the conduit server (default 23456)                                                         |
| `--req-timeout`      | string | no       | Timeout for requests made to conduit (default 1m)                                                  |

## Commands

### cp - Copy files and directories

Copy files and/or directories from source to destination.

#### Usage

```bash
conduit cp SOURCE... DESTINATION [flags]
```

#### Arguments & Flags

| Name                    | Type       | Required | Description                                                                    |
| ----------------------- | ---------- | -------- | ------------------------------------------------------------------------------ |
| `SOURCE`                | positional | yes      | Path to source file(s)/directories                                             |
| `DESTINATION`           | positional | yes      | Path to destination file/directory                                             |
| `--background, -b`      | flag       | no       | Submit a conduit transfer without watching it progress to completion           |
| `--quiet, -q`           | flag       | no       | Reduce command output to only a transferID                                     |
| `--recursive, -r`       | flag       | no       | Copy directories recursively                                                   |
| `--skip-validation, -s` | flag       | no       | Skip waiting for validation to succeed                                         |
| `--user`                | string     | no       | The user to start the transfer as. Requires an admin cert & key to be provided |
| `--validate-only`       | flag       | no       | Do not transfer any data, just run validation                                  |

#### Examples

Copy a single file:
```bash
conduit cp /mnt/fs_1/file_1 /mnt/fs_2/dest_dir/
```

Copy multiple files to a destination directory:
```bash
conduit cp /mnt/fs_1/file_1 /mnt/fs_1/file_2 /mnt/fs_2/dest_dir/
```

Copy a directory recursively:
```bash
conduit cp -r /mnt/fs_1/source_dir /mnt/fs_2/dest_dir/
```

Submit a transfer in the background:
```bash
conduit cp -b /mnt/fs_1/large_file /mnt/fs_2/
```

Validate paths without transferring data:
```bash
conduit cp --validate-only /mnt/fs_1/file_1 /mnt/fs_2/
```

### mv - Move files and directories

Move files and/or directories from source to destination. The source files are deleted after successful transfer.

#### Usage

```bash
conduit mv SOURCE... DESTINATION [flags]
```

#### Arguments & Flags

| Name                    | Type       | Required | Description                                                                    |
| ----------------------- | ---------- | -------- | ------------------------------------------------------------------------------ |
| `SOURCE`                | positional | yes      | Path to source file(s)/directories                                             |
| `DESTINATION`           | positional | yes      | Path to destination file/directory                                             |
| `--background, -b`      | flag       | no       | Submit a conduit transfer without watching it progress to completion           |
| `--quiet, -q`           | flag       | no       | Reduce command output to only a transferID                                     |
| `--recursive, -r`       | flag       | no       | Move directories recursively                                                   |
| `--skip-validation, -s` | flag       | no       | Skip waiting for validation to succeed                                         |
| `--user`                | string     | no       | The user to start the transfer as. Requires an admin cert & key to be provided |
| `--validate-only`       | flag       | no       | Do not transfer any data, just run validation                                  |

#### Examples

Move a single file:
```bash
conduit mv /mnt/fs_1/file_1 /mnt/fs_2/dest_dir/
```

Move a directory recursively:
```bash
conduit mv -r /mnt/fs_1/source_dir /mnt/fs_2/dest_dir/
```

### status - Get transfer status

Get the status of one or more transfers. If no transfer ID is provided, returns status of all your transfers.

#### Usage

```bash
conduit status [TRANSFER_ID | SLURM_JOB_ID | TRANSFER_TIMESTAMP] [flags]
```

#### Arguments & Flags

| Name                 | Type       | Required | Description                                          |
|----------------------|------------|----------|------------------------------------------------------|
| `TRANSFER_ID`        | positional | no       | Specific transfer ID to query                        |
| `SLURM_JOB_ID`       | positional | no       | Slurm job ID associated with transfer                |
| `TRANSFER_TIMESTAMP` | positional | no       | Timestamp to filter transfers                        |
| `-n, --limit`        | int        | no       | Limit number of transfers returned                   |
| `-o, --output`       | string     | no       | Output format: "normal" or "wide" (default "normal") |

#### Examples

Get status of all your transfers:
```bash
conduit status
```

Get status of a specific transfer:
```bash
conduit status 3881b460-f415-4351-a69e-ba530d4cb341
```

Get status with wide output format:
```bash
conduit status -o wide
```

Limit results to 10 most recent transfers:
```bash
conduit status -n 10
```

### describe - Get detailed transfer information

Get detailed information about transfers in JSON or YAML format, with optional JSONPath filtering for scripting.

#### Usage

```bash
conduit describe [TRANSFER_ID | SLURM_JOB_ID | TRANSFER_TIMESTAMP] [flags]
```

#### Arguments & Flags

| Name                 | Type       | Required | Description                                                        |
|----------------------|------------|----------|--------------------------------------------------------------------|
| `TRANSFER_ID`        | positional | no       | Specific transfer ID to query                                      |
| `SLURM_JOB_ID`       | positional | no       | Slurm job ID associated with transfer                              |
| `TRANSFER_TIMESTAMP` | positional | no       | Timestamp to filter transfers                                      |
| `-n, --limit`        | int        | no       | Limit number of transfers returned                                 |
| `-o, --output`       | string     | no       | Output format: "json" or "yaml" (default "yaml")                   |
| `--jsonpath`         | string     | no       | JSONPath expression to filter output (only works with json output) |

#### Examples

Get detailed transfer information in YAML:
```bash
conduit describe 3881b460-f415-4351-a69e-ba530d4cb341
```

Get detailed information in JSON:
```bash
conduit describe -o json
```

Get the most recent transfer:
```bash
conduit describe -n 1
```

### watch - Monitor transfer progress

Watch the progress of one or more transfers in real-time until they complete or fail.

#### Usage

```bash
conduit watch [TRANSFER_ID ...] [flags]
```

#### Arguments & Flags

| Name          | Type       | Required | Description                                                       |
|---------------|------------|----------|-------------------------------------------------------------------|
| `TRANSFER_ID` | positional | no       | One or more transfer IDs to watch (watches all if not provided)   |
| `-n, --limit` | int        | no       | Limit number of transfers to watch (default 20 when watching all) |

#### Examples

Watch a specific transfer:
```bash
conduit watch 3881b460-f415-4351-a69e-ba530d4cb341
```

Watch multiple specific transfers:
```bash
conduit watch 3881b460-f415-4351-a69e-ba530d4cb341 202e1d65-8a76-4fb7-859d-58166560268c
```

Watch all your transfers (defaults to showing 20 most recent):
```bash
conduit watch
```

Watch all your transfers with a custom limit:
```bash
conduit watch -n 50
```

### abort - Cancel a transfer

Abort a running or queued transfer.

#### Usage

```bash
conduit abort TRANSFER_ID [flags]
```

#### Arguments & Flags

| Name          | Type       | Required | Description                    |
| ------------- | ---------- | -------- | ------------------------------ |
| `TRANSFER_ID` | positional | yes      | Transfer ID to abort           |

#### Examples

Abort a transfer:
```bash
conduit abort 3881b460-f415-4351-a69e-ba530d4cb341
```

### purge - Clean up errant paths

Remove errant paths (temporary staging directories) left by failed or aborted transfers. You must specify the trash paths you want to purge.

#### Usage

```bash
conduit purge TRASH_PATH... [flags]
```

#### Arguments & Flags

| Name         | Type       | Required | Description                                                   |
|--------------|------------|----------|---------------------------------------------------------------|
| `TRASH_PATH` | positional | yes      | One or more errant trash paths to purge from conduit tracking |

#### Examples

Purge a specific errant path:
```bash
conduit purge /mnt/fs_1/.conduit_trash_12345
```

Purge multiple errant paths:
```bash
conduit purge /mnt/fs_1/.conduit_trash_12345 /mnt/fs_2/.conduit_trash_67890
```

### cert - Generate client certificates

Generate a client certificate/key pair for authentication with the conduit server as an alternative to Kerberos. Note: This command requires Kerberos authentication to generate the certificate, but subsequent commands can use the generated certificate instead of Kerberos.

#### Usage

```bash
conduit cert [flags]
```

#### Flags

| Name           | Type   | Required | Description                                                                  |
|----------------|--------|----------|------------------------------------------------------------------------------|
| `--user`       | string | no       | Retrieve a cert for this user. Requires an admin cert & key to be provided   |
| `--output, -o` | string | no       | Path to write the cert-key-bundle (default "~/.conduit-cert-key-bundle.pem") |

#### Description

This command requests a certificate and key pair from the conduit server. Once generated, these credentials can be used with the `--cert` and `--key` flags (or `--cert-key-bundle`) for authentication without requiring Kerberos tickets.

#### Examples

Generate a certificate/key pair (requires active Kerberos ticket):
```bash
kinit username@REALM.COM
conduit cert
```

Generate a certificate with custom output location:
```bash
conduit cert -o /path/to/my-cert-bundle.pem
```

Generate a certificate for another user (requires admin privileges):
```bash
conduit cert --user otheruser
```

Use the generated certificate for subsequent commands:
```bash
conduit --cert-key-bundle ~/.conduit-cert-key-bundle.pem status
```

### version - Display version information

Display the version of both the conduit CLI and the conduit server.

#### Usage

```bash
conduit version
```

#### Description

This command displays version information for both the conduit CLI client and the conduit server. It shows the git commit hash and whether the build was modified from that commit.

#### Example

```bash
conduit version
# Output:
# conduit-cli version: abc123def456
# conduit-server version: abc123def456
```

## Advanced Querying with JSONPath

The `describe` command supports JSONPath filtering, which is useful for scripting and extracting specific information from transfers.

### JSONPath Syntax

JSONPath uses expressions to navigate JSON structures:
- `$` - Root element
- `.` - Child element
- `..` - Recursive descent
- `[n]` - Array index
- `[*]` - All array elements
- `[? @.field=="value"]` - Filter expression

### Query Examples

Get all transfer IDs:
```bash
conduit describe -n -1 -o json --jsonpath '$..transferID'
# Output: ["3881b460-f415-4351-a69e-ba530d4cb341","202e1d65-8a76-4fb7-859d-58166560268c"]
```

Get the transfer ID of the most recent transfer:
```bash
conduit describe -n -1 -o json --jsonpath '$[0].transferID'
# Output: "3881b460-f415-4351-a69e-ba530d4cb341"
```

Get the state of a specific transfer:
```bash
conduit describe -n -1 -o json --jsonpath '$[? @.transferID=="3881b460-f415-4351-a69e-ba530d4cb341"].state'
# Output: ["TRANSFER_FINALIZED"]
```

Get transfer IDs of all failed transfers:
```bash
conduit describe -n -1 -o json --jsonpath '$[? @.state=="TRANSFER_ERROR"].transferID'
# Output: ["3881b460-f415-4351-a69e-ba530d4cb341"]
```

Get transfer IDs of all completed transfers:
```bash
conduit describe -n -1 -o json --jsonpath '$[? @.state=="TRANSFER_FINALIZED"].transferID'
# Output: ["a2a29634-a064-40aa-bc19-4f0f3d77f43c","f62cc9bd-6096-4009-9340-cdf434c2ce4f"]
```

Get source paths of all transfers:
```bash
conduit describe -n -1 -o json --jsonpath '$..source'
```

Get byte counts for completed transfers:
```bash
conduit describe -n -1 -o json --jsonpath '$[? @.state=="TRANSFER_FINALIZED"].dataTransferred'
```

### Scripting Examples

Check if a transfer completed successfully:
```bash
#!/bin/bash
TRANSFER_ID="3881b460-f415-4351-a69e-ba530d4cb341"
STATE=$(conduit describe -o json --jsonpath "\$[? @.transferID==\"$TRANSFER_ID\"].state" | jq -r '.[0]')

if [ "$STATE" == "TRANSFER_COMPLETE" ]; then
    echo "Transfer completed successfully"
    exit 0
else
    echo "Transfer failed or still running: $STATE"
    exit 1
fi
```

Get a count of active transfers:
```bash
#!/bin/bash
ACTIVE_STATES='TRANSFER_QUEUED|TRANSFER_SCHEDULED|TRANSFER_DATA_TRANSFERRING'
ACTIVE_COUNT=$(conduit describe -o json --jsonpath "\$[? @.active==true].transferID" | jq 'length')
echo "Active transfers: $ACTIVE_COUNT"
```

## Configuration File

The conduit CLI can be configured using a YAML configuration file. By default, it looks for `/etc/conduit/conduit-cli-config.yaml`, but you can specify a different location with the `--config` flag.

### Example Configuration

[CONDUIT CLI Full Reference Config](../configs/conduit-cli-full-reference-config.yaml)

## Authentication

Conduit supports two authentication methods:

### Kerberos Authentication

The default authentication method. Ensure you have a valid Kerberos ticket:

```bash
# Get a Kerberos ticket
kinit username@REALM.COM

# Verify your ticket
klist

# Use conduit (will automatically use your Kerberos ticket)
conduit status
```

### Certificate-based Authentication (mTLS)

For automated scripts or when Kerberos is not available:

```bash
# Generate a client certificate
conduit cert

# Use a combined cert+key bundle for subsequent commands
conduit --cert-key-bundle ~/.conduit-cert-key-bundle.pem status

# Or use the separate cert key flags
conduit --cert ~/.conduit-cert.pem --key ~/.conduit-key.pem status

```
