# Slurm Burst Buffer Plugin

The Conduit Slurm plugin integrates Conduit data transfer capabilities with Slurm's burst buffer system. This allows users to stage data in and out as part of their Slurm job workflows using special `#CONDUIT_PRE` and `#CONDUIT_POST` directives.

## Installation

### Build from Source

1. Build the Conduit CLI binary:

```bash
git clone https://github.com/lanl/conduit
cd conduit
go build -o conduit ./cmd/cli
```

2. Navigate to the Slurm plugin directory:

```bash
cd integrations/slurm
```

3. Copy the conduit binary:

```bash
cp ../../conduit .
```

4. Build the RPM package:

```bash
make rpm
```

5. Install the RPM:

```bash
sudo rpm -i conduit-slurm-plugin-*.rpm
```

### Manual Installation

If not using RPM:

```bash
# Build the plugin components
cd integrations/slurm
make

# Install files manually
sudo cp burst_buffer.lua /etc/slurm/
sudo cp burst_buffer.conf /etc/slurm/
sudo cp conduit /usr/sbin/
```

## Configuration

### Slurm Configuration

Add the following to your `slurm.conf`:

```conf
# Enable burst buffer plugin
BurstBufferType=burst_buffer/lua
```

### Plugin Configuration

The Lua script (`/etc/slurm/burst_buffer.lua`) includes configuration at the top:

```lua
-- Path to conduit CLI binary
local CONDUIT_CLI = "/usr/sbin/conduit"

-- Authentication certificates
local CONDUIT_CERT = "/etc/slurm/conduit-cert.pem"
local CONDUIT_KEY = "/etc/slurm/conduit-key.pem"
local CONDUIT_CA = "/etc/slurm/conduit-external-ca.pem"

-- Conduit CLI configuration file
local CONDUIT_CLI_CONFIG = "/etc/conduit/conduit-cli-config.yaml"
```

Edit these paths to match your installation.

### Burst Buffer Configuration

Create `/etc/slurm/burst_buffer.conf`:

```conf
# The directive string jobs must use
Directive=CONDUIT

# Teardown burst buffer after staging errors
Flags=TeardownFailure
```

### Certificate Setup

The plugin requires a service certificate for authentication:

```bash
# Generate a service certificate (from conduit server)
conduit-server external-client-cert -d \
    --separate-cert-key \
    --cert-name conduit-slurm-cert.pem \
    --key-name conduit-slurm-key.pem \
    --output /etc/slurm/ \
    --client-commonname conduit-service \
    --expiration 365
```

Copy the CA certificate:

```bash
cp /etc/conduit/keys/conduit-external-ca.pem /etc/slurm/
```

## Usage

### Directive Syntax

Conduit directives follow the same format as a conduit cli command. See [conduit-cli Usage](../usage/conduit-cli-usage.md) for more details:

```bash
#CONDUIT_PRE <command> [flags] <source> <destination>
#CONDUIT_POST <command> [flags] <source> <destination>
```

- `CONDUIT_PRE`: Transfers executed before job starts (stage-in)
- `CONDUIT_POST`: Transfers executed after job completes (stage-out)
- `<command>`: Either `cp` (copy) or `mv` (move)
- `[flags]`: Optional flags like `-r` for recursive
- `<source>`: Source file or directory path
- `<destination>`: Destination path

### Basic Example

```bash
#!/bin/sh
#SBATCH --nodes=1
#SBATCH --time=01:00:00

#CONDUIT_PRE cp /mnt/fs_1/data/input.txt /mnt/fs_2/work/input.txt
#CONDUIT_POST cp /mnt/fs_2/work/output.txt /mnt/fs_1/results/output.txt

# Your computation here
./my_application /mnt/fs_2/work/input.txt /mnt/fs_2/work/output.txt
```

### Directory Transfers

Use the `-r` flag for recursive directory copies:

```bash
#!/bin/sh
#SBATCH --nodes=1
#SBATCH --time=02:00:00

# Stage in entire dataset directory
#CONDUIT_PRE cp -r /archive/project/dataset /campaign/workspace/dataset

# Stage out results directory
#CONDUIT_POST cp -r /campaign/workspace/results /archive/project/results

# Process data
./batch_process /campaign/workspace/dataset /campaign/workspace/results
```

### Multiple Transfers

You can specify multiple directives:

```bash
#!/bin/sh
#SBATCH --nodes=1
#SBATCH --time=02:00:00

# Stage in multiple files
#CONDUIT_PRE cp /mnt/fs_1/foo/hello.txt /mnt/fs_2/bar/
#CONDUIT_PRE cp /mnt/fs_1/foo/source1 /mnt/fs_2/bar/destination
#CONDUIT_PRE cp /mnt/fs_1/foo/source2 /mnt/fs_2/bar/destination
#CONDUIT_PRE cp -r /mnt/fs_1/foo/source3 /mnt/fs_2/bar/destination3

# Stage out results
#CONDUIT_POST mv -r /mnt/fs_2/bar/destination /mnt/fs_1/foo/results

echo "Processing complete"
```

## Job Lifecycle

The plugin manages transfers through Slurm's job stages:

1. **Job Submission** (`slurm_bb_job_process`):
   - Directives are parsed and validated
   - Validation checks ensure transfers will succeed
   - Job is rejected if validation fails

2. **Setup Stage** (`slurm_bb_setup`):
   - Placeholder for future functionality
   - Currently returns immediately

3. **Stage-In Phase** (`slurm_bb_data_in`, `slurm_bb_test_data_in`):
   - All `CONDUIT_PRE` transfers are initiated
   - Job waits in pending state until transfers complete
   - Progress is polled periodically
   - Job fails if any stage-in transfer fails

4. **Job Execution**:
   - Job runs with staged data available
   - Standard job execution

5. **Stage-Out Phase** (`slurm_bb_data_out`, `slurm_bb_test_data_out`):
   - All `CONDUIT_POST` transfers are initiated after job completes
   - Transfers run asynchronously
   - Progress is polled periodically
   - Job marked complete when all stage-out finishes

6. **Teardown Stage** (`slurm_bb_job_teardown`):
   - Aborts any active transfers if job is cancelled
   - Waits for transfers to complete on normal termination
   - Sends final `scancel` to clean up job

## Monitoring

### Check Job Status

Use standard Slurm commands:

```bash
# View job status
squeue -j <job_id>

# View detailed job information
scontrol show job <job_id>
```

### Check Conduit Transfer Status

Query Conduit directly:

```bash
# View status via scontrol bbstat
scontrol show bbstat conduit <job_id>

# Or use conduit CLI directly
conduit status <transfer_id>

# View specific transfer
conduit describe <transfer_id>
```

## See Also

- [Conduit Installation](../installation.md)
- [Conduit Command Usage](../usage/conduit-cli-usage.md)
- [Slurm Burst Buffer Documentation](https://slurm.schedmd.com/burst_buffer.html)
