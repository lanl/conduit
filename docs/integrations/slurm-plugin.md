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
    --output /etc/conduit/ \
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

There are three useful views when monitoring a job that uses the Conduit Slurm burst buffer plugin:

1. Slurm job state: whether the job is pending, running, staging out, held, or complete.
2. Slurm burst buffer state: where the job is in the burst buffer lifecycle.
3. Conduit transfer state: the status of each Conduit transfer started by the plugin.

### Quick Status Check

Use `squeue` for a compact view of the job state, pending reason, node allocation, and working directory:

```bash
squeue -j <job_id> -o "%.18i %.10T %.24r %.30R %.80Z"
```

Example:

```bash
[testuser@slurm ~]$ squeue -j 10 -o "%.18i %.10T %.24r %.30R %.80Z"
JOBID              STATE      REASON                   NODELIST(REASON)               WORK_DIR
10                 PENDING    BurstBufferStageIn       (BurstBufferStageIn)           /home/testuser
```

The `REASON` field is especially useful while a job is waiting for stage-in. For example, `BurstBufferStageIn` indicates that Slurm is waiting for the burst buffer plugin to finish staging input data before the job can run.

### Detailed Slurm Job Status

Use `scontrol show job` to see the Slurm job state and burst buffer state together:

```bash
scontrol show job <job_id>
```

A focused view can be produced with:

```bash
scontrol show job <job_id> | tr ' ' '\n' | egrep '^(JobId|JobState|Reason|BurstBufferState|WorkDir|Command|StdOut|StdErr|SubmitTime|EligibleTime|StartTime|EndTime|ExitCode)='
```

Example:

```bash
[testuser@slurm ~]$ scontrol show job 10 | tr ' ' '\n' | egrep '^(JobId|JobState|Reason|BurstBufferState|WorkDir|Command|ExitCode)='
JobId=10
JobState=PENDING
Reason=BurstBufferStageIn
BurstBufferState=staging-in
WorkDir=/home/testuser
Command=/home/testuser/job.batch
ExitCode=0:0
```

### Slurm Burst Buffer Plugin Status

Use `scontrol show burst` to see Slurm's global burst buffer view:

```bash
scontrol show burst
```

Depending on the cluster configuration, this may show the loaded burst buffer plugin, configured pools, allocated buffers, and per-user usage.

Example:

```bash
[root@slurm ~]# scontrol show burst
Name=lua DefaultPool=(null) Granularity=1 TotalSpace=0 FreeSpace=0 UsedSpace=0
  Flags=TeardownFailure
  StageInTimeout=86400 StageOutTimeout=86400 ValidateTimeout=5 OtherTimeout=300
```

This command is useful for checking whether Slurm has loaded the Lua burst buffer plugin and whether Slurm is tracking any burst buffer resources. It does not show detailed Conduit transfer state for each transfer. Use `scontrol show bbstat conduit <job_id>` for that.

### Conduit Transfer Status Through Slurm

Use `scontrol show bbstat` to query the Conduit plugin status for a specific Slurm job:

```bash
scontrol show bbstat conduit <job_id>
```

This calls the plugin's `slurm_bb_get_status` function. The Conduit Slurm plugin returns all Conduit transfers associated with the Slurm job and displays them in a table.

Example:

```bash
[testuser@slurm ~]$ scontrol show bbstat conduit 10
TRANSFER_ID                           STATE                     ERROR
49d1e99a-a72e-413f-877d-64ccbfc917f5  TRANSFER_DATA_TRANSFERRING ERROR_NONE
069d76c4-989c-4ef5-8722-72ef12d0eaa2  TRANSFER_FINALIZED        ERROR_NONE
```

After all transfers have completed, the output should show each transfer in `TRANSFER_FINALIZED` with `ERROR_NONE`:

```bash
[testuser@slurm ~]$ scontrol show bbstat conduit 10
TRANSFER_ID                           STATE                     ERROR
c52603c5-aba0-4404-b6a0-d472ad7ea660  TRANSFER_FINALIZED        ERROR_NONE
69eb3a9c-8a2b-46aa-bca0-56cd2020d0bd  TRANSFER_FINALIZED        ERROR_NONE
606cdec9-0ffd-49b8-b13d-566b4e609a00  TRANSFER_FINALIZED        ERROR_NONE
49d1e99a-a72e-413f-877d-64ccbfc917f5  TRANSFER_FINALIZED        ERROR_NONE
069d76c4-989c-4ef5-8722-72ef12d0eaa2  TRANSFER_FINALIZED        ERROR_NONE
```

If there are no Conduit transfers associated with the requested job, the command prints:

```text
No Conduit transfers found for Slurm job <job_id>
```

The transfer IDs shown in `scontrol show bbstat` output can be used with these CLI commands for more detailed diagnostics.

### Detailed Conduit Diagnostics

The `scontrol show bbstat conduit <job_id>` command is intended to provide a concise transfer summary. For detailed information about a specific transfer, use the Conduit CLI:

```bash
conduit describe <transfer_id>
```

For example:

```bash
conduit describe 49d1e99a-a72e-413f-877d-64ccbfc917f5
```

To list transfers associated with a Slurm job directly through Conduit, query by the comment prefix used by the plugin:

```bash
conduit status 'SLURMJOB:<job_id>,'
```

Example:

```bash
conduit status 'SLURMJOB:10,'
```

The trailing comma is intentional. It prevents matching jobs with similar prefixes, such as matching `SLURMJOB:100` when querying for `SLURMJOB:10`.

## See Also

- [Conduit Installation](../installation.md)
- [Conduit Command Usage](../usage/conduit-cli-usage.md)
- [Slurm Burst Buffer Documentation](https://slurm.schedmd.com/burst_buffer.html)
