#!/bin/sh
set -e

if [ $(id -u) -ne 0 ]; then
	echo "Please run as root." >&2
	exit 1
fi

SLURM_DOCKER_PATH="/etc/slurm_docker"

# get path of script
SCRIPT_DIR=$( cd -- "$( dirname -- "$0" )" &> /dev/null && pwd )
echo SCRIPT_DIR:$SCRIPT_DIR

# Get "global" variables
. "${SCRIPT_DIR}"/vars.sh

# create slurm_docker area
mkdir -p $SLURM_DOCKER_PATH
mkdir -p $SLURM_DOCKER_PATH/munge

# generate ssh keys for testuser
mkdir -p $SLURM_DOCKER_PATH/ssh/testuser
rm -f $SLURM_DOCKER_PATH/ssh/testuser/id_rsa
rm -f $SLURM_DOCKER_PATH/ssh/testuser/id_rsa.pub
ssh-keygen -f $SLURM_DOCKER_PATH/ssh/testuser/id_rsa -C testuser@slurm_docker_example -q -N ""
cat $SLURM_DOCKER_PATH/ssh/testuser/id_rsa.pub > $SLURM_DOCKER_PATH/ssh/testuser/authorized_keys
chmod 600 $SLURM_DOCKER_PATH/ssh/testuser/authorized_keys
chown -R 1433:1433 $SLURM_DOCKER_PATH/ssh/testuser

# Check for Docker
$DOCKER --version || echo "Docker not found: '$DOCKER'" >&2

# build slurm-docker images
cd $SCRIPT_DIR/../
$SCRIPT_DIR/../build.sh
cd $SCRIPT_DIR

# generate munge key
chmod 777 $SLURM_DOCKER_PATH/munge
$DOCKER run --rm -v $SLURM_DOCKER_PATH/munge:/etc/munge mungekey:latest --force
chmod 755 $SLURM_DOCKER_PATH/munge

# build and run slurm_docker example images
cd $SCRIPT_DIR
$DOCKER_COMPOSE build

# Correct permissions on munge
chown 994:994 -R "${SLURM_DOCKER_PATH}"/munge


# Verify cgroupsv2; otherwise, /sys/fs/cgroups gets mounted readonly in
# containers erroneously.
# See: https://access.redhat.com/documentation/en-us/red_hat_enterprise_linux/8/html/managing_monitoring_and_updating_the_kernel/using-cgroups-v2-to-control-distribution-of-cpu-time-for-applications_managing-monitoring-and-updating-the-kernel
mount | grep -q 'cgroup2 on /sys/fs/cgroup' || echo "WARNING: cgroupsv2 is not being used; make sure 'systemd.unified_cgroup_hierarchy=1' is in the kernel parameters. For Red Hat derivatives, use: 'sudo grubby --update-kernel=ALL --args=\"systemd.unified_cgroup_hierarchy=1\"'"
