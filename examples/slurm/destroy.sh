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

# stop and rm example containers
cd $SCRIPT_DIR
$DOCKER_COMPOSE rm --stop --force 
rm -rf $SLURM_DOCKER_PATH/*
