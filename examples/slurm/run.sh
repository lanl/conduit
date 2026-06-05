#!/bin/sh
#set -e

SLURM_DOCKER_PATH="/etc/slurm_docker"

# get path of script
SCRIPT_DIR=$( cd -- "$( dirname -- "$0" )" &> /dev/null && pwd )
echo SCRIPT_DIR:$SCRIPT_DIR

# Get "global" variables
. "${SCRIPT_DIR}"/vars.sh

# run slurm docker example images
cd $SCRIPT_DIR
$DOCKER_COMPOSE up -d --force-recreate slurmctld node1 node2
