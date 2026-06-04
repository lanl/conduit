#!/bin/sh
set -e

DEFAULT_SLURM_VER=24.05.8
SLURM_VER="${SLURM_VER:-$DEFAULT_SLURM_VER}"
MUNGE_VER=0.5.16

echo munge-build:
$DOCKER build \
	-t munge-build:$MUNGE_VER \
	-f Dockerfile.munge-build \
	--build-arg MUNGE_VER=$MUNGE_VER \
	.
echo mungekey:
$DOCKER build \
	-t mungekey:$MUNGE_VER \
	-t mungekey:latest \
	-f Dockerfile.mungekey \
	--build-arg MUNGE_VER=$MUNGE_VER \
	.
echo munged:
$DOCKER build \
	-t munged:$MUNGE_VER \
	-f Dockerfile.munged \
	--build-arg MUNGE_VER=$MUNGE_VER \
	.
echo slurm-build:
$DOCKER build \
	-t slurm-build:$SLURM_VER \
	-f Dockerfile.slurm-build \
	--build-arg MUNGE_VER=$MUNGE_VER \
	--build-arg SLURM_VER=$SLURM_VER \
	.
echo slurmctld:
$DOCKER build \
	-t slurmctld:$SLURM_VER \
	-f Dockerfile.slurmctld \
	--build-arg MUNGE_VER=$MUNGE_VER \
	--build-arg SLURM_VER=$SLURM_VER \
	.
echo slurmd:
$DOCKER build \
	-t slurmd:$SLURM_VER \
	-f Dockerfile.slurmd \
	--build-arg MUNGE_VER=$MUNGE_VER \
	--build-arg SLURM_VER=$SLURM_VER \
	.
echo slurmrestd:
$DOCKER build \
	-t slurmrestd:$SLURM_VER \
	-f Dockerfile.slurmrestd \
	--build-arg MUNGE_VER=$MUNGE_VER \
	--build-arg SLURM_VER=$SLURM_VER \
	.
