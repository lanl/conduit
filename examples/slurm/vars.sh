#!/bin/sh

# Location of docker binary. Prepend sudo here if running user is not in the
# 'docker' group.
export DOCKER="docker"
# What to use for 'docker-compose'.
export DOCKER_COMPOSE="${DOCKER} compose"

export SLURM_VER=25.11.5
