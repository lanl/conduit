#!/bin/bash

runuser -u munge -- /usr/sbin/munged
exec runuser -u slurm -- /usr/local/sbin/slurmctld -Dvvv
