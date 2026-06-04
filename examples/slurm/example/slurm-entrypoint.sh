#!/bin/bash

cp /tmp/slurm.conf /usr/local/etc/slurm.conf

NODE_SPEC=$(/usr/local/sbin/slurmd -C)
regex="CPUs=([0-9]+) Boards=([0-9]+) SocketsPerBoard=([0-9]+) CoresPerSocket=([0-9]+) ThreadsPerCore=([0-9]+) RealMemory=([0-9]+)"

[[ $NODE_SPEC =~ $regex ]]
sed -c -i --regexp-extended "s/(CPUs=)[0-9]+/\1${BASH_REMATCH[1]}/" /usr/local/etc/slurm.conf 
sed -c -i --regexp-extended "s/(Boards=)[0-9]+/\1${BASH_REMATCH[2]}/" /usr/local/etc/slurm.conf 
sed -c -i --regexp-extended "s/(SocketsPerBoard=)[0-9]+/\1${BASH_REMATCH[3]}/" /usr/local/etc/slurm.conf 
sed -c -i --regexp-extended "s/(CoresPerSocket=)[0-9]+/\1${BASH_REMATCH[4]}/" /usr/local/etc/slurm.conf 
sed -c -i --regexp-extended "s/(ThreadsPerCore=)[0-9]+/\1${BASH_REMATCH[5]}/" /usr/local/etc/slurm.conf
sed -c -i --regexp-extended "s/(RealMemory=)[0-9]+/\1${BASH_REMATCH[6]}/" /usr/local/etc/slurm.conf 

cat /usr/local/etc/cgroup.conf

/etc/slurm/slurmctld-entrypoint.sh
