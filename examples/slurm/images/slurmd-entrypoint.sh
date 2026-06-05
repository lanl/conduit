#!/bin/bash

runuser -u munge -- /usr/sbin/munged
/usr/local/sbin/slurmd -D