#!/bin/bash

sudo -u munge /usr/sbin/munged
/usr/local/sbin/slurmrestd -u slurmrest -g slurmrest -vvvvvvvvvvvvvv