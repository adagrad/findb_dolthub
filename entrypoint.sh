#!/usr/bin/env bash

set -e

# show the command we intend executing
echo fin-get "$@"

echo "current directory: `pwd`"
ls -l

# run command
echo run command: "$@"
fin-get "$@"
