#!/usr/bin/env bash

set -e

# show the command we intend executing
echo fin-get "$@"
pwd

# puke out hidden state
cp -u /fin.meta.db.sqlite .
ls -l

# run command
fin-get "$@"
