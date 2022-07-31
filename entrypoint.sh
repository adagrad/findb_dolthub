#!/usr/bin/env bash

set -e

# show the command we intend executing
echo fin-get "$@"
mkdir -p findb && cd findb
pwd

# puke out hidden state
cp /fin.meta.db.sqlite .
ls -l  fin.meta.db.sqlite

# run command
fin-get "$@"
