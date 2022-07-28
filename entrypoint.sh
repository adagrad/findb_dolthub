#!/usr/bin/env bash

set -e

# show the command we intend executing
echo fin-get "$@"
mkdir -p findb && cd findb
pwd

# prepare dolt environment
echo dolt config
echo "$DOLTHUB_SECRET_JWT" > /tmp/$DOLTHUB_SECRET.jwk
dolt creds import /tmp/$DOLTHUB_SECRET.jwk

dolt login $DOLTHUB_SECRET
dolt config --list

# run command
fin-get "$@"
