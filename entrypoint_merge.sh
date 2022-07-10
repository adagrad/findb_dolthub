#!/usr/bin/env bash

set -e

# show the command we intend executing
echo merge $1 into main

# prepare dolt environment
echo dolt config
echo "$DOLTHUB_SECRET_JWT" > /tmp/$DOLTHUB_SECRET.jwk
dolt creds import /tmp/$DOLTHUB_SECRET.jwk

dolt login $DOLTHUB_SECRET
dolt config --list

# clone repository
dolt clone "$REPO/$DATABASE" && cd "$DATABASE"

# merge, commit, push, delete
dolt merge origin "$1"
dolt commit $DOLT_COMMIT_ARGS
dolt push
dolt push origin --delete "$1"
