#!/usr/bin/env bash

set -e

# show the command we intend executing
eval "commit_args=($DOLT_COMMIT_ARGS)"
echo "merge <$1> into <main>, commit as ${commit_args[@]}"

# prepare dolt environment
echo dolt config
echo dolt version
echo "$DOLTHUB_SECRET_JWT" > /tmp/$DOLTHUB_SECRET.jwk
dolt creds import /tmp/$DOLTHUB_SECRET.jwk

dolt login $DOLTHUB_SECRET
dolt config --list

# clone repository
echo dolt clone
dolt clone "$REPO/$DATABASE" && cd "$DATABASE"

echo dolt fetch origin "$1"
dolt fetch origin "$1"

echo dolt merge origin/$1
set +e  # allow grep to fail
dolt merge "origin/$1" | grep "CONFLICT"
if [ $? -eq 0 ]; then
    echo resolve conflicts using theirs
    dolt conflicts resolve --theirs .
fi

set -e
echo dolt commit "${commit_args[@]}"
dolt add . || true
dolt commit "${commit_args[@]}"

echo dolt push main
dolt push

echo delete remote
dolt push origin ":$1"
