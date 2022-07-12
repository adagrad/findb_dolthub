#!/usr/bin/env bash

set -e

# show the command we intend executing
echo "merge <$1> into <main>"

# prepare dolt environment
echo dolt config
echo "$DOLTHUB_SECRET_JWT" > /tmp/$DOLTHUB_SECRET.jwk
dolt creds import /tmp/$DOLTHUB_SECRET.jwk

dolt login $DOLTHUB_SECRET
dolt config --list

# clone repository
#dolt clone "$REPO/$DATABASE" && cd "$DATABASE"
cd "$DATABASE"

# merge, commit, push, delete
echo pull main
dolt branch -d main
dolt fetch origin main
dolt checkout main

#echo dolt fetch origin "$1"
#dolt fetch origin "$1"

echo dolt merge origin/$1
dolt merge "$1" | grep "CONFLICT"
if [ $? -eq 0 ]; then
    echo resolve conflicts using theirs
    dolt conflicts resolve --theirs .
fi

echo dolt commit "$DOLT_COMMIT_ARGS"
dolt add . || true
dolt commit $DOLT_COMMIT_ARGS

echo dolt push main
dolt push

echo delete remote
dolt push origin ":$1"
