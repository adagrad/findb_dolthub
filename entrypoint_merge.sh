#!/usr/bin/env bash

set -e

# show the command we intend executing
echo "merge <$1> into <main>"

# clone repository
#dolt clone "$REPO/$DATABASE" && cd "$DATABASE"
cd "$DATABASE"

# merge, commit, push, delete
echo pull main
dolt branch -d main
dolt pull
dolt checkout main

echo fetch and merge remote
dolt fetch origin "$1"
dolt merge "origin/$1"
dolt commit $DOLT_COMMIT_ARGS

echo push main
dolt push

echo delete remote
dolt push origin ":$1"
