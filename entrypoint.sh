#!/bin/sh -l

dolt config --local --add user.email action@github.com
dolt config --local --add user.name "github action"
dolt config --local --add user.creds $DOLTHUB_SECRET

dolt clone "$REPO/$DATABASE" --branch=schema

echo "Hello $1"
time=$(date)

echo "::set-output name=time::$time"
