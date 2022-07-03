#!/bin/sh -l

REPO=adagrad
DATABASE=findb

dolt config --local --add user.email action@github.com
dolt config --local --add user.name "github action"
# dolt config --local --add user.creds <TODO get github secret>

dolt clone "$REPO/$DATABASE" --branch=schema

echo "Hello $1"
time=$(date)

echo "::set-output name=time::$time"
