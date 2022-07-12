#!/bin/bash

# stop on any error
set -e

# load environment variable
source .env

# login to github registry
# provide a developer setting access token with everything granted under packages as password
docker login ghcr.io -u adagrad -p "$GHCIO"

# build container
docker build -t finget .

# tag and push image to github registry
image=`docker images "finget" -q`
docker tag $image ghcr.io/adagrad/finget:latest
docker push ghcr.io/adagrad/finget:latest

git tag v1 -f && git push --tags -f
