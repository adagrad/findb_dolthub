#!/bin/bash

# stop on any error
set -e

if [ "$#" -ne 1 ]
then
  echo A tag needs to be provided i.e. \"latest\"
  exit 1
fi

TAG=$1

# load environment variable
source .env

# login to github registry
# provide a developer setting access token with everything granted under packages as password
docker login ghcr.io -u adagrad -p "$GHCIO"

# build container
echo docker build -t finget:$TAG .
docker build -t finget:$TAG .

# tag and push image to github registry
image=`docker images "finget:$TAG" -q`
echo "docker image = $image"

docker tag $image ghcr.io/adagrad/finget:$TAG
docker push ghcr.io/adagrad/finget:$TAG

git tag $TAG -f && git push --tags -f
