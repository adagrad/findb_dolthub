#!/usr/bin/env bash

set -e

# show the command we intend executing
echo fin-get $1 $2 $3

# prepare dolt environment
echo dolt config
echo "$DOLTHUB_SECRET_JWT" > /tmp/$DOLTHUB_SECRET.jwk
dolt creds import /tmp/$DOLTHUB_SECRET.jwk

dolt login $DOLTHUB_SECRET
dolt config --list

# clone schema branch
echo dolt init and fetch "$REPO/$DATABASE"
mkdir $DATABASE && cd $DATABASE

dolt init
dolt config --list
dolt remote add origin https://doltremoteapi.dolthub.com/$REPO/$DATABASE
dolt fetch origin schema
dolt checkout schema

# branch off into a data branch
BRANCH=`echo $RANDOM | md5sum | head -c 20`
BRANCH="$1/$2/$BRANCH"

echo dolt checkout -b "$BRANCH"
dolt checkout -b "$BRANCH"

# enable tor proxies
# tor -f /etc/tor/torrc.default &

# run command
fin-get $1 $2 $3

# commit changes and push data branch
dolt add .
dolt commit -m"gh action"
dolt push --set-upstream origin "$BRANCH"

# set output variable containing the branch we have worked on
echo "::set-output name=branch::$BRANCH"