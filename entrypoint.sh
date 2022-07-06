#!/usr/bin/env bash

set -e

# clone schema branch
echo dolt clone "$REPO/$DATABASE"
mkdir $DATABASE && cd $DATABASE
dolt init
dolt config --global --add user.email 'bot@bot.bot'
dolt config --global --add user.name 'adagrad'
dolt remote add origin https://doltremoteapi.dolthub.com/$REPO/$DATABASE
dolt fetch origin schema
dolt checkout schema

# add dolt credentials
dolt config --local --add user.creds "$DOLTHUB_SECRET"
# dolt config --list

# branch off into a data branch
BRANCH=`echo $RANDOM | md5sum | head -c 20`
BRANCH="$1/$2/$BRANCH"

echo dolt checkout -b "$BRANCH"
dolt checkout -b "$BRANCH"

# enable tor proxies
# tor -f /etc/tor/torrc.default &

# run command
#python main.py yfinance symbol --time 150 --dolt-load
echo fin-get $INPUT_COMMAND1 $INPUT_COMMAND2 $INPUT_COMMAND_ARGS
fin-get $INPUT_COMMAND1 $INPUT_COMMAND2 $INPUT_COMMAND_ARGS

# commit changes and push data branch
dolt add .
dolt commit -m"gh action"
dolt push --set-upstream origin "$BRANCH"

