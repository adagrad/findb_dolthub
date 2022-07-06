#!/usr/bin/env bash

set -e

# clone schema branch
echo dolt clone "$REPO/$DATABASE"
mkdir $DATABASE && cd $DATABASE
dolt init
dolt remote add origin https://doltremoteapi.dolthub.com/$REPO/$DATABASE
dolt fetch origin schema
dolt checkout schema

# add dolt config
dolt config --local --add user.email 'bot@bot.bot'
dolt config --local --add user.name 'adagrad'
dolt config --local --add user.creds "$DOLTHUB_SECRET"
# dolt config --list

# branch off into a data branch
BRANCH=`echo $RANDOM | md5sum | head -c 20`
BRANCH="$1/$2/$BRANCH"
dolt checkout -b "$BRANCH"

# enable tor proxies
#tor -f /etc/tor/torrc.default &

# run command
#python main.py yfinance symbol --time 150 --dolt-load
fin-get yfinance symbol --help

# commit changes and push data branch

dolt add .
dolt commit -m"gh action"
dolt push --set-upstream origin "$BRANCH"

