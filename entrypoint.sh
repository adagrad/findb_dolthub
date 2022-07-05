#!/bin/sh -l

dolt config --global --add user.email action@github.com
dolt config --global --add user.name "github action"
dolt config --global --add user.creds "$DOLTHUB_SECRET"

# clone schema branch
dolt clone "$REPO/$DATABASE" --branch=schema && cd $DATABASE

# branch off into a data branch
BRANCH=`echo $RANDOM | md5sum | head -c 20`
BRANCH="$1/$2/$BRANCH"
dolt checkout -b "$BRANCH"

# enable tor proxies
tor -f /etc/tor/torrc.default &

# run command
python main.py yfinance symbol --time 150 --dolt-load

# commit changes and push data branch
dolt add .
dolt commit -m"gh action"
dolt push --set-upstream origin "$BRANCH"

