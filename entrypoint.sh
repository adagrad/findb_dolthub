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

tor -f /etc/tor/torrc.default

# run command
#  TODO ...
echo "Hello $1"
time=$(date)

# load data and
#  TODO load csv: dolt table import -u "$1_$2" ../pull/yfsymbols.csv
#   mabye we can force a structure that the command need to return a list of table and file path combination to load the data

# commit changes and push data branch
dolt add .
dolt commit -m"gh action"
dolt push --set-upstream origin "$BRANCH"

echo "::set-output name=time::$time"
