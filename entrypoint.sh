#!/usr/bin/env bash

set -e

# show the command we intend executing
echo fin-get "$@"

# puke out hidden state
lrzip -f -d "/$FILE.lrz" -o "$FILE"

echo "current directory: `pwd`"
ls -l

# run command
echo run command: "$@"
fin-get "$@"

# compress back hidden state
# echo compress back for import as hidden state
# lrzip -l -i -f -o "$FILE.lrz" "$FILE"
# lrzip --level 5 -i -f -o "$FILE.lrz" "$FILE"
