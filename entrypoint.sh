#!/usr/bin/env bash

set -e

# show the command we intend executing
echo fin-get "$@"

# puke out hidden state
FILE=fin.db.sqlite
if [ -f "$FILE" ]; then
    echo "WARNING sqlite $FILE already exists, do nothing!"
else
    echo "export hidden state file $FILE"
    # cp -u /fin.meta.db.sqlite .
    lrzip -d "/$FILE.lrz" -o "$FILE"
fi

echo "current directory: `pwd`"
ls -l

# run command
echo run command: "$@"
fin-get "$@"

# compress back hidden state
echo compress back for import as hidden state
lrzip -o "$FILE.lrz" "$FILE"
