#!/bin/sh

echo "removing state files"
find ~/src/cb/test -name .cb\*.json\* -print -exec rm {} \;
rm /tmp/*.json.bz2

echo "removing backup stuff"
rm -r ~/src/cb/test/backup?/*
