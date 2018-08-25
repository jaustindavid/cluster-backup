#!/bin/sh

echo "removing state files"
find ~/src/cb/test -name .cb\*.json\* -print -exec rm {} \;

echo "removing backup stuff"
rm -vr ~/src/cb/test/backup?/*
