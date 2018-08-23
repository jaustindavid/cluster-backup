#!/bin/sh

echo "removing state files"
find test -name .cb\*.json\* -print -exec rm {} \;

echo "removing backup stuff"
rm -vr test/backup?/*
