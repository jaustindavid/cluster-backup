#!/bin/sh


mkdir -p test
# create some source data
size=1 # mb
for source in 1 2 3 
do
  mkdir -p test/source${source}
  for file in $(seq 1 20)
  do
    echo $source : $file : $size
    size=$((size + 5))
    dd if=/dev/zero of=test/source${source}/file${file} bs=1024k count=$size
  done
done
