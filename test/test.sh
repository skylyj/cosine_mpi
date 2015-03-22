#!/bin/sh
set -ex
(cd ../; make)
mpirun -n 5 ../main  --sim_bar 0.1 --topk 10 --outpath0 ./debug --outpath ./res --datapath_a ./data --datapath_b ./data \
       --outpath_load ./load
# cat ./res/*|ruby -ne 'a=$_.split; puts $_ if a[0].to_i == 1000'
#cat ./res/*|sort -n -k1
#cat ./load/*
