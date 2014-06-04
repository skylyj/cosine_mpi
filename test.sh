#!/bin/sh
set -ex
mpirun -n 4 ./main  --sim_bar 0.1 --topk 10 --outpath0 ../out0 --outpath ../out --datapath ../data 
