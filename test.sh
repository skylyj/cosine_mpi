#!/bin/sh
set -ex
mpic++ -o main --std=c++11 -lboost_filesystem -lboost_system \
	-lboost_mpi -lboost_serialization -lboost_program_options \
    ./all_to_all.cpp ./cos.cpp ./main.cpp ./parallel_read.cpp ./utils.cpp
mpirun -n 4 ./main  --sim_bar 0.1 --topk 10 --outpath0 ../out0 --outpath ../out --datapath ../data 
