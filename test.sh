#!/bin/sh
set -ex
mpic++ --std=c++11 -lboost_mpi -lboost_serialization main.cpp parallel_read.cpp all_to_all.cpp utils.cpp cos.cpp -o main
mpirun -n 3 ./main
