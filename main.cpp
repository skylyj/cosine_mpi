#include <mpi.h>
#include <boost/mpi.hpp>
#include <boost/foreach.hpp>
#include <iostream>
#include <sstream>
#include <fstream>
#include <algorithm>
#include <boost/algorithm/string.hpp>
#include "types.h"
void parallel_read(MPI_File *in, const int rank,const int size,const int overlap, std::map<int,DataSet> &load_data);
void all_to_all(boost::mpi::communicator world,std::map<int,DataSet> &load_data,DataSet&data );
void cal_sim(boost::mpi::communicator world, const DataSet& data_a,const DataSet & data_b_inv,
               DataSet &sims,const int &topk=100, const float &sim_bar=0);
void chain_pass_ball(boost::mpi::communicator world,DataSet &data);
void transpose_data(const DataSet &indata,DataSet &outdata);
void parallel_dump(const std::string &outpath,const int &rank, const std::map<int,DataSet> &data);
void parallel_dump(const std::string &outpath,const int &rank, const DataSet &data);
void normalize_data(DataSet &data);
int main(int argc, char **argv) {
  int ierr;
  int rank,size;
  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);
  MPI_File in;
  boost::mpi::communicator world(MPI_COMM_WORLD, boost::mpi::comm_attach);
  ierr = MPI_File_open(MPI_COMM_WORLD, "../data", MPI_MODE_RDONLY, MPI_INFO_NULL, &in);
  if (ierr) {
    if (rank == 0) fprintf(stderr, "%s: Couldn't open file %s\n", argv[0], argv[1]);
    MPI_Finalize();
    exit(2);
  }

  std::map<int,DataSet> load_data;
  const int overlap = 100;
  parallel_read(&in, world.rank(), world.size(), overlap,load_data);
  MPI_File_close(&in);
  parallel_dump("../out0",world.rank(),load_data);

  DataSet data;
  all_to_all(world,load_data,data);
  normalize_data(data);

  DataSet data_inv;
  transpose_data(data,data_inv);

  DataSet sims;
  cal_sim(world, data,data_inv, sims,5);
  for (int i=1;i<world.size();i++){
    chain_pass_ball(world,data_inv);
    cal_sim(world, data,data_inv, sims,5);
    if (world.rank()==0){
      std::cout<<"round "<<i<<std::endl;
      BOOST_FOREACH(auto &d,sims) {
        BOOST_FOREACH(auto &i,d.second) {
          std::cout<<i.first<<"|";
        }
        std::cout<<std::endl;
        break;
      }
    }
  }
  parallel_dump(std::string("../out"),world.rank(),sims);
  MPI_Finalize();
  return 0;
}
