#include <mpi.h>
#include <boost/mpi.hpp>
#include <boost/foreach.hpp>
#include <iostream>
#include <sstream>
#include <fstream>
#include "types.h"
namespace mpi = boost::mpi;
void parallel_read(MPI_File *in, const int rank,const int size,const int overlap, std::map<int,DataSet> &load_data);
void all_to_all(mpi::communicator world,std::map<int,DataSet> &load_data,DataSet&data );
void cal_sim(mpi::communicator world, DataSet& data_a,DataSet & data_b_inv, DataSet & sims);
void chain_pass_ball(mpi::communicator world,DataSet &data);
void transpose_data(const DataSet &indata,DataSet &outdata);
int main(int argc, char **argv) {
  int ierr;
  int rank,size;
  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);
  // mpi::environment env;
  MPI_File in;
  mpi::communicator world(MPI_COMM_WORLD, mpi::comm_attach);
  ierr = MPI_File_open(MPI_COMM_WORLD, "../data", MPI_MODE_RDONLY, MPI_INFO_NULL, &in);
  if (ierr) {
    if (rank == 0) fprintf(stderr, "%s: Couldn't open file %s\n", argv[0], argv[1]);
    MPI_Finalize();
    exit(2);
  }

  std::map<int,DataSet> load_data;
  DataSet data;
  DataSet sims;
  const int overlap = 100;
  parallel_read(&in, world.rank(), world.size(), overlap,load_data);
  // for (int i=0;i< world.size();i++){
  //   if (not load_data.count(i)){
  //     std::cout<<i<<"empty"<<std::endl;
  //     load_data[i]=DataSet();
  //   }
  // }
  std::ofstream ofp(std::string("../out0/"+std::to_string(rank)));
    BOOST_FOREACH(auto &h,load_data){
      ofp<<"mod "<<h.first<<"\n";
      BOOST_FOREACH(auto &d,h.second){
        ofp<<"user "<<d.first<<"\t";
        BOOST_FOREACH(auto &i,d.second){
          ofp<<i.first<<":"<<i.second<<"|";
        }
        ofp<<"\n";
      }
      ofp<<"\n\n\n";
    }
  ofp.close();

  all_to_all(world,load_data,data);
  DataSet sims;
  DataSet data_inv;
  transpose_data(data,data_inv);
  for (int i=0;i<world.rank();i++){
    cal_sim(world, data,data_inv, sims);
    chain_pass_ball(world,data);
  }

  std::ofstream ofp1(std::string("../out/"+std::to_string(rank)));
  BOOST_FOREACH(auto &d,sims){
    auto user=d.first;
    auto info=d.second;
    ofp1<<user<<"\t";
    BOOST_FOREACH(auto &i,info){
      ofp1<<i.first<<":"<<i.second<<"|";
    }
    ofp1<<"\n";
  }
  ofp1.close();

  MPI_File_close(&in);
  MPI_Finalize();
  return 0;
}
