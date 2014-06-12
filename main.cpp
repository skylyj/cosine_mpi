#include <mpi.h>
#include <boost/mpi.hpp>
#include <boost/foreach.hpp>
#include <iostream>
#include <sstream>
#include <fstream>
#include <algorithm>
#include <boost/algorithm/string.hpp>
#include <boost/program_options.hpp>
#include <boost/date_time.hpp>
#include <boost/format.hpp>
#include <boost/mpi/collectives.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/unordered_map.hpp>
#include <boost/serialization/utility.hpp>
#include "types.h"
void parallel_read(MPI_File *in, const int rank,const int size,const int overlap, std::vector<DataSet> &load_data);
void cal_sim(const boost::mpi::communicator &world, const std::vector<int> &users,
             const DataSet& data_a,const DataSet & data_b_inv,
             DataSet &sims,const int &topk, const double &sim_bar);
void chain_pass_ball(boost::mpi::communicator world,DataSet &data);
void transpose_data(const DataSet &indata,DataSet &outdata);
void parallel_dump(const std::string &outpath,const int &rank, const std::vector<DataSet> &data);
void parallel_dump(const std::string &outpath,const int &rank, const DataSet &data);
void normalize_data(DataSet &data);
void solo_log(const int & irank,const int &prank,const std::string & info);
void solo_log(const int & irank,const int &prank,boost::format & info);
void construct_data(const std::vector<DataSet> &indata, DataSet &outdata);
std::string info2str(const std::vector<std::pair<int,double> >& info);
void sort_sims(DataSet &sims);
using namespace boost::program_options;

int main(int argc, char **argv) {
  options_description desc("Allowed options");
  double sim_bar;int topk;
  std::string outpath0,outpath,datapath;
  desc.add_options()
    ("help", "Use --help or -h to list all arguments")
    ("sim_bar", value<double>(&sim_bar)->default_value(0), "Use --sim_bar 0.01")
    ("topk", value<int>(&topk)->default_value(10), "Use --topk 10")
    ("datapath", value<std::string>(&datapath), "datapath")
    ("outpath0", value<std::string>(&outpath0), "outpath0")
    ("outpath", value<std::string>(&outpath), "outpath")
    ;

  variables_map vm;        
  store(parse_command_line(argc, argv, desc), vm);
  notify(vm);    

  if (vm.count("help")) {
    std::cout << desc;
    return 0;
  }
  int rank,size;
  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);
  MPI_File in;
  boost::mpi::communicator world(MPI_COMM_WORLD, boost::mpi::comm_attach);

  if (world.rank()==0) {
    std::string paramstr="sim_bar topk datapath outpath0 outpath";
    std::vector<std::string> params;
    split(params, paramstr, boost::is_any_of(" "), boost::token_compress_on);
    BOOST_FOREACH(auto &para,params){
      if (vm.count(para)==0){
        std::cout<<para<<" required!!\n";
        exit(2);
      }
    }
    std::cout << "sim_bar = " << sim_bar<<", topk= " << topk
              <<", datapath = "<< datapath <<", outpath0 = "<< outpath0 <<", out= " <<outpath<<std::endl;
  }

  int ierr = MPI_File_open(MPI_COMM_WORLD, datapath.c_str(), MPI_MODE_RDONLY, MPI_INFO_NULL, &in);
  if (ierr) {
    if (rank == 0) fprintf(stderr, "%s: Couldn't open file %s\n", argv[0], argv[1]);
    MPI_Finalize();
    exit(2);
  }

  boost::posix_time::ptime start = boost::posix_time::second_clock::local_time();
  solo_log(world.rank(),0,boost::format("start time %1% ...\n") %start);

  std::vector<DataSet> load_data;
  load_data.resize(world.size());
  const int overlap = 100;

  solo_log(world.rank(),0,boost::format("parallel_read start at  %1% ...") %boost::posix_time::second_clock::local_time());
  parallel_read(&in, world.rank(), world.size(), overlap,load_data);
  MPI_File_close(&in);
  // parallel_dump(outpath0,world.rank(),load_data);

  std::vector<DataSet> idata;
  idata.resize(world.size());
  solo_log(world.rank(),0,boost::format("all_to_all start at  %1% ...") %boost::posix_time::second_clock::local_time());
  all_to_all(world,load_data,idata);
  load_data.clear();

  DataSet data;
  solo_log(world.rank(),0,boost::format("construct start at  %1% ...") %boost::posix_time::second_clock::local_time());
  construct_data(idata,data);
  solo_log(world.rank(),0,boost::format("normalize start at  %1% ...") %boost::posix_time::second_clock::local_time());
  normalize_data(data);

  DataSet data_inv;
  solo_log(world.rank(),0,boost::format("transpose start at  %1% ...") %boost::posix_time::second_clock::local_time());
  transpose_data(data,data_inv);

  DataSet sims;
  std::vector<int> users(data.size());
  {
    int i = 0;
    BOOST_FOREACH(auto &uinfo, data) {
      auto u = uinfo.first;
      users[i++] = u;
      sims[u]; // force insertion in a single thread ...
    }
  }

  solo_log(world.rank(),0,boost::format("cal_sim start at  %1% ...") %boost::posix_time::second_clock::local_time());
  cal_sim(world,users, data,data_inv, sims,topk,sim_bar);
  for (int i=1;i<world.size();i++){
    chain_pass_ball(world,data_inv);
    solo_log(world.rank(),0,boost::format("after passing ball;round %1% ...%2%") \
             %i %boost::posix_time::second_clock::local_time());
    cal_sim(world,users, data,data_inv, sims,topk,sim_bar);
      // solo_log(world.rank(),0,boost::format("round %1% ...%2%") %i %info2str(sims[1000070])); 
  }
  sort_sims(sims);  
  // solo_log(world.rank(),0,boost::format(" ...%1%")  %info2str(sims[1000070]));       
  parallel_dump(outpath,world.rank(),sims);

  boost::posix_time::ptime end = boost::posix_time::second_clock::local_time();
  solo_log(world.rank(),0,boost::format("end time %1%elapsed time %2%") %start %(end-start));

  MPI_Finalize();
  return 0;
}
