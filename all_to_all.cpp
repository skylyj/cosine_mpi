#include <boost/mpi.hpp>
#include <iostream>
#include <boost/serialization/string.hpp>
#include <boost/serialization/map.hpp>
#include <boost/serialization/utility.hpp>
#include <boost/foreach.hpp>
#include "types.h"
namespace mpi = boost::mpi;
void all_to_all(mpi::communicator world,std::map<int,DataSet> &load_data, DataSet &data){
  mpi::request reqs[3];
  int sendtag = 0;

  for (int i=1; i<world.size(); i++) {
    int dest = (world.rank()+i) %world.size();
    reqs[i-1]=world.isend(dest,sendtag,load_data[dest]);
  }

  mpi::request reqsr[3];
  for (int i=1; i<world.size(); i++) {
    int from = (world.rank()+i) %world.size();
    reqsr[i-1]=world.irecv(from,sendtag,load_data[from]);
  }

  mpi::wait_all(reqs, reqs + 3);
  mpi::wait_all(reqsr, reqsr + 3);
  BOOST_FOREACH(auto &load,load_data){
    BOOST_FOREACH(auto &d,load.second){
      auto user=d.first;
      auto info=d.second;
      BOOST_FOREACH(auto &i,info){
        data[user].push_back(i);
      }
    }
  }
  load_data.clear();
}



void chain_pass_ball(mpi::communicator world,DataSet &data){
  mpi::request reqs[2];
  int sendtag = 0;
  int dest = (world.rank()+1) %world.size();
  reqs[0]=world.isend(dest,sendtag,data);
  int from = (world.rank()-1) %world.size();
  reqs[1]=world.irecv(from,sendtag,data);
  mpi::wait_all(reqs, reqs + 2);
}
