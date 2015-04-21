#include <boost/mpi.hpp>
#include <boost/serialization/unordered_map.hpp>
#include "types.h"
void chain_pass_ball(boost::mpi::communicator world,DataSet &data){
  boost::mpi::request reqs[2];
  int chaintag = 1;
  int dest = (world.rank()+1) %world.size();
  reqs[0]=world.isend(dest,chaintag,data);
  int from = (world.rank()-1+world.size()) %world.size();
  reqs[1]=world.irecv(from,chaintag,data);
  boost::mpi::wait_all(reqs, reqs + 2);
}
