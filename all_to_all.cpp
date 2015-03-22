#include <boost/mpi.hpp>
#include <iostream>
#include <unordered_map>
#include <boost/serialization/string.hpp>
#include <boost/serialization/map.hpp>
#include <boost/serialization/unordered_map.hpp>
#include <boost/serialization/utility.hpp>
#include <boost/foreach.hpp>
#include <boost/current_function.hpp>
#include "types.h"
#include <boost/format.hpp>
// void all_to_all(boost::mpi::communicator world, std::unordered_map<int,DataSet> &load_data,
//                 DataSet &data){
//   std::vector<boost::mpi::request> reqs;
//   int sendtag = 0;

//   for (int i=1; i<world.size(); i++) {
//     int dest = (world.rank()+i) %world.size();
//     reqs.push_back(world.isend(dest,sendtag,load_data[dest]));
//     std::cout<<world.rank()<<" sended\n";
//   }

//   std::vector<boost::mpi::request> reqsr;
//   for (int i=1; i<world.size(); i++) {
//     int from = (world.rank()+i) %world.size();
//     reqsr.push_back(world.irecv(from,sendtag,load_data[from]));
//     std::cout<<world.rank()<<" received\n";
//   }
//   std::cout<<"all to all waiting\n";
//   boost::mpi::wait_all(reqs.begin(), reqs.end());
//   boost::mpi::wait_all(reqsr.begin(), reqsr.end());
//   std::cout<<"all to all communcation end\n"; 
//   BOOST_FOREACH(auto &load,load_data){
//     BOOST_FOREACH(auto &d,load.second){
//       auto user = d.first;
//       auto info = d.second;
//       BOOST_FOREACH(auto &i,info){
//         data[user].push_back(i);
//       }
//     }
//   }
//   std::cout<<"all to all push back end\n"; 
//   load_data.clear();
// }



void chain_pass_ball(boost::mpi::communicator world,DataSet &data){
  boost::mpi::request reqs[2];
  int chaintag = 1;
  int dest = (world.rank()+1) %world.size();
  reqs[0]=world.isend(dest,chaintag,data);
  int from = (world.rank()-1+world.size()) %world.size();
  reqs[1]=world.irecv(from,chaintag,data);
  boost::mpi::wait_all(reqs, reqs + 2);
}
