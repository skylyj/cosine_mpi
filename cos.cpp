#include "types.h"
#include <boost/foreach>
void cal_sim(mpi::communicator world, DataSet& data_a,DataSet & data_b_inv, DataSet & sims){
  BOOST_FOREACH(auto & d, data_a){
    auto & user = d.first;
    auto & info = d.second;
    float fscore = 0;
    BOOST_FOREACH(auto & i, info){
      auto & item = i.first;
      auto & score = i.second;
      auto it = data_b_inv.find(item);
      if (it != data_b_inv.end()){
        oinfo = it->second;
        BOOST_FOREACH(auto & oi, oinfo){
          auto & ouser = oi.first;
          auto & oscore = oi.second;
          sims[user][ouser] += score* oscore;
        }
      }
    }
  }
}
