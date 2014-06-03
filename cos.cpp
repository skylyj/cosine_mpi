#include <vector>
#include <map>
#include <algorithm>
#include <boost/foreach.hpp>
#include <boost/mpi.hpp>
#include "types.h"
bool compare(const std::pair<int,float> & a, const std::pair<int,float> &b){
      return a.second < b.second;   //升序排列，如果改为return a>b，则为降序
}
void sim_update(DataSet &sims,const int & user,std::map<int,float> isims,const int & topk,const float &sim_bar){
  auto & usims=sims[user];
  BOOST_FOREACH(auto &ipair,isims){
    if (usims.size()<topk) {
      usims.push_back(ipair);
    }
    else {
      if (usims.size()==topk){
        std::make_heap(usims.begin(),usims.end(),compare);
      }
      auto & small = usims.front();
      if (ipair.second > small.second) {
        std::pop_heap (usims.begin(),usims.end(),compare); usims.pop_back();
        usims.push_back(ipair);
        std::push_heap(usims.begin(),usims.end(),compare);
      }
    }
  }
}

void cal_sim(boost::mpi::communicator world, const DataSet& data_a,const DataSet & data_b_inv,\
             DataSet &sims,const int &topk=100, const float &sim_bar=0){
  BOOST_FOREACH(auto & d, data_a){
    auto & user = d.first;
    auto & info = d.second;
    float fscore = 0;
    std::map<int,float> isims;
    BOOST_FOREACH(auto & i, info){
      auto & item = i.first;
      auto & score = i.second;
      auto it = data_b_inv.find(item);
      if (it != data_b_inv.end()){
        auto & oinfo = it->second;
        BOOST_FOREACH(auto & oi, oinfo){
          auto & ouser = oi.first;
          auto & oscore = oi.second;
          isims[ouser] += score* oscore;
        }
      }
    }
    sim_update(sims,user,isims,topk,sim_bar);
  }
}
