#include <vector>
#include <map>
#include <algorithm>
#include <boost/foreach.hpp>
#include <boost/mpi.hpp>
#include <boost/date_time.hpp>
#include <boost/format.hpp>
#include "types.h"
void solo_log(const int & irank,const int &prank,boost::format & info);
inline bool compare(const std::pair<int,double> & a, const std::pair<int,double> &b){
      return a.second > b.second;   //升序排列，如果改为return a>b，则为降序
}

void sort_sims(DataSet &sims){
  BOOST_FOREACH(auto &d,sims){
    auto &user=d.first;
    auto &info=d.second;
    std::sort(info.begin(),info.end(),compare);
  }
}

inline void sim_update(std::vector<std::pair<int, double> > & usims,\
                const std::map<int,double> &isims,const int & topk,const double &sim_bar){
  // 对某个user的sim vector 来做更新
  BOOST_FOREACH(auto &ipair,isims){
    if (usims.size()<topk) {
      usims.push_back(ipair);
      if (usims.size()==topk){
        std::make_heap(usims.begin(),usims.end(),compare);
      }
    }
    else {
      auto & small = usims.front();
      if (ipair.second > small.second) {
        std::pop_heap (usims.begin(),usims.end(),compare); 
        usims.pop_back();
        usims.push_back(ipair);
        std::push_heap(usims.begin(),usims.end(),compare);
      }
    }
  }
}

void cal_sim(const boost::mpi::communicator &world, const DataSet& data_a,const DataSet & data_b_inv,\
             DataSet &sims,const int &topk, const double &sim_bar){
  BOOST_FOREACH(auto & d, data_a){
    auto & user = d.first;
    auto & info = d.second;
    std::map<int,double> isims;
    BOOST_FOREACH(auto & i, info){
      auto & item = i.first;
      auto & score = i.second;
      auto it = data_b_inv.find(item);
      if (it == data_b_inv.end()) continue;
      auto & oinfo = it->second;
      BOOST_FOREACH(auto & oi, oinfo){
        auto & ouser = oi.first;
        auto & oscore = oi.second;
        isims[ouser] += score* oscore;
      }
    }
    sim_update(sims[user],isims,topk,sim_bar);
  }
}
