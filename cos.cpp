#include <vector>
#include <boost/unordered_map.hpp>
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
                       const int & ouser,const double &osim,
                       const int & topk){
  // 对某个用户和其他用户的相似性对做更新
  // add pair <ouser,osim> to usims
  std::pair<int,double> ipair(ouser,osim);
  // 对某个user的sim vector 来做更新
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

void cal_sim(const boost::mpi::communicator &world, const std::vector<int> & users,
             const DataSet& data_a,const DataSet & data_b_inv,\
             DataSet &sims,const int &topk, const double &sim_bar, const int &intersect_bar){
  // users here is for openmp parallel
  int n = users.size();
#pragma omp parallel for num_threads(10)
  for(int i = 0;i < n;i++){
    auto user = users[i];
    auto & info = data_a.find(user)->second;
    boost::unordered_map<int,std::pair<int,double> >isims;
    BOOST_FOREACH(auto & i, info){
      auto & item = i.first;
      auto & score = i.second;
      auto it = data_b_inv.find(item);
      if (it == data_b_inv.end()) continue;
      auto & oinfo = it->second;
      BOOST_FOREACH(auto & oi, oinfo){
        auto & ouser = oi.first;
        auto & oscore = oi.second;
        isims[ouser].second += score* oscore;
        isims[ouser].first += 1;
      }
    } // got similar items for user, add it to global result sims
    BOOST_FOREACH(auto &i,isims){
      auto &ouser = i.first;
      auto &osim = i.second.second;
      auto &intersects = i.second.first;
      if (intersects < intersect_bar) continue;
      if (osim < sim_bar) continue;
      sim_update(sims[user],ouser,osim,topk);
    } 
  }
}
