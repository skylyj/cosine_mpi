#include <boost/foreach.hpp>
#include "types.h"
#include <cmath>
void transpose_data(const DataSet &indata,DataSet &outdata){
  BOOST_FOREACH(auto &d,indata){
    auto &user=d.first;
    BOOST_FOREACH(auto &i,d.second){
      auto &item=i.first;
      auto &score=i.second;
      outdata[item].push_back(std::make_pair(user,score));
    }
  }
}

int get_norm(const std::vector<std::pair<int,float> > &vlst){
  float norm =0;
  BOOST_FOREACH(auto &i, vlst){
    norm += i.second*i.second;
  }
  norm = sqrt(norm);
  return norm;
}
void normalize_data(DataSet &data){
  BOOST_FOREACH(auto &d,data){
    auto &user=d.first;
    auto &info=d.second;
    float norm=get_norm(info);
    BOOST_FOREACH(auto &i,info){
      auto &item=i.first;
      auto &score=i.second;
      score=score/norm;
    }
  }
}
