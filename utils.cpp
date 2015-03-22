#include <boost/foreach.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/format.hpp>
#include "types.h"
#include <iostream>
#include <unordered_map>
#include <cmath>
void construct_data(const std::vector<DataSet> &indata, DataSet &outdata){
  BOOST_FOREACH(auto &mod, indata){
    BOOST_FOREACH(auto &i, mod){
      auto &user=i.first;
      BOOST_FOREACH(auto &ipair, i.second){  
        outdata[user].push_back(ipair);
      }     
    }
  }
}
void solo_log(const int & irank,const int &prank,const std::string & info){
  if (irank == prank ){
    std::cout<<info<<std::endl;
  }
}

void solo_log(const int & irank,const int &prank,boost::format & info){
  if (irank == prank ){
    std::cout<<info<<std::endl;
  }
}

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

double get_norm(const std::vector<std::pair<int,double> > &vlst){
  double norm =0;
  BOOST_FOREACH(auto &i, vlst){
    norm += i.second*i.second;
  }
  return sqrt(norm);
}

void normalize_data(DataSet &data){
  BOOST_FOREACH(auto &d,data){
    auto &user=d.first;
    auto &info=d.second;
    double norm=get_norm(info);
    BOOST_FOREACH(auto &i,info){
      i.second =i.second/norm;
    }
  }
}
