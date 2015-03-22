#include <sstream>
#include <iostream>
#include <iterator>
#include <mpi.h>
#include <boost/foreach.hpp>
#include <fstream>
#include <string>
#include <unordered_map>
#include "types.h"
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
// #include <boost/algorithm/string/classification.hpp>
// #include <boost/algorithm/string/split.hpp>
void show_data(const int &rank, const std::vector<DataSet> &load_data){
    int mod = 0;
    BOOST_FOREACH(auto &loadd,load_data){
        BOOST_FOREACH(auto &d, loadd){
            auto &user=d.first;
            auto &info=d.second;
            std::vector<std::string> outinfo;
            transform(info.begin(),info.end(),back_inserter(outinfo),
                      [](std::pair<int,double>i){return std::to_string(i.first)+":"+std::to_string((int)i.second);});
            std::cout<<rank<<"||"<<mod<<"||"<<user<<"\t"<<boost::join(outinfo,"|")<<std::endl;
        }
        mod ++;
    }
}

std::string info2str(const std::vector<std::pair<int,double> >& info){
    std::vector<std::string> outinfo;
    transform(info.begin(),info.end(),back_inserter(outinfo),
              [](std::pair<int,double>i){return std::to_string(i.first)+":"+std::to_string(i.second);});
    return boost::join(outinfo,"|");
}

void parallel_dump(const std::string &outpath,const int &rank, const DataSet &data){
  std::ofstream ofp(outpath+"/"+std::to_string(rank));
  BOOST_FOREACH(auto &d,data){
    auto user=d.first;
    auto info=d.second;
    std::vector<std::string> outinfo;
    if (info.empty()) continue;
    transform(info.begin(),info.end(),back_inserter(outinfo),
              [](std::pair<int,double>i){return std::to_string(i.first)+":"+std::to_string(i.second);});
    ofp<<user<<"\t"<<boost::join(outinfo,"|")<<std::endl;
  }
  ofp.close();
}


void parallel_dump(const std::string &outpath,const int &rank, const std::vector<DataSet> &load_data){
  boost::filesystem::path path(outpath);
  boost::filesystem::path file(std::to_string(rank));
  boost::filesystem::path full_path(path/file);
  std::ofstream ofp(full_path.string());
  int mod = 0;
  BOOST_FOREACH(auto &loadd,load_data){
    BOOST_FOREACH(auto &d, loadd){
      auto &user=d.first;
      auto &info=d.second;
      std::vector<std::string> outinfo;
      transform(info.begin(),info.end(),back_inserter(outinfo),
                [](std::pair<int,double>i){return std::to_string(i.first)+":"+std::to_string((int)i.second);});
      ofp<<rank<<"||"<<mod<<"||"<<user<<"\t"<<boost::join(outinfo,"|")<<std::endl;
    }
    mod ++;
  }
  ofp.close();
}
