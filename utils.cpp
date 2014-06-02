#include <boost/foreach>
#include "types.h"
void transpose_data(const DataSet &indata,DataSet &outdata){
  BOOST_FOREACH(auto &d,indata){
    auto &user=d.first;
    BOOST_FOREACH(auto &i,d.second){
      auto &item=i.first;
      auto &score=i.second;
      outdata[item][user]=score;
    }
  }
}
