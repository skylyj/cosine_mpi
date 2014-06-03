#include <sstream>
#include <iterator>
#include <mpi.h>
#include <boost/foreach.hpp>
#include <fstream>
#include <string>
#include "types.h"
#include <boost/algorithm/string.hpp>
std::vector<std::string> split(const std::string &s, char delim) {
  std::stringstream ss(s);
  std::string item;
  std::vector<std::string> elems;
  while (std::getline(ss, item, delim)) {
    elems.push_back(item);
  }
  return elems;
}

void parallel_read(MPI_File *in, const int rank,const int size,const int overlap, std::map<int,DataSet> &load_data) {
  // 并行读取文件，格式为uid\ttid\tscore
  /* read in relevant chunk of file into "chunk",
   * which starts at location in the file globalstart
   * and has size mysize 
   */

  /* figure out who reads what */
  MPI_Offset filesize;
  MPI_File_get_size(*in, &filesize);
  filesize--;  /* get rid of text file eof */
  int mysize = filesize/size;
  MPI_Offset globalstart = rank * mysize;
  MPI_Offset globalend   = globalstart + mysize - 1;
  if (rank == size-1) globalend = filesize-1;

  /* add overlap to the end of everyone's chunk except last proc... */
  if (rank != size-1)
    globalend += overlap;

  mysize =  globalend - globalstart + 1;

  /* allocate memory */
  char *chunk = (char *) malloc( (mysize + 1)*sizeof(char));

  /* everyone reads in their part */
  MPI_File_read_at_all(*in, globalstart, chunk, mysize, MPI_CHAR, MPI_STATUS_IGNORE);
  chunk[mysize] = '\0';

  /*
   * everyone calculate what their start and end *really* are by going 
   * from the first newline after start to the first newline after the
   * overlap region starts (eg, after end - overlap + 1)
   */

  int locstart=0, locend=mysize-1;
  if (rank != 0) {
    while(chunk[locstart] != '\n') locstart++;
    locstart++;
  }
  if (rank != size-1) {
    locend-=overlap;
    while(chunk[locend] != '\n') locend++;
  }
  mysize = locend-locstart+1;

  /* "Process" our chunk by replacing non-space characters with '1' for
   * rank 1, '2' for rank 2, etc... 
   */
  std::string str(chunk+locstart,chunk+locend+1);
  std::stringstream ss(str);
  std::string line;
  while (std::getline(ss, line, '\n')) {
    std::vector<std::string> elems=split(line,'\t');
    auto fid=atoi(elems[0].c_str());
    auto sid=atoi(elems[1].c_str());
    auto score=atof(elems[2].c_str());
    int hash_id = fid%size;
    load_data[hash_id][fid].push_back(std::make_pair(sid,score));
  }
  return;
}

void parallel_dump(const std::string &outpath,const int &rank, const DataSet &data){
  std::ofstream ofp(outpath+"/"+std::to_string(rank));
  BOOST_FOREACH(auto &d,data){
    auto user=d.first;
    auto info=d.second;
    std::vector<std::string> outinfo;
    transform(info.begin(),info.end(),back_inserter(outinfo),
              [](std::pair<int,float>i){return std::to_string(i.first)+":"+std::to_string(i.second);});
    ofp<<user<<"\t"<<boost::join(outinfo,"|")<<std::endl;
  }
  ofp.close();
}


void parallel_dump(const std::string &outpath,const int &rank, const std::map<int,DataSet> &load_data){
  // boost::filesystem::path path(outpath);
  // boost::filesystem::path file(std::to_string(rank));
  // boost::filesystem::path full_path(path/file);
  // std::ofstream ofp(full_path.string());
  std::ofstream ofp(outpath+"/"+std::to_string(rank));
  BOOST_FOREACH(auto &h,load_data){
    auto & mod=h.first;
    BOOST_FOREACH(auto &d,h.second){
      auto &user=d.first;
      auto &info=d.second;
      std::vector<std::string> outinfo;
      transform(info.begin(),info.end(),back_inserter(outinfo),
                [](std::pair<int,float>i){return std::to_string(i.first)+":"+std::to_string(i.second);});
      ofp<<mod<<"||"<<user<<"\t"<<boost::join(outinfo,"|")<<std::endl;
    }
  }
  ofp.close();
}
