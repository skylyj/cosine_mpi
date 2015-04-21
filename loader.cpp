#include <mpi.h>
#include <boost/mpi.hpp>
#include <sstream>
#include <fstream>
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
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/date_time.hpp>
#include <boost/format.hpp>

void solo_log(const int & irank,const int &prank,const std::string & info);
void solo_log(const int & irank,const int &prank,boost::format & info);
void parse_chunk(const boost::mpi::communicator &world, const std::string & str, std::vector<DataSet> &load_data){
    std::stringstream ss(str);
    int rank = world.rank();
    int size = world.size();
    std::string line;
    while (std::getline(ss, line, '\n')) {
        std::vector<std::string> elems;
        boost::split(elems, line, boost::is_any_of("\t"), boost::token_compress_on);
        auto fid=atoi(elems[0].c_str());
        auto sid=atoi(elems[1].c_str());
        auto score=atof(elems[2].c_str());
        int hash_id = fid%size;
        DataSet & data = load_data[hash_id];
        data[fid].push_back(std::make_pair(sid,score));
        // if (rank == 1) {
        //     std::cout<<"hash_id is "<<hash_id<<"\t after rank2 read line\t"<<line<<std::endl;
        //     show_data(rank, load_data);
        // }
    }
}
std::string get_chunk(MPI_File *in, const boost::mpi::communicator & world, const int & overlap) {
    int rank = world.rank();
    int size = world.size();
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
  //真实的读取闭区间，[globalstart, globalend] -> [globalstart, globalend+overlap]， 个数是mysize个

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
  }//从overlap 中寻找第一个\n位置的下一个字节
  if (rank != size-1) {
    locend-=overlap;
    locend += 1;
    while(chunk[locend] != '\n') locend++;
  }
  mysize = locend-locstart+1;

  std::string str(chunk+locstart,chunk+locend+1);
  return str;
}

void load_single_file(const boost::mpi::communicator & world, const std::string &inpath, std::vector<DataSet> & load_data){
    int size = world.size();
    std::ifstream fp(inpath.c_str());
    std::string line;
    while (getline(fp,line)){
        std::vector<std::string> elems;
        boost::split(elems, line, boost::is_any_of("\t"), boost::token_compress_on);
        auto fid=atoi(elems[0].c_str());
        auto sid=atoi(elems[1].c_str());
        auto score=atof(elems[2].c_str());
        int hash_id = fid%size;
        DataSet & data = load_data[hash_id];
        data[fid].push_back(std::make_pair(sid,score));
    }
    fp.close();
}


void get_fnms(const std::string & inpath, std::vector<std::string > & fnms){
    namespace fs = boost::filesystem;
    fs::path someDir(inpath);
    fs::directory_iterator end_iter;
    if ( fs::exists(someDir) && fs::is_directory(someDir))
    {
        for( fs::directory_iterator dir_iter(someDir) ; dir_iter != end_iter ; ++dir_iter)
        {
            if (fs::is_regular_file(dir_iter->status()) )
            {
                std::string x = someDir.relative_path().filename().string() + "/" + dir_iter->path().filename().string();
                fnms.push_back(x);
            }
        }
    }
    else if ( fs::exists(someDir) && fs::is_regular_file(someDir)){
        fnms.push_back(someDir.string());
    }
    else{
        std::cout<<"bad file\n";
        exit(2);
    }
}

void load_data(const boost::mpi::communicator & world, std::string & inpath, std::vector<DataSet> & data){
    const int overlap = 100;
    std::vector<std::string> fnms;
    get_fnms(inpath, fnms);
    solo_log(world.rank(), 0,boost::format("load_data start at  %1% ...") %boost::posix_time::second_clock::local_time());
    if (fnms.size() < world.size()) {
        BOOST_FOREACH(auto &fnm, fnms){
            solo_log(world.rank(), 0,boost::format("mpi spliting  fnm  %1% of %2% files...") %fnm %fnms.size());
            MPI_File in;
            int ierr = MPI_File_open(MPI_Comm(world), fnm.c_str(), MPI_MODE_RDONLY, MPI_INFO_NULL, &in);
            std::string chunk = get_chunk(&in, world, overlap);
            parse_chunk(world, chunk, data);
            MPI_File_close(&in);
        }
    }
    else{
        int isize = fnms.size()/world.size();
        int istart = isize * world.rank();
        int iend = istart + isize;
        if (world.rank() == world.size() -1 ){
            iend = fnms.size();
        }
        for (int i = istart; i<iend; i++){
            solo_log(world.rank(), 0,boost::format("loading  single file  %1% ...") %fnms[i]);
            load_single_file(world, fnms[i], data);
        }
    }
}
