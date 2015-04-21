#include <mpi.h>
#include <boost/mpi.hpp>
#include <boost/foreach.hpp>
#include <iostream>
#include <sstream>
#include <fstream>
#include <algorithm>
#include <unordered_map>
#include <boost/algorithm/string.hpp>
#include <boost/program_options.hpp>
#include <boost/date_time.hpp>
#include <boost/format.hpp>
#include <boost/mpi/collectives.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/unordered_map.hpp>
#include <boost/serialization/utility.hpp>
#include "types.h"
void parallel_read(MPI_File *in, const int rank,const int size,const int overlap, std::vector <DataSet> &load_data);
void cal_sim(const boost::mpi::communicator &world, const std::vector<int> & users,
             const DataSet& data_a,const DataSet & data_b_inv,
             DataSet &sims,const int &topk, const double &sim_bar, const int &intersect_bar);
void chain_pass_ball(boost::mpi::communicator world,DataSet &data);
void transpose_data(const DataSet &indata,DataSet &outdata);
void parallel_dump(const std::string &outpath,const int &rank, const std::vector<DataSet> &data);
void parallel_dump(const std::string &outpath,const int &rank, const DataSet &data);
void normalize_data(DataSet &data);
void solo_log(const int & irank,const int &prank,const std::string & info);
void solo_log(const int & irank,const int &prank,boost::format & info);
void construct_data(const std::vector<DataSet> &indata, DataSet &outdata);
std::string info2str(const std::vector<std::pair<int,double> >& info);
void sort_sims(DataSet &sims);
void load_data(const boost::mpi::communicator & world, std::string & inpath, std::vector<DataSet> & data);

int main(int argc, char **argv) {
    boost::program_options::options_description desc("Allowed options");
    double sim_bar;
    int intersect_bar;
    int topk;
    std::string outpath0,outpath,outpath_load,datapath_a,datapath_b;
    desc.add_options()
        ("help", "Use --help or -h to list all arguments")
        ("sim_bar", boost::program_options::value<double>(&sim_bar)->default_value(0.01), "Use --sim_bar 0.01")
        ("intersect_bar", boost::program_options::value<int>(&intersect_bar)->default_value(1), "Use --intersect_bar 1")
        ("topk", boost::program_options::value<int>(&topk)->default_value(10), "Use --topk 10")
        ("datapath_a", boost::program_options::value<std::string>(&datapath_a), "datapath_a")
        ("datapath_b", boost::program_options::value<std::string>(&datapath_b), "datapath_b")
        ("outpath_load", boost::program_options::value<std::string>(&outpath_load), "outpath_load -- for debug")
        ("outpath0", boost::program_options::value<std::string>(&outpath0), "outpath0 -- for debug")
        ("outpath", boost::program_options::value<std::string>(&outpath), "outpath")
        ;

    boost::program_options::variables_map vm;        
    boost::program_options::store(parse_command_line(argc, argv, desc), vm);
    boost::program_options::notify(vm);    

    if (vm.count("help")) {
        std::cout << desc;
        return 0;
    }
    int rank,size;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    boost::mpi::communicator world(MPI_COMM_WORLD, boost::mpi::comm_attach);

    if (rank==0) {
        std::string paramstr="sim_bar topk datapath_a outpath0 outpath";
        std::vector<std::string> params;
        split(params, paramstr, boost::is_any_of(" "), boost::token_compress_on);
        BOOST_FOREACH(auto &para,params){
            if (vm.count(para)==0){
                std::cout<<para<<" required!!\n";
                exit(2);
            }
        }
        std::cout << "sim_bar = " << sim_bar<<", topk= " << topk
                  <<", datapath_a = "<< datapath_a <<", outpath0 = "<< outpath0 <<", out= " <<outpath<<std::endl;
    }
    boost::posix_time::ptime start = boost::posix_time::second_clock::local_time();
    solo_log(rank,0,boost::format("start time %1% ...\n") %start);


    // open file load data
    // std::unordered_map<int, DataSet> load_data_a(size),load_data_b(size);
    std::vector<DataSet> load_data_a(size),load_data_b(size);
    std::vector<DataSet> load_data_a_all(size),load_data_b_all(size);
    solo_log(rank,0,boost::format("parallel_read start at  %1% ...") %boost::posix_time::second_clock::local_time());
    load_data(world, datapath_a, load_data_a);
    load_data(world, datapath_b, load_data_b);
    // data all to all
    solo_log(rank,0,boost::format("all_to_all a start at  %1% ...") %boost::posix_time::second_clock::local_time());
    DataSet data_a,data_b;
    parallel_dump(outpath_load,rank, load_data_a);
    all_to_all(world,load_data_a,load_data_a_all);
    all_to_all(world,load_data_b,load_data_b_all);
    load_data_a.clear();
    load_data_b.clear();

    // construct data; make vector to hash_map
    solo_log(rank,0,boost::format("construct start at  %1% ...") %boost::posix_time::second_clock::local_time());
    construct_data(load_data_a_all, data_a);
    construct_data(load_data_b_all, data_b);
    parallel_dump(outpath0,rank, data_a);
    // normalize_data(data_a);
    // normalize_data(data_b);

    // // 倒排
    solo_log(rank,0,boost::format("transpose start at  %1% ...") %boost::posix_time::second_clock::local_time());
    DataSet data_b_inv;
    transpose_data(data_b,data_b_inv);

    // prepare for openmp
    DataSet sims;
    std::vector<int> users(data_a.size());
    {
        int i = 0;
        BOOST_FOREACH(auto &uinfo, data_a) {
            auto u = uinfo.first;
            users[i++] = u;
            sims[u]; // force insertion in a single thread ...
        }
    }

    solo_log(rank,0,boost::format("cal_sim start at  %1% ...") %boost::posix_time::second_clock::local_time());
    cal_sim(world,users, data_a,data_b_inv, sims,topk,sim_bar,intersect_bar);
    for (int i=1;i<world.size();i++){
        chain_pass_ball(world,data_b_inv);
        solo_log(rank,0,boost::format("after passing ball;round %1% ...%2%") \
                 %i %boost::posix_time::second_clock::local_time());
        cal_sim(world,users, data_a,data_b_inv, sims,topk,sim_bar,intersect_bar);   
    }
    sort_sims(sims);  
    parallel_dump(outpath,rank,sims);

    boost::posix_time::ptime end = boost::posix_time::second_clock::local_time();
    solo_log(rank,0,boost::format("end time %1%elapsed time %2%") %start %(end-start));

    MPI_Finalize();
    return 0;
}
