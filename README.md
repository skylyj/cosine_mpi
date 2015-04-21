# 基于mpi的稀疏矩阵乘法


## 安装

### 安装 mpich-3.1.4+

    tar zxf mpich-3.1.4.tar.gz
    cd mpich-3.1.4;
    ./configure --disable-fortran
    make && make install

### 安装 boost_1_57_0+

项目依赖于boost_mpi， boost_serialization 等，建议安装全部的boost库

    tar zxf boost_1_57_0.tar.gz;
    cd boost_1_57_0;
    ./bootstrap.sh
    cp ./tools/build/example/user-config.jam ~/;
    echo "using mpi ;" >> ~/user-config.jam
    ./b2 install --user-config=/root/user-config.jam

### 部署mpi环境

## 测试

make
cd test
