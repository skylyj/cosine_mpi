FILES = $(wildcard ./*.cpp)

all:main
main:$(FILES)
	mpic++ -o main --std=c++11 -lboost_mpi -lboost_serialization $(FILES)
clean:
	rm -rf *.o *.out
.PHONY : default clean
