cmake_minimum_required(VERSION 2.8)

set(CMAKE_CXX_COMPILER mpic++)
set(CMAKE_C_COMPILER mpicc)
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -fopenmp")

project(OpenMP_MapReduce)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${OpenMP_MapReduce_SOURCE_DIR}/bin)

add_executable(OpenMP_MapReduce main.cpp)