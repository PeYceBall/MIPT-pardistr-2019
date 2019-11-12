cmake_minimum_required(VERSION 2.8)

set(CMAKE_CXX_COMPILER mpic++)
set(CMAKE_C_COMPILER mpicc)

project(Mpi_0_Sum)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${Mpi_0_Sum_SOURCE_DIR}/bin)

add_executable(Mpi_0_Sum main.cpp)