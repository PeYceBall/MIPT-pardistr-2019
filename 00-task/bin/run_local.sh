#!/bin/bash
module add mpi/openmpi4-x86_64
mpiexec -np 4 ./Mpi_0_Sum