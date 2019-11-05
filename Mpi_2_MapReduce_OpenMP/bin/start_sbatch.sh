#!/bin/bash
# MUST BE RUN FROM PROJECT'S MAIN DIRECTORY

module add mpi/openmpi4-x86_64

cmake .
make

export OMP_NUM_THREADS=8

sbatch -n 10 --cpus-per-task 8 bin/run.sh

