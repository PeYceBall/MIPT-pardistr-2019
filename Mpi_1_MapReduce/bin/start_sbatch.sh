#!/bin/bash
# MUST BE RUN FROM PROJECT'S MAIN DIRECTORY

module add mpi/openmpi4-x86_64

cmake .
make

sbatch -n 12 bin/run.sh

# sbatch -n 8 --ntasks-per-node 2 run.sh
