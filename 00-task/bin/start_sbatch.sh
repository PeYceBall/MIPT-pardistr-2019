#!/bin/bash
module add mpi/openmpi4-x86_64
sbatch -n 8 -i input.txt run.sh