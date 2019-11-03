#!/bin/bash
# MUST BE RUN FROM PROJECT'S MAIN DIRECTORY

module add mpi/openmpi4-x86_64

INPUT_DIR='input files'
OUTPUT_DIR='reduce outputs'
NUM_REDUCERS=1

rm -rf "map inputs"
rm -rf intermediate
rm -rf "map outputs"
rm -rf "reduce inputs"
rm -rf "$OUTPUT_DIR"

mkdir "map inputs"
mkdir "intermediate"
mkdir "map outputs"
mkdir "reduce inputs"
mkdir "$OUTPUT_DIR"

for f in $( ls input\ files/); do
    split -b 4096 --additional-suffix="$f" "input files/$f" "map inputs/"
done

for i in $(seq 0 $(($NUM_REDUCERS - 1)))
do
    mkdir "intermediate/$i"
done

cmake .
make

mpiexec -np 4 bin/OpenMP_MapReduce "$INPUT_DIR" "$OUTPUT_DIR" "$NUM_REDUCERS"

rm -rf "map inputs"
rm -rf "intermediate"
rm -rf "map outputs"
rm -rf "reduce inputs"