#!/bin/bash
ls input\ files/

for f in $( ls input\ files/); do
    echo $f
    split -b 4096 --additional-suffix="$f" "input files/$f" "map inputs/"
done