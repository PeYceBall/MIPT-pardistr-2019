#!/bin/bash
for f in $( ls ); do
    echo $f
    split -b 4096 --additional-suffix="$f" "$f"
done