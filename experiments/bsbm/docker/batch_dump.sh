#!/bin/bash

for batch_id in $(seq 0 9) 
do
    upperBound=$((10*$batch_id+9))
    nt_files=()
    for item in $(seq 0 $upperBound)
    do
        nt_files+=("experiments/bsbm/model/dataset/ratingsite$upperBound.nt")
        nt_files+=("experiments/bsbm/model/dataset/vendor$upperBound.nt")
    done
    echo ${nt_files[@]} | xargs cat > "experiments/bsbm/model/dataset/batch_dump/batch_$batch_id.nt"
    #ls experiments/bsbm/model/dataset/(ratingsite|vendor){0..$upperBound}.nt #
done
