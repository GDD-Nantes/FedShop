#!/bin/bash

if [ $# -eq 1 -a "$1" = "deep" ]; then
    rm -rf experiments/bsbm/model/tmp
    rm -rf experiments/bsbm/model/exported  
    rm -rf experiments/bsbm/model/virtuoso
fi

if [ $# -eq 1 -a "$1" = "model" ]; then
    rm -rf experiments/bsbm/model/exported  
    rm -rf experiments/bsbm/model/virtuoso
fi

rm -rf experiments/bsbm/benchmark
rm -rf experiments/bsbm/rulegraph

exit 0