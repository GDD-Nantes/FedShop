#!/bin/bash

if [ $# -eq 1 -a "$1" = "deep" ]; then
    rm -rf experiments/bsbm/model/tmp
fi

rm -rf experiments/bsbm/model/exported  
rm -rf experiments/bsbm/model/virtuoso
rm -rf experiments/bsbm/benchmark
rm experiments/bsbm/virtuoso*.txt
rm experiments/bsbm/generator*.txt
rm -rf experiments/bsbm/benchmark/**/batch*
rm -rf experiments/bsbm/rulegraph
rm experiments/bsbm/benchmark/*.csv
exit 0