#!/bin/bash

if [ $# -eq 1 -a "$1" = "deep" ]; then
    rm -rf bsbm/model/tmp
    rm -rf bsbm/model/exported
    rm -rf bsbm/model/workflow
    rm -rf bsbm/benchmark
fi

rm bsbm/virtuoso*.txt
rm bsbm/generator*.txt
rm -rf bsbm/benchmark/**/batch*
rm bsbm/benchmark/*.csv
exit 0