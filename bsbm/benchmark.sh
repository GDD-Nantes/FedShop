#!/bin/bash

# Snakemake config
export RSFB__N_BATCH=10
export RSFB__SPARQL_ENDPOINT="http://localhost:8890/sparql"
export RSFB__GENERATOR_ENDPOINT="http://localhost:8000"

export RSFB__SPARQL_CONTAINER_NAME="bsbm-virtuoso"
export RSFB__GENERATOR_CONTAINER_NAME="watdiv"

export RSFB__WORK_DIR="bsbm"
export RSFB__N_VARIATIONS=10

export RSFB__VERBOSE=false

# Config per batch
export RSFB__N_VENDOR=5
export RSFB__N_REVIEWER=1
export RSFB__SCALE_FACTOR=1

N_CORES="1" # any number or "all"

for batch in $( seq 1 $RSFB__N_BATCH)
do
    echo "Producing metrics for batch ${batch} out of ${RSFB__N_BATCH}..."
    snakemake --cores ${N_CORES} --debug-dag --batch merge_metrics="${batch}/${RSFB__N_BATCH}" || exit 1
    snakemake -p --cores ${N_CORES} --batch merge_metrics="${batch}/${RSFB__N_BATCH}" || exit 1
done