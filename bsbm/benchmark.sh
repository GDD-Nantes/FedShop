#!/bin/bash

export RSFB__N_BATCH=3
export RSFB__ENDPOINT="http://localhost:8890/sparql"
export RSFB__DOCKER_CONTAINER_NAME="bsbm-virtuoso"
export RSFB__WORK_DIR="bsbm"
export RSFB__N_VARIATIONS=10

export RSFB__VERBOSE=false

# Config per batch
export RSFB__N_VENDOR=3
export RSFB__N_REVIEWER=3
export RSFB__SCALE_FACTOR=1

for batch in $( seq 1 $RSFB__N_BATCH)
do
    echo "Producing metrics for batch ${batch} out of ${RSFB__N_BATCH}..."
    snakemake --cores 1 --debug-dag --batch merge_metrics="${batch}/${RSFB__N_BATCH}" || exit 1
    snakemake -p --cores=1 --batch merge_metrics="${batch}/${RSFB__N_BATCH}" || exit 1
done