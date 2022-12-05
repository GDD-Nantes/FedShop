#!/bin/bash

# CUSTOMISABLE
export RSFB__SPARQL_ENDPOINT="http://localhost:8890/sparql"
export RSFB__GENERATOR_ENDPOINT="http://localhost:8000"

export RSFB__SPARQL_CONTAINER_NAME="bsbm-virtuoso"
export RSFB__GENERATOR_CONTAINER_NAME="watdiv"

export RSFB__WORK_DIR="bsbm"
export RSFB__N_VARIATIONS=10

export RSFB__VERBOSE=false

export RSFB__N_BATCH=3
# Config per batch
export RSFB__N_VENDOR=3
export RSFB__N_REVIEWER=1
export RSFB__SCALE_FACTOR=1

N_CORES="1" # any number or "all"

# FIXED
GENERATION_SNAKEFILE="workflow/generate-batch.Snakefile"
EVALUATION_SNAKEFILE="workflow/evaluate.Snakefile"

SNAKEMAKE_OPTS="-p --cores ${N_CORES}"

WORKFLOW_DIR="${RSFB__WORK_DIR}/rulegraph"
mkdir -p ${WORKFLOW_DIR}

MODE="$1" # One of ["generate", "evaluate"]
DEBUG="$2" # One of ["debug"]

# FUNCTIONS
help(){
    echo 'sh benchmark.sh MODE(["generate", "evaluate"]) DEBUG(["debug"])'
}

syntax_error(){
    help && exit 1
}

# Input handling
if [ $# -lt 1 ]; then
    syntax_error;
fi

# If in generate MODE
if [ "${MODE}"="generate" ]; then
    for batch in $( seq 1 $RSFB__N_BATCH)
    do
        if [ "$2" = "debug" ]; then
            echo "Producing rulegraph..."
            (snakemake ${SNAKEMAKE_OPTS} --snakefile ${GENERATION_SNAKEFILE} --rulegraph > "${WORKFLOW_DIR}/rulegraph_batch${batch}.dot") || exit 1
            (
                #gsed -Ei "s#(digraph snakemake_dag \{)#\1 rankdir=\"LR\"#g" "${WORKFLOW_DIR}/rulegraph_batch${batch}.dot" &&
                dot -Tpng "${WORKFLOW_DIR}/rulegraph_batch${batch}.dot" > "${WORKFLOW_DIR}/rulegraph_batch${batch}.png" 
            ) || exit 1
        else
            echo "Producing metrics for batch ${batch} out of ${RSFB__N_BATCH}..."
            snakemake ${SNAKEMAKE_OPTS} --snakefile ${GENERATION_SNAKEFILE} --debug-dag --batch merge_metrics="${batch}/${RSFB__N_BATCH}" || exit 1
            snakemake ${SNAKEMAKE_OPTS} --snakefile ${GENERATION_SNAKEFILE} --batch merge_metrics="${batch}/${RSFB__N_BATCH}" || exit 1
        fi
    done
# if in evaluate MODE
elif [ "${MODE}"="generate" ]; then

else
    syntax_error
fi