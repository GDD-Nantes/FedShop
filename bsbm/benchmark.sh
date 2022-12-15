#!/bin/bash

# CUSTOMISABLE
RSFB__SPARQL_COMPOSE_FILE="docker-compose-sparql.yml"
RSFB__GENERATOR_COMPOSE_FILE="docker-compose-generator.yml"

RSFB__WORK_DIR="bsbm"
RSFB__N_BATCH=3

N_CORES=1 # any number or "all"
N_ENGINES=1

# FIXED
GENERATION_SNAKEFILE="workflow/generate-batch.smk"
EVALUATION_SNAKEFILE="workflow/evaluate.smk"

SNAKEMAKE_OPTS="-p --cores ${N_CORES} --rerun-incomplete"

WORKFLOW_DIR="${RSFB__WORK_DIR}/rulegraph"
mkdir -p ${WORKFLOW_DIR}

MODE="$1" # One of ["generate", "evaluate"]
OP="$2" # One of ["debug", "clean"]

CLEAN_SCRIPT="${RSFB__WORK_DIR}/clean.sh"

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
if [ "${MODE}" = "generate" ]; then

    if [ "${OP}" = "clean" ]; then
        echo "Cleaning..."
        docker-compose -f ${RSFB__GENERATOR_COMPOSE_FILE} down -v --remove-orphans || exit 1
        docker-compose -f ${RSFB__SPARQL_COMPOSE_FILE} down -v --remove-orphans || exit 1
        sh ${CLEAN_SCRIPT} deep || exit 1
    fi

    for batch in $( seq 1 $RSFB__N_BATCH)
    do
        if [ "${OP}" = "debug" ]; then
            echo "Producing rulegraph..."
            RULEGRAPH_FILE="${WORKFLOW_DIR}/rulegraph_${MODE}_batch${batch}"
            snakemake ${SNAKEMAKE_OPTS} --snakefile ${GENERATION_SNAKEFILE} --debug-dag --batch merge_metrics="${batch}/${RSFB__N_BATCH}" || exit 1
            snakemake ${SNAKEMAKE_OPTS} --snakefile ${GENERATION_SNAKEFILE} --rulegraph > "${RULEGRAPH_FILE}.dot" || exit 1
            (
                #gsed -Ei "s#(digraph snakemake_dag \{)#\1 rankdir=\"LR\"#g" "${RULEGRAPH_FILE}.dot" &&
                dot -Tpng "${RULEGRAPH_FILE}.dot" > "${RULEGRAPH_FILE}.png" 
            ) || exit 1
        else
            echo "Producing metrics for batch ${batch}/${RSFB__N_BATCH}..."
            snakemake ${SNAKEMAKE_OPTS} --snakefile ${GENERATION_SNAKEFILE} --batch merge_metrics="${batch}/${RSFB__N_BATCH}" || exit 1
        fi
    done
# if in evaluate MODE
elif [ "${MODE}" = "evaluate" ]; then
    
    if [ "${OP}" = "clean" ]; then
        echo "Cleaning..."
        #docker-compose -f ${RSFB__SPARQL_COMPOSE_FILE} down &&
        rm -rf "${RSFB__WORK_DIR}/benchmark/evaluation"
    fi

    for batch in $( seq 1 ${N_ENGINES})
    do
        if [ "${OP}" = "debug" ]; then
            echo "Producing rulegraph..."
            snakemake ${SNAKEMAKE_OPTS} --snakefile ${EVALUATION_SNAKEFILE} --debug-dag --batch merge_metrics="${batch}/${N_ENGINES}" || exit 1
            snakemake ${SNAKEMAKE_OPTS} --snakefile ${EVALUATION_SNAKEFILE} --rulegraph > "${RULEGRAPH_FILE}.dot" || exit 1
            (
                #gsed -Ei "s#(digraph snakemake_dag \{)#\1 rankdir=\"LR\"#g" "${RULEGRAPH_FILE}.dot" &&
                dot -Tpng "${RULEGRAPH_FILE}.dot" > "${RULEGRAPH_FILE}.png" 
            ) || exit 1
        else
            echo "Measuring execution time for batch ${batch}/${N_ENGINES}..."
            snakemake ${SNAKEMAKE_OPTS} --snakefile ${EVALUATION_SNAKEFILE} --batch merge_metrics="${batch}/${N_ENGINES}" || exit 1
        fi
    done
else
    syntax_error
fi