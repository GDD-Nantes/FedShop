import numpy as np
import pandas as pd
import os
from pathlib import Path
import glob
import time
import requests
import subprocess
import json
import re
from textops import cat, find_first_pattern

import sys
smk_directory = os.path.abspath(workflow.basedir)
sys.path.append(os.path.join(Path(smk_directory).parent.parent, "rsfb"))

from utils import ping, rsfb_logger, load_config, get_docker_endpoint_by_container_name, get_docker_containers, check_container_status, create_stats, virtuoso_kill_all_transactions, wait_for_container
from utils import activate_one_container as utils_activate_one_container

#===============================
# EVALUATION PHASE:
# - Compile engines
# - Generate results and source selection for each engine
# - Generate metrics and stats for each engine
#===============================

CONFIGFILE = config["configfile"]
BATCH_ID = config["batch"]
ENGINE_ID = config["engine"]
QUERY_PATH = config["query"]
INSTANCE_ID = config["instance"]

WORK_DIR = "experiments/bsbm"
CONFIG = load_config(CONFIGFILE)

CONFIG_GEN = CONFIG["generation"]
CONFIG_EVAL = CONFIG["evaluation"]

SPARQL_COMPOSE_FILE = CONFIG_GEN["virtuoso"]["compose_file"]
SPARQL_SERVICE_NAME = CONFIG_GEN["virtuoso"]["service_name"]
SPARQL_CONTAINER_NAMES = CONFIG_GEN["virtuoso"]["container_names"]

PROXY_COMPOSE_FILE =  CONFIG_EVAL["proxy"]["compose_file"]
PROXY_SERVICE_NAME = CONFIG_EVAL["proxy"]["service_name"]
PROXY_CONTAINER_NAMES = CONFIG_EVAL["proxy"]["container_name"]
PROXY_SERVER = CONFIG["evaluation"]["proxy"]["endpoint"]
PROXY_PORT = re.search(r":(\d+)", PROXY_SERVER).group(1)
PROXY_SPARQL_ENDPOINT = PROXY_SERVER + "sparql"

N_QUERY_INSTANCES = CONFIG_GEN["n_query_instances"]
N_BATCH = CONFIG_GEN["n_batch"]
LAST_BATCH = N_BATCH-1

# Config per batch
N_VENDOR=CONFIG_GEN["schema"]["vendor"]["params"]["vendor_n"]
N_RATINGSITE=CONFIG_GEN["schema"]["ratingsite"]["params"]["ratingsite_n"]

FEDERATION_COUNT=N_VENDOR+N_RATINGSITE

QUERY_DIR = f"{WORK_DIR}/queries"
MODEL_DIR = f"{WORK_DIR}/model"
BENCH_DIR = f"{WORK_DIR}/benchmark/evaluation"
TEMPLATE_DIR = f"{MODEL_DIR}/watdiv"

LOGGER = rsfb_logger(Path(__file__).name)

#=================
# USEFUL FUNCTIONS
#=================

def activate_one_container(batch_id):
    """ Activate one container while stopping all others
    """

    LOGGER.info("Activating Virtuoso docker container...")
    is_virtuoso_restarted = utils_activate_one_container(batch_id, SPARQL_COMPOSE_FILE, SPARQL_SERVICE_NAME, LOGGER, f"{BENCH_DIR}/virtuoso-ok.txt")

    LOGGER.info("Activating proxy docker container...")
    proxy_target = CONFIG_GEN["virtuoso"]["endpoints"][-1]
    proxy_target = proxy_target.replace("/sparql", "/")
    if ping(PROXY_SPARQL_ENDPOINT) == -1:
        LOGGER.info("Starting proxy server...")
        shell(f"docker-compose -f {PROXY_COMPOSE_FILE} up -d {PROXY_SERVICE_NAME}")
    
    while ping(PROXY_SPARQL_ENDPOINT) != 200:
        os.system(f'curl -X GET {PROXY_SERVER + "set-destination"}?proxyTo={proxy_target}')
        time.sleep(1)
        #wait_for_container(PROXY_SPARQL_ENDPOINT, "/dev/null", logger , wait=1)

    if is_virtuoso_restarted:
        shell(f"python rsfb/engines/ideal.py warmup {CONFIGFILE}")

def generate_federation_declaration(federation_declaration_file, engine, batch_id):
    #sparql_endpoint = get_docker_endpoint_by_container_name(SPARQL_COMPOSE_FILE, SPARQL_SERVICE_NAME, SPARQL_CONTAINER_NAMES[LAST_BATCH])
    sparql_endpoint = PROXY_SPARQL_ENDPOINT

    LOGGER.info(f"Rewriting {engine} configfile as it is updated!")
    ratingsite_data_files = [ f"{MODEL_DIR}/dataset/ratingsite{i}.nq" for i in range(N_RATINGSITE) ]
    vendor_data_files = [ f"{MODEL_DIR}/dataset/vendor{i}.nq" for i in range(N_VENDOR) ]

    batch_id = int(batch_id)
    ratingsiteSliceId = np.histogram(np.arange(N_RATINGSITE), N_BATCH)[1][1:].astype(int)[batch_id]
    vendorSliceId = np.histogram(np.arange(N_VENDOR), N_BATCH)[1][1:].astype(int)[batch_id]
    batch_files = ratingsite_data_files[:ratingsiteSliceId+1] + vendor_data_files[:vendorSliceId+1]  

    activate_one_container(LAST_BATCH)
    shell(f"python rsfb/engines/{engine}.py generate-config-file {' '.join(batch_files)} {federation_declaration_file} {CONFIGFILE} {batch_id} {sparql_endpoint}")

#=================
# PIPELINE
#=================

rule all:
    input: 
        provenance=expand(
            "{benchDir}/{engine}/{query}/instance_{instance_id}/batch_{batch_id}/test/provenance.csv", 
            benchDir=BENCH_DIR,
            batch_id=BATCH_ID,
            engine=ENGINE_ID,
            query=QUERY_PATH,
            instance_id=INSTANCE_ID
        ),
        results=expand(
            "{benchDir}/{engine}/{query}/instance_{instance_id}/batch_{batch_id}/test/results.csv", 
            benchDir=BENCH_DIR,
            batch_id=BATCH_ID,
            engine=ENGINE_ID,
            query=QUERY_PATH,
            instance_id=INSTANCE_ID
        ),

rule transform_provenance:
    input: "{benchDir}/{engine}/{query}/instance_{instance_id}/batch_{batch_id}/test/source_selection.txt"
    output: "{benchDir}/{engine}/{query}/instance_{instance_id}/batch_{batch_id}/test/provenance.csv"
    params:
        prefix_cache=expand("{workDir}/benchmark/generation/{{query}}/instance_{{instance_id}}/prefix_cache.json", workDir=WORK_DIR)
    run: 
        shell("python rsfb/engines/{wildcards.engine}.py transform-provenance {input} {output} {params.prefix_cache}")

rule transform_results:
    input: "{benchDir}/{engine}/{query}/instance_{instance_id}/batch_{batch_id}/test/results.txt"
    output: "{benchDir}/{engine}/{query}/instance_{instance_id}/batch_{batch_id}/test/results.csv"
    run:
        # Transform results
        shell("python rsfb/engines/{wildcards.engine}.py transform-results {input} {output}")
        if os.stat(str(output)).st_size > 0:
            expected_results = pd.read_csv(f"{WORK_DIR}/benchmark/generation/{wildcards.query}/instance_{wildcards.instance_id}/batch_{wildcards.batch_id}/results.csv").dropna(how="all", axis=1)
            expected_results = expected_results.reindex(sorted(expected_results.columns), axis=1)
            expected_results = expected_results \
                .sort_values(expected_results.columns.to_list()) \
                .reset_index(drop=True) 
            
            engine_results = pd.read_csv(str(output)).dropna(how="all", axis=1)
            engine_results = engine_results.reindex(sorted(engine_results.columns), axis=1)
            engine_results = engine_results \
                .sort_values(engine_results.columns.to_list()) \
                .reset_index(drop=True) 

            if not expected_results.equals(engine_results):
                LOGGER.debug(expected_results)
                LOGGER.debug("not equals to")
                LOGGER.debug(engine_results)

                create_stats(f"{Path(str(input)).parent}/stats.csv", "error_mismatch_expected_results")

                if len(engine_results) < len(expected_results):
                    raise RuntimeError(f"{wildcards.engine} does not produce the expected results")
            # else:
            #     create_stats(f"{Path(str(input)).parent}/stats.csv")

rule evaluate_engines:
    """Evaluate queries using each engine's source selection on FedX.
    
    - Output: only statistics, no source-seleciton
    """
    threads: 1
    retries: 1
    input: 
        query=ancient(expand("{workDir}/benchmark/generation/{{query}}/instance_{{instance_id}}/injected.sparql", workDir=WORK_DIR)),
        engine_source_selection=ancient(expand("{workDir}/benchmark/generation/{{query}}/instance_{{instance_id}}/batch_{{batch_id}}/provenance.csv", workDir=WORK_DIR)),
        virtuoso_last_batch=ancient(expand("{workDir}/benchmark/generation/virtuoso_batch{batch_n}-ok.txt", workDir=WORK_DIR, batch_n=N_BATCH-1)),
        engine_status=ancient("{benchDir}/{engine}/{engine}-ok.txt"),
    output: 
        stats="{benchDir}/{engine}/{query}/instance_{instance_id}/batch_{batch_id}/test/stats.csv",
        source_selection="{benchDir}/{engine}/{query}/instance_{instance_id}/batch_{batch_id}/test/source_selection.txt",
        result_txt="{benchDir}/{engine}/{query}/instance_{instance_id}/batch_{batch_id}/test/results.txt",
    params:
        query_plan="{benchDir}/{engine}/{query}/instance_{instance_id}/batch_{batch_id}/test/query_plan.txt",
        result_csv="{benchDir}/{engine}/{query}/instance_{instance_id}/batch_{batch_id}/test/results.csv",
        engine_config="{benchDir}/{engine}/config/batch_{batch_id}/{engine}.conf",
        last_batch=LAST_BATCH
    run: 
        activate_one_container(LAST_BATCH)
        engine = str(wildcards.engine)
        batch_id = int(wildcards.batch_id)
        engine_config = f"{WORK_DIR}/benchmark/evaluation/{engine}/config/batch_{batch_id}/{engine}.conf"
        generate_federation_declaration(engine_config, engine, batch_id)
        virtuoso_kill_all_transactions(SPARQL_COMPOSE_FILE, SPARQL_SERVICE_NAME, LAST_BATCH)
        shell("python rsfb/engines/{engine}.py run-benchmark {CONFIGFILE} {params.engine_config} {input.query} --out-result {output.result_txt}  --out-source-selection {output.source_selection} --stats {output.stats} --force-source-selection {input.engine_source_selection} --query-plan {params.query_plan} --batch-id {batch_id}")


rule engines_prerequisites:
    output: "{benchDir}/{engine}/{engine}-ok.txt"
    shell: "python rsfb/engines/{wildcards.engine}.py prerequisites {CONFIGFILE} && echo 'OK' > {output}"

