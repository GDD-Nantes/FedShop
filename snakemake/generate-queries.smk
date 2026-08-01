import pandas as pd
import os
from pathlib import Path
import glob
import time
import requests
import subprocess
import re
from itertools import product

import sys
smk_directory = os.path.abspath(workflow.basedir)
print(smk_directory)
sys.path.append(os.path.join(Path(smk_directory).parent, "fedshop"))

from utils import ping, fedshop_logger, load_config, docker_check_container_running
LOGGER = fedshop_logger(Path(__file__).name)

#===============================
# GENERATION PHASE:
# - Generate data
# - Ingest the data in virtuoso
# - Generate query instances
# - Generate expected results
# - Generate expected source selection
# - Generate expected metrics
#===============================

CONFIGFILE = config["configfile"]

WORK_DIR = "experiments/bsbm"
CONFIG = load_config(CONFIGFILE)
CONFIG_GEN = CONFIG["generation"]
CONFIG_EVAL = CONFIG["evaluation"]

USE_DOCKER = CONFIG["use_docker"]

SPARQL_COMPOSE_FILE = CONFIG_GEN["virtuoso"]["compose_file"]
VIRTUOSO_COMPOSE_CONFIG = load_config(SPARQL_COMPOSE_FILE)

SPARQL_SERVICE_NAME = CONFIG_GEN["virtuoso"]["service_name"]
SPARQL_DEFAULT_ENDPOINT = CONFIG_GEN["virtuoso"]["default_endpoint"]

PROXY_COMPOSE_FILE =  CONFIG_EVAL["proxy"]["compose_file"]
PROXY_SERVICE_NAME = CONFIG_EVAL["proxy"]["service_name"]
PROXY_CONTAINER_NAMES = CONFIG_EVAL["proxy"]["container_name"]
PROXY_SERVER = CONFIG_EVAL["proxy"]["endpoint"]
PROXY_PORT = CONFIG_EVAL["proxy"]["port"]
PROXY_SPARQL_ENDPOINT = PROXY_SERVER + "sparql"

# Change this to your local path to the isql executable
CONTAINER_PATH_TO_ISQL = "/opt/virtuoso-opensource/bin/isql"
CONTAINER_PATH_TO_DATA = "/usr/share/proj/" 

N_QUERY_INSTANCES = CONFIG_GEN["n_query_instances"]
VERBOSE = CONFIG_GEN["verbose"]
N_BATCH = CONFIG_GEN["n_batch"]

QUERY_DIR = f"{WORK_DIR}/queries"
MODEL_DIR = f"{WORK_DIR}/model"
BENCH_DIR = f"{WORK_DIR}/benchmark/generation"
TEMPLATE_DIR = f"{MODEL_DIR}/watdiv"

# Override settings if specified in the config file
BATCHES = str(config["batch"]).split(",") if config.get("batch") is not None else range(N_BATCH)
QUERY_PATH = (
    [Path(os.path.join(QUERY_DIR, f)).resolve().stem for f in str(config["query"]).split(",")] 
    if config.get("query") is not None else 
    [Path(os.path.join(QUERY_DIR, f)).resolve().stem for f in os.listdir(QUERY_DIR) if f.endswith(".sparql")]
)
INSTANCE_ID = str(config["instance"]).split(",") if config.get("instance") is not None else range(N_QUERY_INSTANCES)

DEBUG = eval(str(config["debug"])) if config.get("explain") is not None else False


#=================
# USEFUL FUNCTIONS
#=================

def ping(endpoint):
    try:
        req = requests.get(endpoint)
        LOGGER.debug(f"{req}")
        return req.status_code == 200
    except requests.exceptions.ConnectionError:
        return False

def get_results_per_batch(wildcards):
    def combinator(benchDir, query, instance_id, batch_id):
        for batch_id_u, benchDir_u, query_u, instance_id_u in product(batch_id, benchDir, query, instance_id):
            yield benchDir_u, query_u, instance_id_u, batch_id_u

    return expand(
        "{benchDir}/{query}/instance_{instance_id}/results-batch{batch_id}.csv",
        combinator,
        benchDir=BENCH_DIR,
        query=QUERY_PATH,
        instance_id=INSTANCE_ID,
        batch_id=BATCHES
    )

#=================
# PIPELINE
#=================

rule all:
    input: 
        expand(
            "{benchDir}/generate-batch{batch_id}.txt", 
            benchDir=BENCH_DIR, batch_id=BATCHES
        )

rule generate_batch:
    input: 
        expand(
            "{{benchDir}}/{query}/instance_{instance_id}/results-batch{{batch_id}}.csv",
            query=QUERY_PATH, 
            instance_id=INSTANCE_ID
        )
    output: "{benchDir}/generate-batch{batch_id}.txt"
    shell: "echo 'ok' > {output}"
        
rule execute_instances:
    input: "{benchDir}/{query}/instance_{instance_id}/injected.sparql"
    output: "{benchDir}/{query}/instance_{instance_id}/results-batch{batch_id}.csv"
    params:
        endpoint_batch0 = SPARQL_DEFAULT_ENDPOINT
    run:
        SPARQL_CONTAINER_NAME = f"docker-{SPARQL_SERVICE_NAME}-{int(wildcards.batch_id)+1}"
        if USE_DOCKER and not docker_check_container_running(SPARQL_CONTAINER_NAME):
            shell(f'docker compose -f {SPARQL_COMPOSE_FILE} stop')
            shell(f"docker start {SPARQL_CONTAINER_NAME}")
            while not ping(SPARQL_DEFAULT_ENDPOINT):
                LOGGER.debug(f"Waiting for {SPARQL_DEFAULT_ENDPOINT} to start...")
                time.sleep(1)

        composition_file = f"{Path(str(input)).parent}/composition.json"
        if not os.path.exists(composition_file):
            shell("python -m fedshop.query decompose-query {input} {composition_file}")
        shell("python -m fedshop.query execute-query {params.endpoint_batch0} --queryfile={input} --outfile={output}")

rule instanciate_workload:
    threads: 1
    input: 
        queryfile=expand("{queryDir}/{{query}}.sparql", queryDir=QUERY_DIR),
        workload_value_selection="{benchDir}/{query}/workload_value_selection.csv"
    output:
        injected_query="{benchDir}/{query}/instance_{instance_id}/injected.sparql",
    params:
        batch_id = 0
    run:
        shell("python -m fedshop.query instanciate-workload {input.queryfile} {input.workload_value_selection} {output.injected_query} {wildcards.instance_id}")
        
rule create_workload_value_selection:
    threads: 5
    input: 
        value_selection_infos="{benchDir}/{query}/value_selection.json"
    output: "{benchDir}/{query}/workload_value_selection.csv"
    params:
        n_query_instances = N_QUERY_INSTANCES,
    run:
        SPARQL_CONTAINER_NAME = f"docker-{SPARQL_SERVICE_NAME}-1"
        if USE_DOCKER :
            if not docker_check_container_running(SPARQL_CONTAINER_NAME):
                shell(f'docker compose -f {SPARQL_COMPOSE_FILE} stop')
                shell(f"docker start {SPARQL_CONTAINER_NAME}")
            while not ping(SPARQL_DEFAULT_ENDPOINT):
                LOGGER.debug(f"Waiting for {SPARQL_DEFAULT_ENDPOINT} to start...")
                time.sleep(1)

        constfile = f"{QUERY_DIR}/{wildcards.query}.const.json"
        shell(f"python -m fedshop.query create-workload-value-selection {CONFIGFILE} {constfile} {input.value_selection_infos} {output} {params.n_query_instances}")

rule build_value_selection_query:
    threads: 5
    input: 
        constfile = expand("{queryDir}/{{query}}.const.json", queryDir=QUERY_DIR),
        queryfile = expand("{queryDir}/{{query}}.sparql", queryDir=QUERY_DIR)
    output: "{benchDir}/{query}/value_selection.json"
    shell: "python -m fedshop.query build-value-selection-query {input.queryfile} {input.constfile} {output}"
