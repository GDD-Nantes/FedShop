import json

import numpy as np
import os
from pathlib import Path
import re

import sys
smk_directory = os.path.abspath(workflow.basedir)
sys.path.append(os.path.join(Path(smk_directory).parent, "fedshop"))

from utils import ping, fedshop_logger, load_config, docker_check_container_running
from itertools import product
from omegaconf import OmegaConf
import time
import requests

print(config)

LOGGER = fedshop_logger(Path(__file__).name)

CONFIGFILE = config["configfile"]

WORK_DIR = "experiments/bsbm"
QUERY_DIR = f"{WORK_DIR}/queries"
MODEL_DIR = f"{WORK_DIR}/model"
DATA_DIR = f"{MODEL_DIR}/dataset"
BENCH_DIR = f"{WORK_DIR}/benchmark/generation"
TEMPLATE_DIR = f"{MODEL_DIR}/watdiv"

CONFIG = load_config(CONFIGFILE)
CONFIG_GEN = CONFIG["generation"]
CONFIG_EVAL = CONFIG["evaluation"]

USE_DOCKER = CONFIG["use_docker"]

SPARQL_COMPOSE_FILE = CONFIG_GEN["virtuoso"]["compose_file"]
VIRTUOSO_COMPOSE_CONFIG = load_config(SPARQL_COMPOSE_FILE)

SPARQL_SERVICE_NAME = CONFIG_GEN["virtuoso"]["service_name"]
SPARQL_CONTAINER_NAME = f"docker-{SPARQL_SERVICE_NAME}"
SPARQL_DEFAULT_ENDPOINT = CONFIG_GEN["virtuoso"]["default_endpoint"]
SPARQL_DEFAULT_PORT = CONFIG_GEN["virtuoso"]["port"]

DOCKER_EXEC_CMD = f"docker exec {SPARQL_CONTAINER_NAME}"

# Modify the path to the Virtuoso ISQL executable and the path to the data
VIRTUOSO_PATH_TO_ISQL = CONFIG_GEN["virtuoso"]["isql"]
VIRTUOSO_PATH_TO_DATA = CONFIG_GEN["virtuoso"]["data_dir"]

if USE_DOCKER:
    VIRTUOSO_PATH_TO_ISQL = "/opt/virtuoso-opensource/bin/isql"
    VIRTUOSO_PATH_TO_DATA = "/usr/share/proj/" 

N_QUERY_INSTANCES = CONFIG_GEN["n_query_instances"]
VERBOSE = CONFIG_GEN["verbose"]
N_BATCH = CONFIG_GEN["n_batch"]

BATCHES = range(N_BATCH)

if "batches" in config:
    BATCHES = str(config["batches"]).split(",")
    if len(BATCHES) == 0:
        BATCHES = range(N_BATCH)

global NET_PORT

# Uncomment this line if starting from scratch
NET_PORT = 34200

#=================
# USEFUL FUNCTIONS
#=================
def validate(filename):
    with open(filename, "w") as f:
        f.write("ok")

def ping(endpoint):
    try:
        req = requests.get(endpoint)
        #LOGGER.debug(f"Response code: {req.status_code} for {endpoint}")
        return req.status_code == 200
    except requests.exceptions.ConnectionError:
        return False

def get_data_files(wildcards):
    n_vendor = 10 * (int(wildcards.batch_id) + 1)
    n_ratingsite = 10 * (int(wildcards.batch_id) + 1)
    
    return [ f"{DATA_DIR}/vendor{vendor_id}.nq" for vendor_id in range(n_vendor) ] + [ f"{DATA_DIR}/ratingsite{ratingsite_id}.nq" for ratingsite_id in range(n_ratingsite) ]

#=================
# PIPELINE
#=================

rule all:
    input: 
        expand(
            "{workDir}/virtuoso-federation-endpoints-batch{batch_id}-ok.txt",
            workDir=WORK_DIR,
            batch_id=BATCHES
        )

rule create_federation_endpoints:
    #input: f"{WORK_DIR}/virtuoso-batch-endpoints-ok.txt"
    input: "{workDir}/virtuoso-data-batch{batch_id}-ok.txt"
    output: "{workDir}/virtuoso-federation-endpoints-batch{batch_id}-ok.txt"
    run:
        SPARQL_CONTAINER_NAME = f"docker-{SPARQL_SERVICE_NAME}-{int(wildcards.batch_id)+1}"
        shell(f'docker compose -f {SPARQL_COMPOSE_FILE} stop')
        shell(f'docker start {SPARQL_CONTAINER_NAME}')

        while not ping(SPARQL_DEFAULT_ENDPOINT):
            LOGGER.debug(f"Waiting for {SPARQL_DEFAULT_ENDPOINT} to start...")
            time.sleep(1)

        global NET_PORT
        proxy_mapping = {}
        virtuoso_mapping_file = f"{WORK_DIR}/virtuoso-proxy-mapping-batch{wildcards.batch_id}.json"

        if os.path.exists(virtuoso_mapping_file):
            with open(virtuoso_mapping_file, "r") as f:
                proxy_mapping = json.load(f)

        # Create the federation endpoints
        federation_members_info = CONFIG_GEN["virtuoso"]["federation_members"][f"batch{wildcards.batch_id}"]
        for fed_member_name, fed_member_iri in federation_members_info.items():
            lpath = f"/{fed_member_name}/sparql"
            host = f"localhost:{NET_PORT}"
            proxy_target = f"http://{host}{lpath}"
            proxy_mapping[fed_member_iri] = proxy_target
            
            if USE_DOCKER:
                shell(f"python -m fedshop.virtuoso create-sparql-endpoint --container-name={SPARQL_CONTAINER_NAME} --on-duplicate=REPLACE --host={host} --lpath={lpath} {fed_member_iri}")
            else:
                shell(f'python -m fedshop.virtuoso create-sparql-endpoint --isql="{VIRTUOSO_PATH_TO_ISQL}" --on-duplicate=REPLACE --host={host} --lpath={lpath} {fed_member_iri}')

            NET_PORT += 1

        with open(virtuoso_mapping_file, "w") as f:
            json.dump(proxy_mapping, f)
        validate(str(output))
        
rule ingest_data:
    input:
        datafiles=get_data_files,
        containers_created="{workDir}/virtuoso-containers-ok.txt"
    output: "{workDir}/virtuoso-data-batch{batch_id}-ok.txt"
    run:
        SPARQL_CONTAINER_NAME = f"docker-{SPARQL_SERVICE_NAME}-{int(wildcards.batch_id)+1}"
        if USE_DOCKER:
            if not docker_check_container_running(SPARQL_CONTAINER_NAME):
                shell(f'docker compose -f {SPARQL_COMPOSE_FILE} stop')
                shell(f"docker start {SPARQL_CONTAINER_NAME}")
            while not ping(SPARQL_DEFAULT_ENDPOINT):
                LOGGER.debug(f"Waiting for {SPARQL_DEFAULT_ENDPOINT} to start...")
                time.sleep(1)

            
            datafiles = [ f.replace(DATA_DIR + "/", "") for f in input.datafiles ]
            shell(f'python -m fedshop.virtuoso ingest-data --container-name {SPARQL_CONTAINER_NAME} --datafiles "{",".join(datafiles)}"')
        else:
            shell(f'python -m fedshop.virtuoso ingest-data --isql "{VIRTUOSO_PATH_TO_ISQL}" --datapath {os.path.realpath(VIRTUOSO_PATH_TO_DATA)} --datafiles "{",".join(datafiles)}"')
        
        validate(str(output))

rule create_batches:
    output: "{workDir}/virtuoso-containers-ok.txt"
    run:
        shell(f"docker compose -f {SPARQL_COMPOSE_FILE} create --no-recreate --scale {SPARQL_SERVICE_NAME}={N_BATCH} && touch {output}")