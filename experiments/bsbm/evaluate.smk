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

import sys
smk_directory = os.path.abspath(workflow.basedir)
sys.path.append(os.path.join(Path(smk_directory).parent.parent, "rsfb"))

from utils import load_config, get_virtuoso_endpoint_by_container_name, check_container_status

WORK_DIR = "experiments/bsbm"
CONFIG = load_config(f"{WORK_DIR}/config.yaml")

CONFIG_GEN = CONFIG["generation"]
CONFIG_EVAL = CONFIG["evaluation"]

SPARQL_ENDPOINT = CONFIG_GEN["virtuoso"]["endpoints"]

SPARQL_COMPOSE_FILE = CONFIG_GEN["virtuoso"]["compose_file"]
SPARQL_SERVICE_NAME = CONFIG_GEN["virtuoso"]["service_name"]
SPARQL_CONTAINER_NAMES = CONFIG_GEN["virtuoso"]["container_names"]

N_QUERY_INSTANCES = CONFIG_GEN["n_query_instances"]
N_BATCH = CONFIG_GEN["n_batch"]

# Config per batch
N_VENDOR=CONFIG_GEN["schema"]["vendor"]["params"]["vendor_n"]
N_RATINGSITE=CONFIG_GEN["schema"]["ratingsite"]["params"]["ratingsite_n"]

FEDERATION_COUNT=N_VENDOR+N_RATINGSITE

QUERY_DIR = f"{WORK_DIR}/queries"
MODEL_DIR = f"{WORK_DIR}/model"
BENCH_DIR = f"{WORK_DIR}/benchmark/evaluation"
TEMPLATE_DIR = f"{MODEL_DIR}/watdiv"

#=================
# USEFUL FUNCTIONS
#=================

def wait_for_container(endpoint, outfile, wait=1):
    endpoint_ok = False
    attempt=1
    print(f"Waiting for {endpoint}...")
    while(not endpoint_ok):
        print(f"Attempt {attempt} ...")
        try: endpoint_ok = ( requests.get(endpoint).status_code == 200 )
        except: pass
        attempt += 1
        time.sleep(wait)

    with open(f"{outfile}", "w+") as f:
        f.write("OK")
        f.close()

def prerequisite_for_engine(wildcards):
    engine = str(wildcards.engine)
    return f"{WORK_DIR}/benchmark/generation/virtuoso_batch{N_BATCH-1}-ok.txt"

def activate_one_container(container_infos_file, batch_id):
    """ Activate one container while stopping all others
    """
    container_infos_file = str(container_infos_file)
    container_infos = pd.read_csv(container_infos_file)
    batch_id = int(batch_id)
    container_name = container_infos.loc[batch_id, "Name"]

    if check_container_status(SPARQL_COMPOSE_FILE, SPARQL_SERVICE_NAME, container_name) != "running":
        print("Stopping all containers...")
        shell(f"docker-compose -f {SPARQL_COMPOSE_FILE} stop {SPARQL_SERVICE_NAME}")
            
        print(f"Starting container {container_name}...")
        shell(f"docker start {container_name}")
        container_endpoint = get_virtuoso_endpoint_by_container_name(SPARQL_COMPOSE_FILE, SPARQL_SERVICE_NAME, container_name)
        wait_for_container(container_endpoint, f"{WORK_DIR}/benchmark/generation/virtuoso-ok.txt", wait=1)

#=================
# PIPELINE
#=================

rule all:
    input: expand("{benchDir}/metrics.csv", benchDir=BENCH_DIR)

rule merge_metrics:
    priority: 1
    input: expand("{{benchDir}}/metrics_batch{batch_id}.csv", batch_id=range(N_BATCH))
    #input: expand("{{benchDir}}/metrics_batch{batch_id}.csv", batch_id=0)
    output: "{benchDir}/metrics.csv"
    run: pd.concat((pd.read_csv(f, sep = ';') for f in input)).to_csv(f"{output}", index=False, sep = ';')

rule merge_batch_metrics:
    input: 
        expand(
            "{{benchDir}}/{engine}/{query}/instance_{instance_id}/batch_{{batch_id}}/{mode}/stats.csv", 
            engine=CONFIG_EVAL["engines"],
            query=[Path(os.path.join(QUERY_DIR, f)).resolve().stem for f in os.listdir(QUERY_DIR) if f.endswith(".sparql")],
            instance_id=range(N_QUERY_INSTANCES),
            #mode=["ideal"]
            mode=["default", "ideal"]
        )
    output: "{benchDir}/metrics_batch{batch_id}.csv"
    run: pd.concat((pd.read_csv(f, sep = ';') for f in input)).to_csv(f"{output}", index=False, sep = ';')

rule measure_default_stats:
   threads: 1
   #retries: 3
   input: 
        query=expand("{workDir}/benchmark/generation/{{query}}/instance_{{instance_id}}/injected.sparql", workDir=WORK_DIR),
        engine_config=expand("{workDir}/benchmark/evaluation/{{engine}}/config/batch_{{batch_id}}/{{engine}}.conf", workDir=WORK_DIR),
        virtuoso_last_batch=expand("{workDir}/benchmark/generation/virtuoso_batch{batch_n}-ok.txt", workDir=WORK_DIR, batch_n=N_BATCH-1),
        engine_status="{benchDir}/{engine}/{engine}-ok.txt",
        container_infos=expand("{workDir}/benchmark/generation/container_infos.csv", workDir=WORK_DIR)
   output: 
       results="{benchDir}/{engine}/{query}/instance_{instance_id}/batch_{batch_id}/default/results",
       stats="{benchDir}/{engine}/{query}/instance_{instance_id}/batch_{batch_id}/default/stats.csv"
   params:
       eval_config=expand("{workDir}/config.yaml", workDir=WORK_DIR)
   run:
        activate_one_container(input.container_infos, wildcards.batch_id)
        shell("python rsfb/engines/{wildcards.engine}.py run-benchmark {params.eval_config} {input.engine_config} {input.query} {output.results} {output.stats}")
 
rule measure_ideal_source_selection_stats:
    threads: 1
    #retries: 3
    input: 
        query=expand("{workDir}/benchmark/generation/{{query}}/instance_{{instance_id}}/injected.sparql", workDir=WORK_DIR),
        ideal_ss=expand("{workDir}/benchmark/generation/{{query}}/instance_{{instance_id}}/batch_{{batch_id}}/provenance.csv", workDir=WORK_DIR),
        engine_config="{benchDir}/{engine}/config/batch_{batch_id}/{engine}.conf",
        virtuoso_last_batch=expand("{workDir}/benchmark/generation/virtuoso_batch{batch_n}-ok.txt", workDir=WORK_DIR, batch_n=N_BATCH-1),
        engine_status="{benchDir}/{engine}/{engine}-ok.txt",
        container_infos=expand("{workDir}/benchmark/generation/container_infos.csv", workDir=WORK_DIR)
    output: 
        results="{benchDir}/{engine}/{query}/instance_{instance_id}/batch_{batch_id}/ideal/results",
        stats="{benchDir}/{engine}/{query}/instance_{instance_id}/batch_{batch_id}/ideal/stats.csv",
    params:
        eval_config=expand("{workDir}/config.yaml", workDir=WORK_DIR)
    run: 
        activate_one_container(input.container_infos, wildcards.batch_id)
        shell("python rsfb/engines/{wildcards.engine}.py run-benchmark {params.eval_config} {input.engine_config} {input.query} {output.results} {output.stats} --ideal-ss {input.ideal_ss}")

rule engines_prerequisites:
    output: "{benchDir}/{engine}/{engine}-ok.txt"
    params:
        eval_config=expand("{workDir}/config.yaml", workDir=WORK_DIR)
    shell: "python rsfb/engines/{wildcards.engine}.py prerequisites {params.eval_config} && echo 'OK' > {output}"

rule generate_federation_declaration:
    output: "{benchDir}/{engine}/config/batch_{batch_id}/{engine}.conf"
    run: 
        ratingsite_data_files = [ f"{MODEL_DIR}/dataset/ratingsite{i}.nq" for i in range(N_RATINGSITE) ]
        vendor_data_files = [ f"{MODEL_DIR}/dataset/vendor{i}.nq" for i in range(N_VENDOR) ]

        batchId = int(wildcards.batch_id)
        ratingsiteSliceId = np.histogram(np.arange(N_RATINGSITE), N_BATCH)[1][1:].astype(int)[batchId]
        vendorSliceId = np.histogram(np.arange(N_VENDOR), N_BATCH)[1][1:].astype(int)[batchId]
        batch_files = ratingsite_data_files[:ratingsiteSliceId+1] + vendor_data_files[:vendorSliceId+1]

        SPARQL_ENDPOINT = get_virtuoso_endpoint_by_container_name(SPARQL_COMPOSE_FILE, SPARQL_SERVICE_NAME, SPARQL_CONTAINER_NAMES[batchId])

        if str(wildcards.engine) == "splendid":
            SPLENDID_PROPERTIES = CONFIG_EVAL["engines"]["splendid"]["properties"]
            lines = []
            with open(f"{SPLENDID_PROPERTIES}", "r") as properties_file:
                lines = properties_file.readlines()
                for i in range(len(lines)):
                    if lines[i].startswith("sparql.endpoint"):
                        lines[i] = re.sub(r'sparql.endpoint=.+',r'sparql.endpoint='+str(SPARQL_ENDPOINT), lines[i])
            with open(f"{SPLENDID_PROPERTIES}", "w") as properties_file:
                properties_file.writelines(lines)

        os.system(f"python rsfb/engines/{wildcards.engine}.py generate-config-file {' '.join(batch_files)} {output} --endpoint {SPARQL_ENDPOINT}")

