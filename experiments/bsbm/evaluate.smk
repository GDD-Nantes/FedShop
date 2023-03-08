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
from itertools import pairwise

import sys
smk_directory = os.path.abspath(workflow.basedir)
sys.path.append(os.path.join(Path(smk_directory).parent.parent, "rsfb"))

from utils import load_config, get_docker_endpoint_by_container_name, check_container_status

#===============================
# EVALUATION PHASE:
# - Compile engines
# - Generate results and source selection for each engine
# - Generate metrics and stats for each engine
#===============================

CONFIGFILE = config["configfile"]

WORK_DIR = "experiments/bsbm"
CONFIG = load_config(CONFIGFILE)

CONFIG_GEN = CONFIG["generation"]
CONFIG_EVAL = CONFIG["evaluation"]

SPARQL_COMPOSE_FILE = CONFIG_GEN["virtuoso"]["compose_file"]
SPARQL_SERVICE_NAME = CONFIG_GEN["virtuoso"]["service_name"]
SPARQL_CONTAINER_NAMES = CONFIG_GEN["virtuoso"]["container_names"]

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

#=================
# USEFUL FUNCTIONS
#=================

def wait_for_container(endpoints, outfile, wait=1):
    if isinstance(endpoints, str):
        endpoints = [ endpoints ]
    endpoint_ok = 0
    attempt=1
    print(f"Waiting for all endpoints...")
    while(endpoint_ok < len(endpoints)):
        print(f"Attempt {attempt} ...")
        try:
            for endpoint in endpoints:
                status = requests.get(endpoint).status_code
                if status == 200:
                    print(f"{endpoint} is ready!")
                    endpoint_ok += 1   
        except: pass
        attempt += 1
        time.sleep(wait)

    with open(f"{outfile}", "w") as f:
        f.write("OK")

def activate_one_container(container_infos_file, batch_id):
    """ Activate one container while stopping all others
    """
    container_infos_file = str(container_infos_file)
    container_infos = pd.read_csv(container_infos_file)
    batch_id = int(batch_id)
    container_name = container_infos.loc[batch_id, "Name"]

    if (container_status := check_container_status(SPARQL_COMPOSE_FILE, SPARQL_SERVICE_NAME, container_name)) is None:
        deploy_virtuoso(container_infos_file, restart=True)

    if container_status != "running":
        print("Stopping all containers...")
        shell(f"docker-compose -f {SPARQL_COMPOSE_FILE} stop {SPARQL_SERVICE_NAME}")
            
        print(f"Starting container {container_name}...")
        shell(f"docker start {container_name}")
        container_endpoint = get_docker_endpoint_by_container_name(SPARQL_COMPOSE_FILE, SPARQL_SERVICE_NAME, container_name)
        wait_for_container(container_endpoint, f"{BENCH_DIR}/virtuoso-ok.txt", wait=1)

def generate_federation_declaration(federation_declaration_file, engine, batch_id):
    sparql_endpoint = get_docker_endpoint_by_container_name(SPARQL_COMPOSE_FILE, SPARQL_SERVICE_NAME, SPARQL_CONTAINER_NAMES[LAST_BATCH])

    is_endpoint_updated = False
    if is_file_exists := os.path.exists(federation_declaration_file):
        with open(federation_declaration_file) as f:
            search_string = f'sd:endpoint "{sparql_endpoint}'
            is_endpoint_updated = search_string not in f.read()

    if is_endpoint_updated or not is_file_exists:
        print(f"Rewriting {engine} configfile as it is updated!")
        container_infos_file = f"{WORK_DIR}/benchmark/generation/container_infos.csv"
        ratingsite_data_files = [ f"{MODEL_DIR}/dataset/ratingsite{i}.nq" for i in range(N_RATINGSITE) ]
        vendor_data_files = [ f"{MODEL_DIR}/dataset/vendor{i}.nq" for i in range(N_VENDOR) ]

        batch_id = int(batch_id)
        ratingsiteSliceId = np.histogram(np.arange(N_RATINGSITE), N_BATCH)[1][1:].astype(int)[batch_id]
        vendorSliceId = np.histogram(np.arange(N_VENDOR), N_BATCH)[1][1:].astype(int)[batch_id]
        batch_files = ratingsite_data_files[:ratingsiteSliceId+1] + vendor_data_files[:vendorSliceId+1]

        activate_one_container(container_infos_file, LAST_BATCH)
        shell(f"python rsfb/engines/{engine}.py generate-config-file {' '.join(batch_files)} {federation_declaration_file} {CONFIGFILE} --endpoint {sparql_endpoint}")

#=================
# PIPELINE
#=================

rule all:
    input: expand("{benchDir}/metrics.csv", benchDir=BENCH_DIR)

rule merge_metrics:
    priority: 1
    input: expand("{{benchDir}}/metrics_batch{batch_id}.csv", batch_id=range(N_BATCH))
    output: "{benchDir}/metrics.csv"
    run: pd.concat((pd.read_csv(f) for f in input)).to_csv(f"{output}", index=False)

rule merge_batch_metrics:
    priority: 1
    input: 
        metrics="{benchDir}/eval_metrics_batch{batch_id}.csv",
        stats="{benchDir}/eval_stats_batch{batch_id}.csv"
    output: "{benchDir}/metrics_batch{batch_id}.csv"
    run:
        metrics_df = pd.read_csv(f"{input.metrics}")
        stats_df = pd.read_csv(f"{input.stats}")
        out_df = pd.merge(metrics_df, stats_df, on = ["query", "batch", "instance", "engine", "attempt"], how="left")
        out_df.to_csv(str(output), index=False)

rule merge_stats:
    input: 
        expand(
            "{{benchDir}}/{engine}/{query}/instance_{instance_id}/batch_{{batch_id}}/attempt_{attempt_id}/stats.csv", 
            engine=CONFIG_EVAL["engines"],
            query=[Path(os.path.join(QUERY_DIR, f)).resolve().stem for f in os.listdir(QUERY_DIR) if f.endswith(".sparql")],
            instance_id=range(N_QUERY_INSTANCES),
            attempt_id=range(CONFIG_EVAL["n_attempts"])
        )
    output: "{benchDir}/eval_stats_batch{batch_id}.csv"
    run: pd.concat((pd.read_csv(f) for f in input)).to_csv(f"{output}", index=False)

rule compute_metrics:
    priority: 2
    threads: 1
    input: 
        provenance=expand(
            "{{benchDir}}/{engine}/{query}/instance_{instance_id}/batch_{{batch_id}}/attempt_{attempt_id}/provenance.csv", 
            engine=CONFIG_EVAL["engines"],
            query=[Path(os.path.join(QUERY_DIR, f)).resolve().stem for f in os.listdir(QUERY_DIR) if f.endswith(".sparql")],
            instance_id=range(N_QUERY_INSTANCES),
            attempt_id=range(CONFIG_EVAL["n_attempts"])
        ),
        results=expand(
            "{{benchDir}}/{engine}/{query}/instance_{instance_id}/batch_{{batch_id}}/attempt_{attempt_id}/results.csv", 
            engine=CONFIG_EVAL["engines"],
            query=[Path(os.path.join(QUERY_DIR, f)).resolve().stem for f in os.listdir(QUERY_DIR) if f.endswith(".sparql")],
            instance_id=range(N_QUERY_INSTANCES),
            attempt_id=range(CONFIG_EVAL["n_attempts"])
        ),
    output: "{benchDir}/eval_metrics_batch{batch_id}.csv"
    shell: "python rsfb/metrics.py compute-metrics {CONFIGFILE} {output} {input.provenance}"

rule transform_provenance:
    input: "{benchDir}/{engine}/{query}/instance_{instance_id}/batch_{batch_id}/attempt_{attempt_id}/source_selection.txt"
    output: "{benchDir}/{engine}/{query}/instance_{instance_id}/batch_{batch_id}/attempt_{attempt_id}/provenance.csv"
    params:
        prefix_cache=expand("{workDir}/benchmark/generation/{{query}}/instance_{{instance_id}}/prefix_cache.json", workDir=WORK_DIR)
    run: 
        shell("python rsfb/engines/{wildcards.engine}.py transform-provenance {input} {output} {params.prefix_cache}")

rule transform_results:
    input: "{benchDir}/{engine}/{query}/instance_{instance_id}/batch_{batch_id}/attempt_{attempt_id}/results.txt"
    output: "{benchDir}/{engine}/{query}/instance_{instance_id}/batch_{batch_id}/attempt_{attempt_id}/results.csv"
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
                .drop_duplicates() \
                .reset_index(drop=True) 

            if not expected_results.equals(engine_results):
                print(expected_results)
                print("not equals to")
                print(engine_results)
                raise RuntimeError(f"{wildcards.engine} does not produce the expected results")

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
        engine_status="{benchDir}/{engine}/{engine}-ok.txt",
        container_infos=expand("{workDir}/benchmark/generation/container_infos.csv", workDir=WORK_DIR)
    output: 
        stats="{benchDir}/{engine}/{query}/instance_{instance_id}/batch_{batch_id}/attempt_{attempt_id}/stats.csv",
        query_plan="{benchDir}/{engine}/{query}/instance_{instance_id}/batch_{batch_id}/attempt_{attempt_id}/query_plan.txt",
        source_selection="{benchDir}/{engine}/{query}/instance_{instance_id}/batch_{batch_id}/attempt_{attempt_id}/source_selection.txt",
        result_txt="{benchDir}/{engine}/{query}/instance_{instance_id}/batch_{batch_id}/attempt_{attempt_id}/results.txt",
    params:
        eval_config=expand("{workDir}/config.yaml", workDir=WORK_DIR),
        engine_config="{benchDir}/{engine}/config/batch_{batch_id}/{engine}.conf",
    run: 
        activate_one_container(input.container_infos, LAST_BATCH)

        engine = str(wildcards.engine)
        batch_id = int(wildcards.batch_id)
        engine_config = f"{WORK_DIR}/benchmark/evaluation/{engine}/config/batch_{batch_id}/{engine}.conf"
        generate_federation_declaration(engine_config, engine, batch_id)

        # configPath, queryPath, outResultPath, outSourceSelectionPath, outQueryPlanFile, statPath, inSourceSelectionPath
        previous_batch = batch_id - 1
        same_file_previous_batch = f"{BENCH_DIR}/{wildcards.engine}/{wildcards.query}/instance_{wildcards.instance_id}/batch_{previous_batch}/attempt_{wildcards.attempt_id}/results.txt"
        
        if os.stat(same_file_previous_batch).st_size == 0 and batch_id > 0:
            print(f"Skip evaluation for this because {same_file_previous_batch} timed out")
            shell(f"cp {BENCH_DIR}/{wildcards.engine}/{wildcards.query}/instance_{wildcards.instance_id}/batch_{previous_batch}/attempt_{wildcards.attempt_id}/stats.csv {output.stats}")
            shell(f"cp {BENCH_DIR}/{wildcards.engine}/{wildcards.query}/instance_{wildcards.instance_id}/batch_{previous_batch}/attempt_{wildcards.attempt_id}/query_plan.txt {output.query_plan}")
            shell(f"cp {BENCH_DIR}/{wildcards.engine}/{wildcards.query}/instance_{wildcards.instance_id}/batch_{previous_batch}/attempt_{wildcards.attempt_id}/source_selection.txt {output.source_selection}")
            shell(f"cp {BENCH_DIR}/{wildcards.engine}/{wildcards.query}/instance_{wildcards.instance_id}/batch_{previous_batch}/attempt_{wildcards.attempt_id}/results.txt {output.result_txt}")
        else:
            shell("python rsfb/engines/{engine}.py run-benchmark {params.eval_config} {params.engine_config} {input.query} --out-result {output.result_txt}  --out-source-selection {output.source_selection} --stats {output.stats} --force-source-selection {input.engine_source_selection} --query-plan {output.query_plan} --batch-id {wildcards.batch_id}")

rule engines_prerequisites:
    output: "{benchDir}/{engine}/{engine}-ok.txt"
    params:
        eval_config=expand("{workDir}/config.yaml", workDir=WORK_DIR)
    shell: "python rsfb/engines/{wildcards.engine}.py prerequisites {params.eval_config} && echo 'OK' > {output}"

