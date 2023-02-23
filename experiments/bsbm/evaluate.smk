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
        container_endpoint = get_virtuoso_endpoint_by_container_name(SPARQL_COMPOSE_FILE, SPARQL_SERVICE_NAME, container_name)
        wait_for_container(container_endpoint, f"{BENCH_DIR}/virtuoso-ok.txt", wait=1)

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
        out_df = pd.merge(metrics_df, stats_df, on = ["query", "batch", "instance", "engine"], how="left")
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
    run: pd.concat((pd.read_csv(f, sep = ';') for f in input)).to_csv(f"{output}", index=False, sep = ';')

rule evaluate_source_selection_on_fedx:
    """Evaluate queries using each engine's source selection on FedX.
    
    - Output: only statistics, no source-seleciton
    """
    threads: 1
    #retries: 3
    input: 
        query=expand("{workDir}/benchmark/generation/{{query}}/instance_{{instance_id}}/injected.sparql", workDir=WORK_DIR),
        engine_source_selection=expand("{{benchDir}}/{{engine}}/{{query}}/instance_{{instance_id}}/batch_{{batch_id}}/provenance.csv", workDir=WORK_DIR),
        engine_config="{benchDir}/{engine}/config/batch_{batch_id}/{engine}.conf",
        virtuoso_last_batch=expand("{workDir}/benchmark/generation/virtuoso_batch{batch_n}-ok.txt", workDir=WORK_DIR, batch_n=N_BATCH-1),
        engine_status="{benchDir}/{engine}/{engine}-ok.txt",
        container_infos=expand("{workDir}/benchmark/generation/container_infos.csv", workDir=WORK_DIR)
    output: 
        stats="{benchDir}/{engine}/{query}/instance_{instance_id}/batch_{batch_id}/attempt_{attempt_id}/stats.csv",
    params:
        eval_config=expand("{workDir}/config.yaml", workDir=WORK_DIR),
        out_source_selection="/dev/null",
        result="{benchDir}/{engine}/{query}/instance_{instance_id}/batch_{batch_id}/attempt_{attempt_id}/results.txt",
        query_plan="{benchDir}/{engine}/{query}/instance_{instance_id}/batch_{batch_id}/attempt_{attempt_id}/query_plan.txt"
    run: 
        engine_source_selection = pd.read_csv(str(input.engine_source_selection))
        hasDuplicates = engine_source_selection.duplicated().any()
        if hasDuplicates:
            raise RuntimeError(f"{input.engine_source_selection} contains duplicates!")
        activate_one_container(input.container_infos, LAST_BATCH)
        
        # configPath, queryPath, outResultPath, outSourceSelectionPath, outQueryPlanFile, statPath, inSourceSelectionPath
        shell("python rsfb/engines/fedx.py run-benchmark {params.eval_config} {input.engine_config} {input.query} --out-result {params.result} --out-source-selection {params.out_source_selection} --query-plan {params.query_plan} --stats {output.stats} --force-source-selection {input.engine_source_selection} --batch-id {wildcards.batch_id}")


rule compute_metrics:
    priority: 2
    threads: 1
    input: 
        expand(
            "{{benchDir}}/{engine}/{query}/instance_{instance_id}/batch_{{batch_id}}/provenance.csv", 
            engine=CONFIG_EVAL["engines"],
            query=[Path(os.path.join(QUERY_DIR, f)).resolve().stem for f in os.listdir(QUERY_DIR) if f.endswith(".sparql")],
            instance_id=range(N_QUERY_INSTANCES)
        )
    output: "{benchDir}/eval_metrics_batch{batch_id}.csv"
    shell: "python rsfb/metrics.py compute-metrics {CONFIGFILE} {output} {input}"

rule transform_result:
    input: "{benchDir}/{engine}/{query}/instance_{instance_id}/batch_{batch_id}/results.ss"
    output: "{benchDir}/{engine}/{query}/instance_{instance_id}/batch_{batch_id}/provenance.csv"
    params:
        prefix_cache=expand("{workDir}/benchmark/generation/{{query}}/instance_{{instance_id}}/prefix_cache.json", workDir=WORK_DIR)
    shell: "python rsfb/engines/{wildcards.engine}.py transform-result {input} {output} {params.prefix_cache}"

rule extract_source_selection_from_engines:
    """Extract source selection from each engine, without stats
    """
    threads: 1
    #retries: 3
    input: 
        query=expand("{workDir}/benchmark/generation/{{query}}/instance_{{instance_id}}/injected.sparql", workDir=WORK_DIR),
        engine_config=expand("{workDir}/benchmark/evaluation/{{engine}}/config/batch_{{batch_id}}/{{engine}}.conf", workDir=WORK_DIR),
        virtuoso_last_batch=expand("{workDir}/benchmark/generation/virtuoso_batch{batch_n}-ok.txt", workDir=WORK_DIR, batch_n=N_BATCH-1),
        engine_status="{benchDir}/{engine}/{engine}-ok.txt",
        container_infos=expand("{workDir}/benchmark/generation/container_infos.csv", workDir=WORK_DIR)
    output: 
        source_selection="{benchDir}/{engine}/{query}/instance_{instance_id}/batch_{batch_id}/results.ss",
    params:
        eval_config=expand("{workDir}/config.yaml", workDir=WORK_DIR)
    run:
        activate_one_container(input.container_infos, LAST_BATCH)
        # configPath, queryPath, outResultPath, outSourceSelectionPath, outQueryPlanFile, statPath, inSourceSelectionPath
        shell("python rsfb/engines/{wildcards.engine}.py run-benchmark {params.eval_config} {input.engine_config} {input.query} --out-source-selection {output.source_selection} --batch-id {wildcards.batch_id}")

rule engines_prerequisites:
    output: "{benchDir}/{engine}/{engine}-ok.txt"
    params:
        eval_config=expand("{workDir}/config.yaml", workDir=WORK_DIR)
    shell: "python rsfb/engines/{wildcards.engine}.py prerequisites {params.eval_config} && echo 'OK' > {output}"

rule generate_federation_declaration:
    input: 
        container_infos=expand("{workDir}/benchmark/generation/container_infos.csv", workDir=WORK_DIR)
    output: "{benchDir}/{engine}/config/batch_{batch_id}/{engine}.conf"
    run: 
        ratingsite_data_files = [ f"{MODEL_DIR}/dataset/ratingsite{i}.nq" for i in range(N_RATINGSITE) ]
        vendor_data_files = [ f"{MODEL_DIR}/dataset/vendor{i}.nq" for i in range(N_VENDOR) ]

        batchId = int(wildcards.batch_id)
        ratingsiteSliceId = np.histogram(np.arange(N_RATINGSITE), N_BATCH)[1][1:].astype(int)[batchId]
        vendorSliceId = np.histogram(np.arange(N_VENDOR), N_BATCH)[1][1:].astype(int)[batchId]
        batch_files = ratingsite_data_files[:ratingsiteSliceId+1] + vendor_data_files[:vendorSliceId+1]

        activate_one_container(input.container_infos, LAST_BATCH)
        sparql_endpoint = get_virtuoso_endpoint_by_container_name(SPARQL_COMPOSE_FILE, SPARQL_SERVICE_NAME, SPARQL_CONTAINER_NAMES[LAST_BATCH])

        shell(f"python rsfb/engines/{wildcards.engine}.py generate-config-file {' '.join(batch_files)} {output} --endpoint {sparql_endpoint}")

