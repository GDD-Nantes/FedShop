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
sys.path.append(os.path.join(Path(smk_directory).parent, "fedshop"))

from utils import ping, fedshop_logger, load_config, create_stats, docker_check_container_running

#===============================
# EVALUATION PHASE:
# - Compile engines
# - Generate results and source selection for each engine
# - Generate metrics and stats for each engine
#===============================

print(config)

CONFIGFILE = config["configfile"]
WORK_DIR = "experiments/bsbm"
CONFIG = load_config(CONFIGFILE)

USE_DOCKER = CONFIG["use_docker"]

CONFIG_GEN = CONFIG["generation"]
CONFIG_EVAL = CONFIG["evaluation"]

SPARQL_COMPOSE_FILE = CONFIG_GEN["virtuoso"]["compose_file"]
SPARQL_SERVICE_NAME = CONFIG_GEN["virtuoso"]["service_name"]
SPARQL_DEFAULT_ENDPOINT = CONFIG_GEN["virtuoso"]["default_endpoint"]

VIRTUOSO_COMPOSE_CONFIG = load_config(SPARQL_COMPOSE_FILE)

# Modify the path to the Virtuoso ISQL executable and the path to the data
VIRTUOSO_PATH_TO_ISQL = CONFIG_GEN["virtuoso"]["isql"]
VIRTUOSO_PATH_TO_DATA = CONFIG_GEN["virtuoso"]["data_dir"]

if USE_DOCKER:
    VIRTUOSO_PATH_TO_ISQL = "/opt/virtuoso-opensource/bin/isql"
    VIRTUOSO_PATH_TO_DATA = "/usr/share/proj/" 

PROXY_COMPOSE_FILE =  CONFIG_EVAL["proxy"]["compose_file"]
PROXY_SERVICE_NAME = CONFIG_EVAL["proxy"]["service_name"]
PROXY_CONTAINER_NAME = CONFIG_EVAL["proxy"]["container_name"]
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

# Override setttings if specified in the config file
BATCH_ID = str(config["batch"]).split(",") if config.get("batch") is not None else range(N_BATCH)
ENGINE_ID = str(config["engine"]).split(",") if config.get("engine") is not None else CONFIG_EVAL["engines"]
QUERY_PATH = (
    [Path(os.path.join(QUERY_DIR, f)).resolve().stem for f in str(config["query"]).split(",")] 
    if config.get("query") is not None else 
    [Path(os.path.join(QUERY_DIR, f)).resolve().stem for f in os.listdir(QUERY_DIR) if f.endswith(".sparql")]
)
INSTANCE_ID = str(config["instance"]).split(",") if config.get("instance") is not None else range(N_QUERY_INSTANCES)

DEBUG = eval(str(config["debug"])) if config.get("explain") is not None else False

ATTEMPT_ID = str(config["attempt"]).split(",") if config.get("attempt") is not None else range(CONFIG_EVAL["n_attempts"])
if DEBUG:
    ATTEMPT_ID = ["debug"]

NO_EXEC = eval(str(config["explain"])) if config.get("explain") is not None else False
LOGGER = fedshop_logger(Path(__file__).name)

#=================
# USEFUL FUNCTIONS
#=================


#=================
# PIPELINE
#=================

rule all:
    input: expand("{benchDir}/metrics.csv", benchDir=BENCH_DIR)

rule merge_metrics:
    priority: 1
    input: expand("{{benchDir}}/metrics_batch{batch_id}.csv", batch_id=BATCH_ID)
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
        out_df = pd.merge(metrics_df, stats_df, on = ["query", "batch", "instance", "engine", "attempt"], how="inner")
        out_df.to_csv(str(output), index=False)

rule merge_stats:
    input: 
        expand(
            "{{benchDir}}/{engine}/{query}/instance_{instance_id}/batch_{{batch_id}}/attempt_{attempt_id}/stats.csv", 
            engine=ENGINE_ID,
            query=QUERY_PATH,
            instance_id=INSTANCE_ID,
            attempt_id=ATTEMPT_ID
        )
    output: "{benchDir}/eval_stats_batch{batch_id}.csv"
    run: pd.concat((pd.read_csv(f) for f in input)).to_csv(f"{output}", index=False)

rule compute_metrics:
    priority: 2
    threads: 1
    input: 
        provenance=expand(
            "{{benchDir}}/{engine}/{query}/instance_{instance_id}/batch_{{batch_id}}/attempt_{attempt_id}/provenance.csv", 
            engine=ENGINE_ID,
            query=QUERY_PATH,
            instance_id=INSTANCE_ID,
            attempt_id=ATTEMPT_ID
        ),
        results=expand(
            "{{benchDir}}/{engine}/{query}/instance_{instance_id}/batch_{{batch_id}}/attempt_{attempt_id}/results.csv", 
            engine=ENGINE_ID,
            query=QUERY_PATH,
            instance_id=INSTANCE_ID,
            attempt_id=ATTEMPT_ID
        ),
    output: "{benchDir}/eval_metrics_batch{batch_id}.csv"
    shell: "python -m fedshop.metrics compute-metrics {CONFIGFILE} {output} {input.provenance}"

rule transform_provenance:
    input: "{benchDir}/{engine}/{query}/instance_{instance_id}/batch_{batch_id}/attempt_{attempt_id}/source_selection.txt"
    output: "{benchDir}/{engine}/{query}/instance_{instance_id}/batch_{batch_id}/attempt_{attempt_id}/provenance.csv"
    params:
        composition=expand("{workDir}/benchmark/generation/{{query}}/instance_{{instance_id}}/composition.json", workDir=WORK_DIR)
    run: 
        shell("python -m fedshop.engines.{wildcards.engine} transform-provenance {input} {output} {params.composition}")

rule transform_results:
    input: "{benchDir}/{engine}/{query}/instance_{instance_id}/batch_{batch_id}/attempt_{attempt_id}/results.txt"
    output: "{benchDir}/{engine}/{query}/instance_{instance_id}/batch_{batch_id}/attempt_{attempt_id}/results.csv"
    run:
        # Transform results
        shell("python -m fedshop.engines.{wildcards.engine} transform-results {input} {output}")
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

                # if len(engine_results) < len(expected_results):
                #     raise RuntimeError(f"{wildcards.engine} does not produce the expected results")
            # else:
            #     create_stats(f"{Path(str(input)).parent}/stats.csv")

rule evaluate_engines:
    threads: 1
    retries: 1
    input: 
        query=ancient(expand("{workDir}/benchmark/generation/{{query}}/instance_{{instance_id}}/injected.sparql", workDir=WORK_DIR)),
        #virtuoso_ok=ancient(expand("{workDir}/virtuoso-federation-endpoints-ok.txt", workDir=WORK_DIR)),
        engine_status=ancient("{benchDir}/{engine}/{engine}-ok.txt"),
    output: 
        stats="{benchDir}/{engine}/{query}/instance_{instance_id}/batch_{batch_id}/attempt_{attempt_id}/stats.csv",
        source_selection="{benchDir}/{engine}/{query}/instance_{instance_id}/batch_{batch_id}/attempt_{attempt_id}/source_selection.txt",
        result_txt="{benchDir}/{engine}/{query}/instance_{instance_id}/batch_{batch_id}/attempt_{attempt_id}/results.txt",
    params:
        query_plan="{benchDir}/{engine}/{query}/instance_{instance_id}/batch_{batch_id}/attempt_{attempt_id}/query_plan.txt",
        result_csv="{benchDir}/{engine}/{query}/instance_{instance_id}/batch_{batch_id}/attempt_{attempt_id}/results.csv",
        last_batch=LAST_BATCH
    run: 
        SPARQL_CONTAINER_NAME = f"docker-{SPARQL_SERVICE_NAME}-{int(wildcards.batch_id)+1}"

        if USE_DOCKER:
            if not docker_check_container_running(SPARQL_CONTAINER_NAME):
                shell(f'docker compose -f {SPARQL_COMPOSE_FILE} stop')
                shell(f"docker start {SPARQL_CONTAINER_NAME}")
                while not ping(SPARQL_DEFAULT_ENDPOINT):
                    LOGGER.debug(f"Waiting for {SPARQL_DEFAULT_ENDPOINT} to start...")
                    time.sleep(1)

            if not docker_check_container_running(PROXY_CONTAINER_NAME):
                shell(f'docker compose -f {PROXY_COMPOSE_FILE} stop')
                shell(f"docker start {PROXY_CONTAINER_NAME}")
                while not ping(PROXY_SPARQL_ENDPOINT):
                    LOGGER.debug(f"Waiting for {PROXY_SPARQL_ENDPOINT} to start...")
                    time.sleep(1)
        


        engine = str(wildcards.engine)
        batch_id = int(wildcards.batch_id)
        engine_dir = CONFIG_EVAL["engines"][engine]["dir"]
        shell(f"python -m fedshop.engines.{engine} generate-config-file {CONFIGFILE} {batch_id}")
        
        if USE_DOCKER:
            shell(f"python -m fedshop.virtuoso virtuoso-kill-all-transactions --container-name={SPARQL_CONTAINER_NAME}")
        else:
            shell(f"python -m fedshop.virtuoso virtuoso-kill-all-transactions --isql={VIRTUOSO_PATH_TO_ISQL}")

        # Early stop if earlier attempts got timed out
        skipBatch = batch_id - 1
        same_file_previous_batch = f"{BENCH_DIR}/{wildcards.engine}/{wildcards.query}/instance_{wildcards.instance_id}/batch_{skipBatch}/attempt_{wildcards.attempt_id}/results.txt"
        skipAttempt = int(wildcards.attempt_id)
        canSkip = batch_id > 0 and os.path.exists(same_file_previous_batch) and os.stat(same_file_previous_batch).st_size == 0
        skipReason = f"Skip evaluation because previous batch at {same_file_previous_batch} timed out or error"

        skipCount = 0
        for attempt in range(CONFIG_EVAL["n_attempts"]):
            same_file_other_attempt = f"{BENCH_DIR}/{wildcards.engine}/{wildcards.query}/instance_{wildcards.instance_id}/batch_{batch_id}/attempt_{attempt}/results.txt"
            LOGGER.info(f"Checking {same_file_other_attempt} ...")
            if  os.path.exists(same_file_other_attempt) and \
                os.path.exists(same_file_other_attempt) and \
                os.stat(same_file_other_attempt).st_size == 0:

                skipBatch = batch_id
                skipAttempt = attempt
                skipReason = f"Skip evaluation because another attempt at {same_file_other_attempt} timed out"
                canSkip = True
                skipCount += 1
                #break

        canSkip = (skipCount == CONFIG_EVAL["n_attempts"] ) 

        skip_stats_file = f"{BENCH_DIR}/{wildcards.engine}/{wildcards.query}/instance_{wildcards.instance_id}/batch_{skipBatch}/attempt_{skipAttempt}/stats.csv"
        previous_reason = str(skip_stats_file | cat() | find_first_pattern([r"(timeout)"]))

        if NO_EXEC:
            shell("python -m fedshop.engines.{engine} run-benchmark {CONFIGFILE} {input.query} --out-result {output.result_txt}  --out-source-selection {output.source_selection} --stats {output.stats} --query-plan {params.query_plan} --batch-id {batch_id} --noexec")

        else:
            if canSkip and previous_reason != "":
                LOGGER.info(skipReason)
                shell("python -m fedshop.engines.{engine} run-benchmark {CONFIGFILE} {input.query} --out-result {output.result_txt}  --out-source-selection {output.source_selection} --stats {output.stats} --query-plan {params.query_plan} --batch-id {batch_id} --noexec")
                create_stats(str(output.stats), previous_reason)
                # shell(f"cp {BENCH_DIR}/{wildcards.engine}/{wildcards.query}/instance_{wildcards.instance_id}/batch_{previous_batch}/attempt_{wildcards.attempt_id}/stats.csv {output.stats}")
                # shell(f"cp {BENCH_DIR}/{wildcards.engine}/{wildcards.query}/instance_{wildcards.instance_id}/batch_{skipBatch}/attempt_{skipAttempt}/query_plan.txt {params.query_plan}")
                # shell(f"cp {BENCH_DIR}/{wildcards.engine}/{wildcards.query}/instance_{wildcards.instance_id}/batch_{skipBatch}/attempt_{skipAttempt}/source_selection.txt {output.source_selection}")
                # shell(f"cp {BENCH_DIR}/{wildcards.engine}/{wildcards.query}/instance_{wildcards.instance_id}/batch_{skipBatch}/attempt_{skipAttempt}/results.txt {output.result_txt}")
            else:
                shell("python -m fedshop.engines.{engine} run-benchmark {CONFIGFILE} {input.query} --out-result {output.result_txt}  --out-source-selection {output.source_selection} --stats {output.stats} --query-plan {params.query_plan} --batch-id {batch_id}")


rule engines_prerequisites:
    output: "{benchDir}/{engine}/{engine}-ok.txt"
    shell: "python -m fedshop.engines.{wildcards.engine} prerequisites {CONFIGFILE} && echo 'OK' > {output}"

