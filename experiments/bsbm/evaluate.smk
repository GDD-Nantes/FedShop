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
LOGGER = rsfb_logger(Path(__file__).name)

#=================
# USEFUL FUNCTIONS
#=================

def activate_one_container(batch_id):
    """ Activate one container while stopping all others
    """

    is_virtuoso_restarted = False
    VIRTUOSO_MANUAL_ENDPOINT = CONFIG_GEN["virtuoso"]["manual_port"]
    if VIRTUOSO_MANUAL_ENDPOINT != -1:
        if ping(f"http://localhost:{VIRTUOSO_MANUAL_ENDPOINT}/sparql") != 200:
            raise RuntimeError(f"Virtuoso endpoint {VIRTUOSO_MANUAL_ENDPOINT} is not available!")

    else:
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
        stats="{benchDir}/{engine}/{query}/instance_{instance_id}/batch_{batch_id}/attempt_{attempt_id}/stats.csv",
        source_selection="{benchDir}/{engine}/{query}/instance_{instance_id}/batch_{batch_id}/attempt_{attempt_id}/source_selection.txt",
        result_txt="{benchDir}/{engine}/{query}/instance_{instance_id}/batch_{batch_id}/attempt_{attempt_id}/results.txt",
    params:
        query_plan="{benchDir}/{engine}/{query}/instance_{instance_id}/batch_{batch_id}/attempt_{attempt_id}/query_plan.txt",
        result_csv="{benchDir}/{engine}/{query}/instance_{instance_id}/batch_{batch_id}/attempt_{attempt_id}/results.csv",
        engine_config="{benchDir}/{engine}/config/batch_{batch_id}/{engine}.conf",
        last_batch=LAST_BATCH
    run: 
        activate_one_container(LAST_BATCH)

        engine = str(wildcards.engine)
        batch_id = int(wildcards.batch_id)
        engine_config = f"{WORK_DIR}/benchmark/evaluation/{engine}/config/batch_{batch_id}/{engine}.conf"
        generate_federation_declaration(engine_config, engine, batch_id)
        virtuoso_kill_all_transactions(SPARQL_COMPOSE_FILE, SPARQL_SERVICE_NAME, LAST_BATCH)

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
            shell("python rsfb/engines/{engine}.py run-benchmark {CONFIGFILE} {params.engine_config} {input.query} --out-result {output.result_txt}  --out-source-selection {output.source_selection} --stats {output.stats} --force-source-selection {input.engine_source_selection} --query-plan {params.query_plan} --batch-id {batch_id} --noexec")

        else:
            if canSkip and previous_reason != "":
                LOGGER.info(skipReason)
                shell("python rsfb/engines/{engine}.py run-benchmark {CONFIGFILE} {params.engine_config} {input.query} --out-result {output.result_txt}  --out-source-selection {output.source_selection} --stats {output.stats} --force-source-selection {input.engine_source_selection} --query-plan {params.query_plan} --batch-id {batch_id} --noexec")
                create_stats(str(output.stats), previous_reason)
                # shell(f"cp {BENCH_DIR}/{wildcards.engine}/{wildcards.query}/instance_{wildcards.instance_id}/batch_{previous_batch}/attempt_{wildcards.attempt_id}/stats.csv {output.stats}")
                # shell(f"cp {BENCH_DIR}/{wildcards.engine}/{wildcards.query}/instance_{wildcards.instance_id}/batch_{skipBatch}/attempt_{skipAttempt}/query_plan.txt {params.query_plan}")
                # shell(f"cp {BENCH_DIR}/{wildcards.engine}/{wildcards.query}/instance_{wildcards.instance_id}/batch_{skipBatch}/attempt_{skipAttempt}/source_selection.txt {output.source_selection}")
                # shell(f"cp {BENCH_DIR}/{wildcards.engine}/{wildcards.query}/instance_{wildcards.instance_id}/batch_{skipBatch}/attempt_{skipAttempt}/results.txt {output.result_txt}")
            else:
                shell("python rsfb/engines/{engine}.py run-benchmark {CONFIGFILE} {params.engine_config} {input.query} --out-result {output.result_txt}  --out-source-selection {output.source_selection} --stats {output.stats} --force-source-selection {input.engine_source_selection} --query-plan {params.query_plan} --batch-id {batch_id}")


rule engines_prerequisites:
    output: "{benchDir}/{engine}/{engine}-ok.txt"
    shell: "python rsfb/engines/{wildcards.engine}.py prerequisites {CONFIGFILE} && echo 'OK' > {output}"

