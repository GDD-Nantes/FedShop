import numpy as np
import pandas as pd
import os
from pathlib import Path
import glob
import time
import requests
import subprocess

import sys
smk_directory = os.path.abspath(workflow.basedir)
sys.path.append(os.path.join(Path(smk_directory).parent, "scripts"))

from utils import load_config

WORK_DIR = "bsbm"
CONFIG = load_config(f"{WORK_DIR}/config.yaml")["generation"]

SPARQL_ENDPOINT = CONFIG["sparql"]["endpoint"]
GENERATOR_ENDPOINT = CONFIG["generator"]["endpoint"]

SPARQL_COMPOSE_FILE = CONFIG["sparql"]["compose-file"]
SPARQL_CONTAINER_NAME = CONFIG["sparql"]["container-name"]

GENERATOR_COMPOSE_FILE = CONFIG["generator"]["compose-file"]
GENERATOR_CONTAINER_NAME = CONFIG["generator"]["container-name"]

N_QUERY_INSTANCES = CONFIG["n_query_instances"]
VERBOSE = CONFIG["verbose"]
N_BATCH = CONFIG["n_batch"]

# Config per batch
N_VENDOR=CONFIG["schema"]["vendor"]["params"]["vendor_n"]*CONFIG["schema"]["vendor"]["scale_factor"]
N_REVIEWER=CONFIG["schema"]["person"]["params"]["person_n"]*CONFIG["schema"]["person"]["scale_factor"]

FEDERATION_COUNT=N_VENDOR+N_REVIEWER

QUERY_DIR = f"{WORK_DIR}/queries"
MODEL_DIR = f"{WORK_DIR}/model"
BENCH_DIR = f"{WORK_DIR}/benchmark/generation"
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

def restart_virtuoso(status_file):
    shell(f"docker-compose -f {SPARQL_COMPOSE_FILE} up -d {SPARQL_CONTAINER_NAME}")
    wait_for_container(SPARQL_ENDPOINT, status_file, wait=1)
    return status_file

def start_generator(status_file):
    exec_cmd = CONFIG["generator"]["exec"]
    if "docker exec" in exec_cmd:
        shell(f"docker-compose -f {GENERATOR_COMPOSE_FILE} up -d {GENERATOR_CONTAINER_NAME}")
        wait_for_container(GENERATOR_ENDPOINT, status_file, wait=1)
    elif os.system(f"command -v {exec_cmd}") == 0:
        with open(status_file, "w+") as f:
            f.write(exec_cmd + "\n")
            f.close()

    return status_file

def generate_virtuoso_scripts(nqfiles, shfiles, batch_id, n_items):
    nq_files = [ os.path.basename(f) for f in nqfiles ]
    _, edges = np.histogram(np.arange(n_items), N_BATCH)
    edges = edges[1:].astype(int)
    batch = edges[batch_id]
    with open(f"{shfiles}", "w+") as f:
        f.write(f"echo \"Writing ingest script for {batch_id}, slicing at {batch}-th source...\"\n")
        for nq_file in nq_files[:batch]:
            f.write(f"docker exec {SPARQL_CONTAINER_NAME} /usr/local/virtuoso-opensource/bin/isql-v \"EXEC=ld_dir('/usr/local/virtuoso-opensource/share/virtuoso/vad/', '{nq_file}', 'http://example.com/datasets/default');\"&&\n")
        f.write(f"docker exec {SPARQL_CONTAINER_NAME} /usr/local/virtuoso-opensource/bin/isql-v \"EXEC=rdf_loader_run(log_enable=>2);\" &&\n")
        f.write(f"docker exec {SPARQL_CONTAINER_NAME} /usr/local/virtuoso-opensource/bin/isql-v \"EXEC=checkpoint;\"&&\n")
        f.write("exit 0\n")
        f.close()

#=================
# PIPELINE
#=================

rule all:
    input: 
        expand(
            "{benchDir}/metrics.csv",
            benchDir=BENCH_DIR
        )

rule merge_metrics:
    priority: 1
    input: expand("{{benchDir}}/metrics_batch{batch_id}.csv", batch_id=range(N_BATCH))
    output: "{benchDir}/metrics.csv"
    run: pd.concat((pd.read_csv(f) for f in input)).to_csv(f"{output}", index=False)

rule compute_metrics:
    priority: 2
    threads: 1
    input: 
        expand(
            "{{benchDir}}/{query}/{instance_id}/batch_{{batch_id}}/provenance.csv",
            query=[Path(os.path.join(QUERY_DIR, f)).resolve().stem for f in os.listdir(QUERY_DIR) if "_" not in f],
            instance_id=range(N_QUERY_INSTANCES)
        )
    output: "{benchDir}/metrics_batch{batch_id}.csv"
    params:
        fedcount=FEDERATION_COUNT
    shell: "python scripts/metrics.py compute-metrics {input} {params.fedcount} {output}"

rule exec_provenance_query:
    priority: 3
    threads: 1
    input: 
        provenance_query="{benchDir}/{query}/{instance_id}/provenance.sparql",
        loaded_virtuoso="{benchDir}/virtuoso-batch{batch_id}-ok.txt"
    output: "{benchDir}/{query}/{instance_id}/batch_{batch_id}/provenance.csv"
    params:
        endpoint=SPARQL_ENDPOINT
    shell: 
        'python scripts/query.py execute-query {input.provenance_query} {output} --endpoint {params.endpoint}'

rule ingest_virtuoso_next_batches:
    priority: 4
    threads: 1
    input: 
        vendor=expand("{modelDir}/virtuoso/ingest_vendor_batch{{batch_id}}.sh", modelDir=MODEL_DIR),
        person=expand("{modelDir}/virtuoso/ingest_person_batch{{batch_id}}.sh", modelDir=MODEL_DIR),
        virtuoso_status="{benchDir}/virtuoso-up.txt"
    output: "{benchDir}/virtuoso-batch{batch_id}-ok.txt"
    run: 
        proc = subprocess.run(f"docker exec {SPARQL_CONTAINER_NAME} ls /usr/local/virtuoso-opensource/share/virtuoso/vad | wc -l", shell=True, capture_output=True)
        nFiles = int(proc.stdout.decode())
        expected_nFiles = len(glob.glob(f"{MODEL_DIR}/exported/*.nq"))
        if nFiles != expected_nFiles: raise RuntimeError(f"Expecting {expected_nFiles} *.nq files in virtuoso container, got {nFiles}!") 
        shell(f'sh {input.vendor} bsbm && sh {input.person} && echo "OK" > {output}')

rule restart_virtuoso:
    priority: 5
    threads: 1
    output: "{benchDir}/virtuoso-up.txt"
    run: restart_virtuoso(output)

rule make_virtuoso_ingest_command_for_vendor:
    priority: 5
    threads: 1
    input: expand("{modelDir}/exported/vendor{vendor_id}.nq", vendor_id=range(N_VENDOR), modelDir=MODEL_DIR)
    output: "{modelDir}/virtuoso/ingest_vendor_batch{batch_id}.sh"
    run: generate_virtuoso_scripts(input, output, int(wildcards.batch_id), N_VENDOR)

rule make_virtuoso_ingest_command_for_person:
    priority: 5
    threads: 1
    input: expand("{modelDir}/exported/person{person_id}.nq", person_id=range(N_REVIEWER), modelDir=MODEL_DIR)
    output: "{modelDir}/virtuoso/ingest_person_batch{batch_id}.sh"
    run: generate_virtuoso_scripts(input, output, int(wildcards.batch_id), N_REVIEWER)

rule build_provenance_query: 
    """
    From the data generated for the 10 shops (TODO: and the review datasets?), 
    for each query template created in the previous step, 
    we select 10 random values for the placeholders in the BSBM query templates.
    """
    priority: 6
    input: "{benchDir}/{query}/{instance_id}/injected.sparql",
    output: "{benchDir}/{query}/{instance_id}/provenance.sparql"
    shell: "python scripts/query.py build-provenance-query {input} {output}"

rule instanciate_workload:
    priority: 7
    threads: 1
    input: 
        queryfile=expand("{queryDir}/{{query}}.sparql", queryDir=QUERY_DIR),
        workload_value_selection="{benchDir}/{query}/workload_value_selection.csv"
    output:
        value_selection_query="{benchDir}/{query}/{instance_id}/injected.sparql",
    shell:
        "python scripts/query.py instanciate-workload {input.queryfile} {input.workload_value_selection} {output.value_selection_query} {wildcards.instance_id}"

rule create_workload_value_selection:
    priority: 8
    input: "{benchDir}/{query}/value_selection.csv"
    output: "{benchDir}/{query}/workload_value_selection.csv"
    run:
        pd.read_csv(f"{input}").sample(N_QUERY_INSTANCES).to_csv(f"{output}", index=False)

rule exec_value_selection_query:
    priority: 9
    threads: 1
    input: "{benchDir}/{query}/value_selection.sparql"
    output: "{benchDir}/{query}/value_selection.csv"
    params:
        endpoint=SPARQL_ENDPOINT
    shell: "python scripts/query.py execute-query {input} {output} --endpoint {params.endpoint}"

rule build_value_selection_query:
    priority: 10
    input: 
        queryfile=expand("{queryDir}/{{query}}.sparql", queryDir=QUERY_DIR),
        virtuoso_status="{benchDir}/virtuoso-batch0-ok.txt"
    output: "{benchDir}/{query}/value_selection.sparql"
    shell: "python scripts/query.py build-value-selection-query {input.queryfile} {output}"

rule agg_product_person:
    priority: 11
    retries: 2
    input:
        person="{modelDir}/tmp/person{person_id}.nt.tmp",
        product="{modelDir}/tmp/product/",
    output: "{modelDir}/exported/person{person_id}.nq"
    shell: 'python scripts/aggregator.py {input.person} {input.product} {output} http://www.person{wildcards.person_id}.fr'

rule agg_product_vendor:
    priority: 11
    retries: 2
    input: 
        vendor="{modelDir}/tmp/vendor{vendor_id}.nt.tmp",
        product="{modelDir}/tmp/product/"
    output: "{modelDir}/exported/vendor{vendor_id}.nq",   
    shell: 'python scripts/aggregator.py {input.vendor} {input.product} {output} http://www.vendor{wildcards.vendor_id}.fr'

rule split_products:
    priority: 12
    threads: 1
    retries: 2
    input: "{modelDir}/tmp/product0.nt.tmp"
    output: directory("{modelDir}/tmp/product/")
    shell: 'python scripts/splitter.py {input} {output}'

rule generate_reviewers:
    priority: 13
    input: expand("{benchDir}/generator-ok.txt", benchDir=BENCH_DIR)
    output: "{modelDir}/tmp/person{person_id}.nt.tmp"
    shell: 'python scripts/generate.py generate {WORK_DIR}/config.yaml person {output} --id {wildcards.person_id}'

rule generate_vendors:
    priority: 13
    input: expand("{benchDir}/generator-ok.txt", benchDir=BENCH_DIR)
    output: "{modelDir}/tmp/vendor{vendor_id}.nt.tmp"
    shell: 'python scripts/generate.py generate {WORK_DIR}/config.yaml vendor {output} --id {wildcards.vendor_id}'

rule generate_products:
    priority: 14
    input: expand("{benchDir}/generator-ok.txt", benchDir=BENCH_DIR)
    output: "{modelDir}/tmp/product0.nt.tmp", 
    shell: 'python scripts/generate.py generate {WORK_DIR}/config.yaml product {output}'

rule start_generator_container:
    output: "{benchDir}/generator-ok.txt"
    run: start_generator(f"{output}")