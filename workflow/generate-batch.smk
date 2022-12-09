import pandas as pd
import os
from pathlib import Path
import glob
import time
import requests
import subprocess

SPARQL_ENDPOINT = os.environ["RSFB__SPARQL_ENDPOINT"]
GENERATOR_ENDPOINT = os.environ["RSFB__GENERATOR_ENDPOINT"]

SPARQL_COMPOSE_FILE = os.environ["RSFB__SPARQL_COMPOSE_FILE"]
SPARQL_CONTAINER_NAME = os.environ["RSFB__SPARQL_CONTAINER_NAME"]

GENERATOR_COMPOSE_FILE = os.environ["RSFB__GENERATOR_COMPOSE_FILE"]
GENERATOR_CONTAINER_NAME = os.environ["RSFB__GENERATOR_CONTAINER_NAME"]

WORK_DIR = os.environ["RSFB__WORK_DIR"]
N_VARIATIONS = int(os.environ["RSFB__N_VARIATIONS"])
VERBOSE = bool(os.environ["RSFB__VERBOSE"])
N_BATCH=int(os.environ["RSFB__N_BATCH"])

# Config per batch
N_VENDOR=int(os.environ["RSFB__N_VENDOR"])
N_REVIEWER=int(os.environ["RSFB__N_REVIEWER"])

TOTAL_VENDOR = N_VENDOR * N_BATCH
TOTAL_REVIEWER = N_REVIEWER * N_BATCH 

FEDERATION_COUNT=TOTAL_VENDOR+TOTAL_REVIEWER
SCALE_FACTOR=int(os.environ["RSFB__SCALE_FACTOR"])

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
    shell(f"docker-compose -f {GENERATOR_COMPOSE_FILE} up -d {GENERATOR_CONTAINER_NAME}")
    wait_for_container(GENERATOR_ENDPOINT, status_file, wait=1)
    return status_file

def generate_virtuoso_scripts(nqfiles, shfiles, batch_id, n_items):
    nq_files = [ os.path.basename(f) for f in nqfiles ]
    fileIds = list(range(N_BATCH, N_BATCH*(n_items+1), N_BATCH))
    fileId = fileIds[batch_id]
    with open(f"{shfiles}", "w+") as f:
        f.write(f"echo \"Writing ingest script for {batch_id}, slicing at {fileId}-th source...\"\n")
        for nq_file in nq_files[:fileId]:
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
            instance_id=range(N_VARIATIONS)
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
    input: expand("{modelDir}/exported/vendor{vendor_id}.nq", vendor_id=range(TOTAL_VENDOR), modelDir=MODEL_DIR)
    output: "{modelDir}/virtuoso/ingest_vendor_batch{batch_id}.sh"
    params:
        n_items=TOTAL_VENDOR
    run: generate_virtuoso_scripts(input, output, int(wildcards.batch_id), params.n_items)

rule make_virtuoso_ingest_command_for_person:
    priority: 5
    threads: 1
    input: expand("{modelDir}/exported/person{person_id}.nq", person_id=range(TOTAL_REVIEWER), modelDir=MODEL_DIR)
    output: "{modelDir}/virtuoso/ingest_person_batch{batch_id}.sh"
    params:
        n_items=TOTAL_REVIEWER
    run: generate_virtuoso_scripts(input, output, int(wildcards.batch_id), params.n_items)

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
    params:
        n_variations=N_VARIATIONS
    run:
        pd.read_csv(f"{input}").sample(params.n_variations).to_csv(f"{output}", index=False)

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
    params:
        n_variations=N_VARIATIONS
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
    input: "{modelDir}/tmp/product0.nt.tmp"
    output: directory("{modelDir}/tmp/product/")
    shell: 'python scripts/splitter.py {input} {output}'

rule generate_reviewers:
    priority: 13
    input: expand("{benchDir}/generator-ok.txt", benchDir=BENCH_DIR)
    output: "{modelDir}/tmp/person{person_id}.nt.tmp"
    params:
        verbose=VERBOSE
    shell: 'python scripts/generate.py generate {WORK_DIR}/config.yaml person {output} --id {wildcards.person_id} --verbose {params.verbose}'

rule generate_vendors:
    priority: 13
    input: expand("{benchDir}/generator-ok.txt", benchDir=BENCH_DIR)
    output: "{modelDir}/tmp/vendor{vendor_id}.nt.tmp"
    params:
        verbose=VERBOSE
    shell: 'python scripts/generate.py generate {WORK_DIR}/config.yaml vendor {output} --id {wildcards.vendor_id} --verbose {params.verbose}'

rule generate_products:
    priority: 14
    input: expand("{benchDir}/generator-ok.txt", benchDir=BENCH_DIR)
    output: "{modelDir}/tmp/product0.nt.tmp", 
    params:
        verbose=VERBOSE
    shell: 'python scripts/generate.py generate {WORK_DIR}/config.yaml product {output} --verbose {params.verbose}'

rule start_generator_container:
    output: "{benchDir}/generator-ok.txt"
    run: start_generator(f"{output}")