import pandas as pd
import os
from pathlib import Path
import glob
import time
from tcp_latency import measure_latency

VIRTUOSO_HOME = ""
ENDPOINT = "http://localhost:8890/sparql"

DOCKER_CONTAINER_NAME="bsbm-virtuoso"

WORK_DIR = "bsbm"
QUERY_DIR = f"{WORK_DIR}/queries"
MODEL_DIR = f"{WORK_DIR}/model"
BENCH_DIR = f"{WORK_DIR}/benchmark"
FEDERAPP_DIR = "Federapp/target"
N_VARIATIONS = 10
VERBOSE = False

# How many batch
N_BATCH=3

# Config per batch
N_VENDOR=3
N_REVIEWER=3
FEDERATION_COUNT=N_VENDOR+N_REVIEWER
SCALE_FACTOR=1

#=================
# USEFUL FUNCTIONS
#=================

def wait_for_virtuoso_container(outfile, wait=1):
    while(len(measure_latency(host="localhost", port=8890)) == 0):
        print("Waiting for Virtuoso...")
        time.sleep(wait)
    with open(f"{outfile}", "w+") as f:
        f.write("OK")
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
    input: 
        metrics_files = lambda wildcards: glob.glob(f"{wildcards.benchDir}/metrics_batch*.csv"),
        final_metric = expand("{{benchDir}}/metrics_batch{n_batch}.csv", n_batch=N_BATCH-1)
    output: "{benchDir}/metrics.csv"
    run: pd.concat((pd.read_csv(f) for f in input.metrics_files))

def get_batch_input_recursively(batch_id):
    """Use recursive function to produce result for one batch at a time
    Source: https://stackoverflow.com/questions/56274065/snakemake-using-a-rule-in-a-loop
    """
    next_batch = batch_id
    if batch_id > 1:
        next_batch = batch_id - 1
    if batch_id < 1:
        raise RuntimeError(f"batch_id must be greater than 1, received {batch_id}")
    return next_batch

rule compute_metrics:
    input: 
        expand(
            "{{benchDir}}/{query}/{instance_id}/batch_{batch_id}/provenance.csv",
            query=[Path(os.path.join(QUERY_DIR, f)).resolve().stem for f in os.listdir(QUERY_DIR) if "_" not in f],
            instance_id=range(N_VARIATIONS),
            batch_id=get_batch_input_recursively(N_BATCH-1)
        )
    output: "{benchDir}/metrics_batch{batch_id}.csv"
    params:
        fedcount=FEDERATION_COUNT
    shell: "python scripts/metrics.py compute-metrics {input} {params.fedcount} {output}"

rule exec_provenance_query:
    input: 
        provenance_query="{benchDir}/{query}/{instance_id}/provenance.sparql",
        loaded_virtuoso=expand("{workDir}/virtuoso-batch{{batch_id}}-ok.txt", workDir=WORK_DIR)
    output: "{benchDir}/{query}/{instance_id}/batch_{batch_id}/provenance.csv"
    params:
        endpoint=ENDPOINT
    shell: 
        'python scripts/query.py execute-query {input} {output} --endpoint {params.endpoint}'

def restart_virtuoso(wildcards):
    status_file = f"{WORK_DIR}/virtuoso-up.txt"
    if len(measure_latency(host="localhost", port=8890)) == 0:
        os.system(f"docker-compose down && docker-compose up -d {DOCKER_CONTAINER_NAME}")
        wait_for_virtuoso_container(status_file, wait=1)
    return status_file

rule ingest_virtuoso_next_batches:
    input: 
        vendor=expand("{modelDir}/workflow/ingest_vendor_batch{{batch_id}}.sh", modelDir=MODEL_DIR),
        person=expand("{modelDir}/workflow/ingest_person_batch{{batch_id}}.sh", modelDir=MODEL_DIR),
        virtuoso_status=restart_virtuoso,
        provenance_query=expand(
            "{benchDir}/{query}/{instance_id}/provenance.sparql",
            benchDir=BENCH_DIR,
            query=[Path(os.path.join(QUERY_DIR, f)).resolve().stem for f in os.listdir(QUERY_DIR) if "_" not in f],
            instance_id=range(N_VARIATIONS),
        )
    output: expand("{workDir}/virtuoso-batch{{batch_id}}-ok.txt", workDir=WORK_DIR)
    shell: 'sh {input.vendor} bsbm && sh {input.person} && echo "OK" > {output}'

rule build_provenance_query: 
    """
    From the data generated for the 10 shops (TODO: and the review datasets?), 
    for each query template created in the previous step, 
    we select 10 random values for the placeholders in the BSBM query templates.
    """
    input: "{benchDir}/{query}/{instance_id}/injected.sparql",
    output: "{benchDir}/{query}/{instance_id}/provenance.sparql"
    shell: "python scripts/query.py build-provenance-query {input} {output}"

rule instanciate_workload:
    input: 
        queryfile=expand("{queryDir}/{{query}}.sparql", queryDir=QUERY_DIR),
        workload_value_selection="{benchDir}/{query}/workload_value_selection.csv"
    output:
        value_selection_query="{benchDir}/{query}/{instance_id}/injected.sparql",
    shell:
        "python scripts/query.py instanciate-workload {input.queryfile} {input.workload_value_selection} {output.value_selection_query} {wildcards.instance_id}"

rule create_workload_value_selection:
    input: "{benchDir}/{query}/value_selection.csv"
    output: "{benchDir}/{query}/workload_value_selection.csv"
    params:
        n_variations=N_VARIATIONS
    run:
        pd.read_csv(f"{input}").sample(params.n_variations).to_csv(f"{output}", index=False)


rule exec_value_selection_query:
    input: "{benchDir}/{query}/value_selection.sparql"
    output: "{benchDir}/{query}/value_selection.csv"
    params:
        endpoint=ENDPOINT
    shell: "python scripts/query.py execute-query {input} {output} --endpoint {params.endpoint}"

rule build_value_selection_query:
    input: 
        queryfile=expand("{queryDir}/{{query}}.sparql", queryDir=QUERY_DIR),
        virtuoso_status=expand("{workDir}/virtuoso-batch0-ok.txt", workDir=WORK_DIR)
    output: "{benchDir}/{query}/value_selection.sparql"
    params:
        n_variations=N_VARIATIONS
    shell: "python scripts/query.py build-value-selection-query {input.queryfile} {output}"

rule ingest_virtuoso_first_batch:
    input: 
        vendor=expand("{modelDir}/workflow/ingest_vendor_batch0.sh", modelDir=MODEL_DIR),
        person=expand("{modelDir}/workflow/ingest_person_batch0.sh", modelDir=MODEL_DIR),
        virtuoso_status=expand("{workDir}/virtuoso-up.txt", workDir=WORK_DIR)
    output: expand("{workDir}/virtuoso-batch0-ok.txt", workDir=WORK_DIR)
    shell: 'sh {input.vendor} bsbm && sh {input.person} && echo "OK" > {output}'

rule start_virtuoso:
    output: "{workDir}/virtuoso-up.txt"
    run:
        os.system(f"docker-compose up -d {DOCKER_CONTAINER_NAME}") 
        wait_for_virtuoso_container(f"{output}")
        
def generate_virtuoso_scripts(nqfiles, shfiles, n_items):
    nq_files = [ os.path.basename(f) for f in nqfiles ]
    for batchId, fileId in enumerate(range(N_BATCH, N_BATCH*(n_items+1), N_BATCH)):
        with open(f"{shfiles[batchId]}", "w+") as f:
            f.write(f"echo \"Writing ingest script for {batchId}, slicing at {fileId}-th source...\"\n")
            for nq_file in nq_files[:fileId]:
                f.write(f"docker exec {DOCKER_CONTAINER_NAME} /usr/local/virtuoso-opensource/bin/isql-v \"EXEC=ld_dir('/usr/local/virtuoso-opensource/share/virtuoso/vad/', '{nq_file}', 'http://example.com/datasets/default');\"&&\n")
            f.write(f"docker exec {DOCKER_CONTAINER_NAME} /usr/local/virtuoso-opensource/bin/isql-v \"EXEC=rdf_loader_run(log_enable=>2);\" &&\n")
            f.write(f"docker exec {DOCKER_CONTAINER_NAME} /usr/local/virtuoso-opensource/bin/isql-v \"EXEC=checkpoint;\"&&\n")
            f.write("exit 0\n")
            f.close()

rule make_virtuoso_ingest_command_for_vendor:
    input: expand("{modelDir}/exported/vendor{vendor_id}.nq", vendor_id=range(N_VENDOR*N_BATCH), modelDir=MODEL_DIR)
    output: expand("{{modelDir}}/workflow/ingest_vendor_batch{batchId}.sh", batchId=range(N_BATCH))
    params:
        n_items=N_VENDOR
    run: generate_virtuoso_scripts(input, output, params.n_items)

rule make_virtuoso_ingest_command_for_person:
    input: expand("{modelDir}/exported/person{person_id}.nq", person_id=range(N_REVIEWER*N_BATCH), modelDir=MODEL_DIR)
    output: expand("{{modelDir}}/workflow/ingest_person_batch{batchId}.sh", batchId=range(N_BATCH))
    params:
        n_items=N_REVIEWER
    run: generate_virtuoso_scripts(input, output, params.n_items)

rule agg_product_person:
    input:
        person="{modelDir}/tmp/person{person_id}.nt.tmp",
        product="{modelDir}/tmp/product/"
    output: "{modelDir}/exported/person{person_id}.nq"
    shell: 'python scripts/aggregator.py {input.person} {input.product} {output} http://www.person{wildcards.person_id}.fr'

rule agg_product_vendor:
    input: 
        vendor="{modelDir}/tmp/vendor{vendor_id}.nt.tmp",
        product="{modelDir}/tmp/product/"
    output: "{modelDir}/exported/vendor{vendor_id}.nq",   
    shell: 'python scripts/aggregator.py {input.vendor} {input.product} {output} http://www.vendor{wildcards.vendor_id}.fr'

rule split_products:
    input: "{modelDir}/tmp/product0.nt.tmp"
    output: directory("{modelDir}/tmp/product/")
    shell: 'python scripts/splitter.py {input} {output}'

rule generate_reviewers:
    output: "{modelDir}/tmp/person{person_id}.nt.tmp"
    params:
        verbose=VERBOSE
    shell: 'python scripts/generate.py generate {WORK_DIR}/config.yaml person {wildcards.modelDir}/bsbm-person.template {output} {wildcards.person_id} --verbose {params.verbose}'

rule generate_products:
    output: "{modelDir}/tmp/product0.nt.tmp", 
    params:
        verbose=VERBOSE,
        product_id=0
    shell: 'python scripts/generate.py generate {WORK_DIR}/config.yaml product {wildcards.modelDir}/bsbm-product.template {output} {params.product_id} --verbose {params.verbose}'

rule generate_vendors:
    output: "{modelDir}/tmp/vendor{vendor_id}.nt.tmp"
    params:
        verbose=VERBOSE
    shell: 'python scripts/generate.py generate {WORK_DIR}/config.yaml vendor {wildcards.modelDir}/bsbm-vendor.template {output} {wildcards.vendor_id} --verbose {params.verbose}'