import numpy as np
import pandas as pd
import os
from pathlib import Path
import glob
import time
import requests
import subprocess
import json

import sys
smk_directory = os.path.abspath(workflow.basedir)
sys.path.append(os.path.join(Path(smk_directory).parent.parent, "rsfb"))

from utils import load_config, get_docker_endpoint_by_container_name, check_container_status

#===============================
# GENERATION PHASE:
# - Generate data
# - Ingest the data in virtuoso
#===============================

CONFIGFILE = config["configfile"]

WORK_DIR = "experiments/bsbm"
CONFIG = load_config(CONFIGFILE)["generation"]

SPARQL_COMPOSE_FILE = CONFIG["virtuoso"]["compose_file"]
SPARQL_SERVICE_NAME = CONFIG["virtuoso"]["service_name"]

CONTAINER_PATH_TO_ISQL = "/opt/virtuoso-opensource/bin/isql"
CONTAINER_PATH_TO_DATA = "/usr/share/proj/" 

# CONTAINER_PATH_TO_ISQL = "/usr/local/virtuoso-opensource/bin/isql-v" 
# CONTAINER_PATH_TO_DATA = "/usr/local/virtuoso-opensource/share/virtuoso/vad"

N_QUERY_INSTANCES = CONFIG["n_query_instances"]
VERBOSE = CONFIG["verbose"]
N_BATCH = CONFIG["n_batch"]

# Config per batch
N_VENDOR=CONFIG["schema"]["vendor"]["params"]["vendor_n"]
N_RATINGSITE=CONFIG["schema"]["ratingsite"]["params"]["ratingsite_n"]

FEDERATION_COUNT=N_VENDOR+N_RATINGSITE

QUERY_DIR = f"{WORK_DIR}/queries"
MODEL_DIR = f"{WORK_DIR}/model"
BENCH_DIR = f"{WORK_DIR}/benchmark/generation"
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

def deploy_virtuoso(container_infos_file, restart=False):
    if restart:
        shell(f"docker-compose -f {SPARQL_COMPOSE_FILE} down --remove-orphans")
        shell("docker volume prune --force")
        time.sleep(2)
    # shell(f"docker-compose -f {SPARQL_COMPOSE_FILE} up -d --scale {SPARQL_SERVICE_NAME}={N_BATCH}")
    # wait_for_container(CONFIG["virtuoso"]["endpoints"], f"{BENCH_DIR}/virtuoso-up.txt", wait=1)
    
    shell(f"docker-compose -f {SPARQL_COMPOSE_FILE} create --no-recreate --scale {SPARQL_SERVICE_NAME}={N_BATCH} {SPARQL_SERVICE_NAME}") # For docker-compose version > 2.15.1
    SPARQL_CONTAINER_NAMES = CONFIG["virtuoso"]["container_names"]
    pd.DataFrame(SPARQL_CONTAINER_NAMES, columns=["Name"]).to_csv(str(container_infos_file), index=False)

    shell(f"docker-compose -f {SPARQL_COMPOSE_FILE} stop {SPARQL_SERVICE_NAME}")
    return container_infos_file

def start_generator(status_file):
    exec_cmd = CONFIG["generator"]["exec"]
    if os.system(f"command -v {exec_cmd}") == 0:
        with open(status_file, "w") as f:
            f.write(exec_cmd + "\n")
    else: raise RuntimeError(f"{exec_cmd} doesn't exist...")

    return status_file

def generate_virtuoso_scripts(container_infos_file, prefix, shfiles, batch_id, n_items):
    batch_id = int(batch_id)
    container_infos_file = str(container_infos_file)
    container_name = pd.read_csv(container_infos_file).loc[batch_id, "Name"]

    _, edges = np.histogram(np.arange(n_items), N_BATCH)
    edges = edges[1:].astype(int) + 1
    batch = edges[batch_id]

    with open(f"{shfiles}", "w") as f:
        f.write(f"echo \"Writing ingest script for batch {batch_id}, slicing at {batch}-th source...\"\n")
        # /usr/local/virtuoso-opensource/bin/isql-v
        f.write(f'docker exec {container_name} {CONTAINER_PATH_TO_ISQL} "EXEC=grant select on \\\"DB.DBA.SPARQL_SINV_2\\\" to \\\"SPARQL\\\";"\n')
        f.write(f'docker exec {container_name} {CONTAINER_PATH_TO_ISQL} "EXEC=grant execute on \\\"DB.DBA.SPARQL_SINV_IMP\\\" to \\\"SPARQL\\\";"\n')
        
        f.write(f"for id in $(seq 0 {batch-1}); do\n")
        f.write(f"  docker exec {container_name} {CONTAINER_PATH_TO_ISQL} \"EXEC=ld_dir('{CONTAINER_PATH_TO_DATA}', '{prefix}$id.nq', 'http://example.com/datasets/default');\" >> /dev/null\n")
        f.write(f"  echo $id\n")
        f.write(f"done | tqdm --total {batch} --unit files >> /dev/null\n")

        f.write(f"docker exec {container_name} {CONTAINER_PATH_TO_ISQL} \"EXEC=rdf_loader_run(log_enable=>2);\" &&\n")
        f.write(f"docker exec {container_name} {CONTAINER_PATH_TO_ISQL} \"EXEC=checkpoint;\"&&\n")
        f.write("exit 0\n")

def check_file_presence(container_infos_file, batch_id):
    batch_id = int(batch_id)
    container_infos_file = str(container_infos_file)
    container_name = pd.read_csv(container_infos_file).loc[batch_id, "Name"]

    proc = subprocess.run(f'docker exec {container_name} sh -c "ls {CONTAINER_PATH_TO_DATA}*.nq | wc -l"', shell=True, capture_output=True)
    nFiles = int(proc.stdout.decode())
    expected_files = glob.glob(f"{MODEL_DIR}/dataset/*.nq")
    expected_nFiles = len(expected_files)
    while nFiles != expected_nFiles: 
        print(f"Expecting {expected_nFiles} *.nq files in virtuoso container, got {nFiles}!") 
        activate_one_container(container_infos_file, batch_id)
       
        proc = subprocess.run(f'docker exec {container_name} sh -c "ls {CONTAINER_PATH_TO_DATA}/*.nq | wc -l"', shell=True, capture_output=True)
        nFiles = int(proc.stdout.decode())

def check_file_stats(container_infos_file, batch_id):
    batch_id = int(batch_id)
    container_infos_file = str(container_infos_file)
    container_name = pd.read_csv(container_infos_file).loc[batch_id, "Name"]

    cmd = 'stat -c "%Y" '
    proc = subprocess.run(f'docker exec {container_name} sh -c "ls {CONTAINER_PATH_TO_DATA}/*.nq | sort"', shell=True, capture_output=True)
    container_files = str(proc.stdout.decode()).split("\n")
    local_files = sorted(glob.glob(f"{MODEL_DIR}/dataset/*.nq"))

    for local_file, container_file in zip(local_files, container_files):
        
        if Path(local_file).stem != Path(container_file).stem:
            raise RuntimeError(f"Mismatch between local file {Path(local_file).stem} and container file {Path(container_file).stem}")

        ctn_mod_time = int(subprocess.run(f'docker exec {container_name} {cmd} {container_file}', shell=True, capture_output=True).stdout.decode())
        local_mod_time = int(subprocess.run(f"{cmd} {local_file}", shell=True, capture_output=True).stdout.decode())

        if ctn_mod_time < local_mod_time:
            raise RuntimeError(f"Container file {container_file} is older than local file {local_file}")
            # shell(f"docker exec {container_name} rm {CONTAINER_PATH_TO_DATA}/{container_file}")
            # shell(f'docker cp {local_file} {container_name}:{CONTAINER_PATH_TO_DATA}')

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

#=================
# PIPELINE
#=================

rule all:
    input: 
        expand(
            "{benchDir}/virtuoso_batch{batch_id}-ok.txt",
            benchDir=BENCH_DIR,
            batch_id=range(N_BATCH)
        )

rule ingest_virtuoso:
    priority: 4
    threads: 1
    input: 
        vendor=expand("{modelDir}/virtuoso/ingest_vendor_batch{{batch_id}}.sh", modelDir=MODEL_DIR),
        ratingsite=expand("{modelDir}/virtuoso/ingest_ratingsite_batch{{batch_id}}.sh", modelDir=MODEL_DIR),
    output: "{benchDir}/virtuoso_batch{batch_id}-ok.txt"
    run: 
        container_infos = f"{BENCH_DIR}/container_infos.csv"
        activate_one_container(container_infos, wildcards.batch_id)
        check_file_presence(container_infos, wildcards.batch_id)
        check_file_stats(container_infos, wildcards.batch_id)
        shell(f'sh {input.vendor} bsbm && sh {input.ratingsite}')
        
        # Mini tests for sources number per batch
        shell(f"RSFB__CONFIGFILE={CONFIGFILE} RSFB__BATCHID={wildcards.batch_id} python -W ignore:UserWarning {WORK_DIR}/tests/test.py -v TestGenerationVendor.test_vendor_nb_sources")
        shell(f"RSFB__CONFIGFILE={CONFIGFILE} RSFB__BATCHID={wildcards.batch_id} python -W ignore:UserWarning {WORK_DIR}/tests/test.py -v TestGenerationRatingSite.test_ratingsite_nb_sources")
        shell(f'echo "OK" > {output}')

        # test_proc = subprocess.run(
        #     f"RSFB__CONFIGFILE={CONFIGFILE} RSFB__BATCHID={wildcards.batch_id} python -W ignore:UserWarning {WORK_DIR}/tests/test.py -v", 
        #     capture_output=True, shell=True
        # )

        # if test_proc.returncode == 0:
        #     with open(str(output), "w") as f:
        #         f.write(test_proc.stdout.decode())
        #         f.close()
        # else: 
        #     raise RuntimeError("The ingested data did not pass the tests. Check the output file for more information.")


rule deploy_virtuoso:
    priority: 5
    threads: 1
    input:
        vendor=expand("{modelDir}/dataset/vendor{vendor_id}.nq", vendor_id=range(N_VENDOR), modelDir=MODEL_DIR),
        ratingsite=expand("{modelDir}/dataset/ratingsite{ratingsite_id}.nq", ratingsite_id=range(N_RATINGSITE), modelDir=MODEL_DIR),
    output: "{benchDir}/container_infos.csv"
    run: deploy_virtuoso(output)

rule make_virtuoso_ingest_command_for_vendor:
    priority: 5
    threads: 1
    input: 
        container_infos = expand("{benchDir}/container_infos.csv", benchDir=BENCH_DIR)
    output: "{modelDir}/virtuoso/ingest_vendor_batch{batch_id}.sh"
    run: generate_virtuoso_scripts(input.container_infos, "vendor", output, wildcards.batch_id, N_VENDOR)

rule make_virtuoso_ingest_command_for_ratingsite:
    priority: 5
    threads: 1
    input: 
        container_infos = expand("{benchDir}/container_infos.csv", benchDir=BENCH_DIR)
    output: "{modelDir}/virtuoso/ingest_ratingsite_batch{batch_id}.sh"
    run: generate_virtuoso_scripts(input.container_infos, "ratingsite", output, wildcards.batch_id, N_RATINGSITE)

rule generate_ratingsites:
    priority: 12
    threads: 5
    input: 
        status=expand("{workDir}/generator-ok.txt", workDir=WORK_DIR),
        product=ancient(CONFIG["schema"]["product"]["export_output_dir"])
    output: "{modelDir}/dataset/ratingsite{ratingsite_id}.nq"
    shell: "python rsfb/generate.py generate {CONFIGFILE} ratingsite {output} --id {wildcards.ratingsite_id}"

rule generate_vendors:
    priority: 13
    threads: 5
    input: 
        status=expand("{workDir}/generator-ok.txt", workDir=WORK_DIR),
        product=ancient(CONFIG["schema"]["product"]["export_output_dir"])
    output: "{modelDir}/dataset/vendor{vendor_id}.nq"
    shell: "python rsfb/generate.py generate {CONFIGFILE} vendor {output} --id {wildcards.vendor_id}"

rule generate_products:
    priority: 14
    threads: 1
    input: expand("{workDir}/generator-ok.txt", workDir=WORK_DIR)
    output: directory(CONFIG["schema"]["product"]["export_output_dir"]), 
    shell: 'python rsfb/generate.py generate {CONFIGFILE} product {output}'

rule start_generator_container:
    output: "{workDir}/generator-ok.txt"
    run: start_generator(f"{output}")