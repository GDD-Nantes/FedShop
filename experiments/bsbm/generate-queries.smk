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
sys.path.append(os.path.join(Path(smk_directory).parent.parent, "fedshop"))

from utils import ping, fedshop_logger, load_config, get_docker_endpoint_by_container_name, check_container_status

#===============================
# GENERATION PHASE:
# - Generate data
# - Ingest the data in virtuoso
# - Generate query instances
# - Generate expected results
# - Generate expected source selection
# - Generate expected metrics
#===============================

CONFIGFILE = config["configfile"]

WORK_DIR = "experiments/bsbm"
CONFIG = load_config(CONFIGFILE)
CONFIG_GEN = CONFIG["generation"]
CONFIG_EVAL = CONFIG["evaluation"]


SPARQL_COMPOSE_FILE = CONFIG_GEN["virtuoso"]["compose_file"]
SPARQL_SERVICE_NAME = CONFIG_GEN["virtuoso"]["service_name"]

PROXY_COMPOSE_FILE =  CONFIG_EVAL["proxy"]["compose_file"]
PROXY_SERVICE_NAME = CONFIG_EVAL["proxy"]["service_name"]
PROXY_CONTAINER_NAMES = CONFIG_EVAL["proxy"]["container_name"]
PROXY_SERVER = CONFIG_EVAL["proxy"]["endpoint"]
PROXY_PORT = re.search(r":(\d+)", PROXY_SERVER).group(1)
PROXY_SPARQL_ENDPOINT = PROXY_SERVER + "sparql"

CONTAINER_PATH_TO_ISQL = "/opt/virtuoso-opensource/bin/isql"
CONTAINER_PATH_TO_DATA = "/usr/share/proj/" 

# CONTAINER_PATH_TO_ISQL = "/usr/local/virtuoso-opensource/bin/isql-v" 
# CONTAINER_PATH_TO_DATA = "/usr/local/virtuoso-opensource/share/virtuoso/vad"

N_QUERY_INSTANCES = CONFIG_GEN["n_query_instances"]
VERBOSE = CONFIG_GEN["verbose"]
N_BATCH = CONFIG_GEN["n_batch"]

# Config per batch
N_VENDOR=CONFIG_GEN["schema"]["vendor"]["params"]["vendor_n"]
N_RATINGSITE=CONFIG_GEN["schema"]["ratingsite"]["params"]["ratingsite_n"]

FEDERATION_COUNT=N_VENDOR+N_RATINGSITE

QUERY_DIR = f"{WORK_DIR}/queries"
MODEL_DIR = f"{WORK_DIR}/model"
BENCH_DIR = f"{WORK_DIR}/benchmark/generation"
TEMPLATE_DIR = f"{MODEL_DIR}/watdiv"

LOGGER = fedshop_logger(Path(__file__).name)

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
    # wait_for_container(CONFIG_GEN["virtuoso"]["endpoints"], f"{BENCH_DIR}/virtuoso-up.txt", wait=1)
    
    shell(f"docker-compose -f {SPARQL_COMPOSE_FILE} create --no-recreate --scale {SPARQL_SERVICE_NAME}={N_BATCH} {SPARQL_SERVICE_NAME}") # For docker-compose version > 2.15.1
    SPARQL_CONTAINER_NAMES = CONFIG_GEN["virtuoso"]["container_names"]
    pd.DataFrame(SPARQL_CONTAINER_NAMES, columns=["Name"]).to_csv(str(container_infos_file), index=False)

    shell(f"docker-compose -f {SPARQL_COMPOSE_FILE} stop {SPARQL_SERVICE_NAME}")
    return container_infos_file

def start_generator(status_file):
    exec_cmd = CONFIG_GEN["generator"]["exec"]
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

    is_virtuoso_restarted = False
    VIRTUOSO_MANUAL_ENDPOINT = CONFIG_GEN["virtuoso"]["manual_port"]
    if VIRTUOSO_MANUAL_ENDPOINT != -1:
        if ping(f"http://localhost:{VIRTUOSO_MANUAL_ENDPOINT}/sparql") != 200:
            raise RuntimeError(f"Virtuoso endpoint {VIRTUOSO_MANUAL_ENDPOINT} is not available!")

    else:
        LOGGER.info("Activating Virtuoso docker container...")
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

#=================
# PIPELINE
#=================

rule all:
    input: 
        expand(
                "{benchDir}/{query}/instance_{instance_id}/batch_{batch_id}/rsa.sparql",
                benchDir=BENCH_DIR,
                query=[Path(os.path.join(QUERY_DIR, f)).resolve().stem for f in os.listdir(QUERY_DIR) if f.endswith(".sparql")],
                instance_id=range(N_QUERY_INSTANCES),
                batch_id=range(N_BATCH)
        )

rule ingest_virtuoso:
    priority: 4
    threads: 1
    input: 
        vendor=expand("{modelDir}/virtuoso/ingest_vendor_batch{{batch_id}}.sh", modelDir=MODEL_DIR),
        ratingsite=expand("{modelDir}/virtuoso/ingest_ratingsite_batch{{batch_id}}.sh", modelDir=MODEL_DIR),
    output: "{benchDir}/virtuoso_batch0-ok.txt"
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

rule deploy_virtuoso:
    priority: 5
    threads: 1
    input:
        vendor=ancient(expand("{modelDir}/dataset/vendor{vendor_id}.nq", vendor_id=range(N_VENDOR), modelDir=MODEL_DIR)),
        ratingsite=ancient(expand("{modelDir}/dataset/ratingsite{ratingsite_id}.nq", ratingsite_id=range(N_RATINGSITE), modelDir=MODEL_DIR)),
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

rule execute_workload_instances:
    priority: 7
    threads: 1
    input: 
        workload_instance="{benchDir}/{query}/instance_{instance_id}/injected.sparql",
        loaded_virtuoso="{benchDir}/virtuoso_batch9-ok.txt",
    output: "{benchDir}/{query}/instance_{instance_id}/batch_{batch_id}/rsa.sparql"
    run: 
        activate_one_container(f"{BENCH_DIR}/container_infos.csv", 9)
        shell("python fedshop/engines/rsa.py create-service-query {CONFIGFILE} {input.workload_instance} {output} --batch-id {wildcards.batch_id}")

rule instanciate_workload:
    priority: 7
    threads: 1
    input: 
        queryfile=expand("{queryDir}/{{query}}.sparql", queryDir=QUERY_DIR),
        workload_value_selection="{benchDir}/{query}/workload_value_selection.csv",
        container_infos = "{benchDir}/container_infos.csv"
    output:
        injected_query="{benchDir}/{query}/instance_{instance_id}/injected.sparql",
        injection_cache="{benchDir}/{query}/instance_{instance_id}/injection_cache.json",
        prefix_cache="{benchDir}/{query}/instance_{instance_id}/prefix_cache.json"
    params:
        batch_id = 0
    run:
        activate_one_container(f"{BENCH_DIR}/container_infos.csv", params.batch_id)
        shell("python fedshop/query.py instanciate-workload {CONFIGFILE} {input.queryfile} {input.workload_value_selection} {output.injected_query} {wildcards.instance_id}")
        
        in_injected_opt_query = f"{QUERY_DIR}/{wildcards.query}.injected.opt"
        out_injected_opt_query = f"{output.injected_query}.opt"

        if os.path.exists(in_injected_opt_query):
            shell(f"python fedshop/query.py inject-from-cache {in_injected_opt_query} {output.injection_cache} {out_injected_opt_query}")

rule create_workload_value_selection:
    priority: 8
    threads: 5
    input: 
        value_selection_query="{benchDir}/{query}/value_selection.sparql",
        value_selection="{benchDir}/{query}/value_selection.csv"
    output: "{benchDir}/{query}/workload_value_selection.csv"
    params:
        n_query_instances = N_QUERY_INSTANCES
    shell:
        "python fedshop/query.py create-workload-value-selection {input.value_selection_query} {input.value_selection} {output} {params.n_query_instances}"

rule exec_value_selection_query:
    priority: 9
    threads: 1
    retries: 2
    input: 
        value_selection_query="{benchDir}/{query}/value_selection.sparql",
        virtuoso_status="{benchDir}/virtuoso_batch0-ok.txt"
    output: "{benchDir}/{query}/value_selection.csv"
    params:
        batch_id=0
    run: 
        activate_one_container(f"{BENCH_DIR}/container_infos.csv", params.batch_id)
        shell("python fedshop/query.py execute-query {CONFIGFILE} {input.value_selection_query} {output} {params.batch_id}")

rule build_value_selection_query:
    priority: 10
    threads: 5
    input: 
        queryfile=expand("{queryDir}/{{query}}.sparql", queryDir=QUERY_DIR)
    output: "{benchDir}/{query}/value_selection.sparql"
    shell: "python fedshop/query.py build-value-selection-query {input.queryfile} {output}"
