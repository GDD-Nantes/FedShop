from omegaconf import OmegaConf as yaml
import pandas as pd
import os
import time
import requests
import glob

SPARQL_ENDPOINT = os.environ["RSFB__SPARQL_ENDPOINT"]
SPARQL_COMPOSE_FILE = os.environ["RSFB__SPARQL_COMPOSE_FILE"]
SPARQL_CONTAINER_NAME = os.environ["RSFB__SPARQL_CONTAINER_NAME"]

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
BENCH_DIR = f"{WORK_DIR}/benchmark/evaluation"

config = yaml.load(f"{WORK_DIR}/config.yaml")["evaluation"]

# ======== USEFUL FUNCTIONS =========
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
    os.system(f"docker-compose -f {SPARQL_COMPOSE_FILE} up -d {SPARQL_CONTAINER_NAME}")
    wait_for_container(SPARQL_ENDPOINT, status_file, wait=1)
    return status_file

def prerequisite_for_engine(wildcards):
    engine = str(wildcards.engine)
    if engine == "fedx":
        return f"{WORK_DIR}/fedx/virtuoso-batch{N_BATCH-1}-ok.txt"
    return "unknown"

# ======== RULES =========

rule all:
    input: expand("{benchDir}/metrics.csv", benchDir=BENCH_DIR)

rule merge_metrics:
    input: 
        expand(
            "{{benchDir}}/{engine}/{query}/{instance_id}/batch_{batch_id}/{mode}/stats.csv", 
            engine=config["engines"],
            query=[Path(os.path.join(QUERY_DIR, f)).resolve().stem for f in os.listdir(QUERY_DIR) if "_" not in f],
            instance_id=range(N_VARIATIONS),
            batch_id=range(N_BATCH),
            mode=["default", "ideal"]
        )
    output: "{benchDir}/metrics.csv"
    run: pd.concat((pd.read_csv(f) for f in input)).to_csv(f"{output}", index=False)

rule measure_default_stats:
    threads: 1
    retries: 3
    input: 
        query=expand("{workDir}/benchmark/generation/{{query}}/{{instance_id}}/injected.sparql", workDir=WORK_DIR),
        configfile="{benchDir}/{engine}/config/{batch_id}/{engine}.conf",
        prerequisite=prerequisite_for_engine
    output: 
        results="{benchDir}/{engine}/{query}/{instance_id}/batch_{batch_id}/default/results",
        stats="{benchDir}/{engine}/{query}/{instance_id}/batch_{batch_id}/default/stats.csv",
    shell:
        "python scripts/engines/{wildcards.engine}.py run-benchmark {input.configfile} {input.query} {output.results} {output.stats}"
 
rule measure_ideal_source_selection_stats:
    threads: 1
    retries: 3
    input: 
        query=expand("{workDir}/benchmark/generation/{{query}}/{{instance_id}}/injected.sparql", workDir=WORK_DIR),
        ideal_ss=expand("{workDir}/benchmark/generation/{{query}}/{{instance_id}}/batch_{{batch_id}}/provenance.csv", workDir=WORK_DIR),
        configfile="{benchDir}/{engine}/config/{batch_id}/{engine}.conf",
        prerequisite=prerequisite_for_engine
    output: 
        results="{benchDir}/{engine}/{query}/{instance_id}/batch_{batch_id}/ideal/results",
        stats="{benchDir}/{engine}/{query}/{instance_id}/batch_{batch_id}/ideal/stats.csv",
    shell: 
        "python scripts/engines/{wildcards.engine}.py run-benchmark {input.configfile} {input.query} {output.results} {output.stats} --ideal-ss {input.ideal_ss}"

rule generate_federation_declaration:
    output: "{benchDir}/{engine}/config/{batch_id}/{engine}.conf"
    run: 
        person_data_files = [ f"{MODEL_DIR}/exported/person{i}.nq" for i in range(TOTAL_REVIEWER) ]
        vendor_data_files = [ f"{MODEL_DIR}/exported/vendor{i}.nq" for i in range(TOTAL_VENDOR) ]

        batchId = int(wildcards.batch_id)
        personSliceId = list(range(N_BATCH, (TOTAL_REVIEWER + 1)*N_BATCH, N_BATCH))[batchId]
        vendorSliceId = list(range(N_BATCH, (TOTAL_VENDOR + 1)*N_BATCH, N_BATCH))[batchId]
        batch_files = person_data_files[:personSliceId] + vendor_data_files[:vendorSliceId]

        os.system(f"python scripts/engines/{wildcards.engine}.py generate-config-file {' '.join(batch_files)} {output} --endpoint {SPARQL_ENDPOINT}")

rule ingest_virtuoso:
    threads: 1
    input: 
        vendor=expand("{modelDir}/virtuoso/ingest_vendor_batch{lastBatch}.sh", modelDir=MODEL_DIR, lastBatch=N_BATCH-1),
        person=expand("{modelDir}/virtuoso/ingest_person_batch{lastBatch}.sh", modelDir=MODEL_DIR, lastBatch=N_BATCH-1),
        virtuoso_status="{benchDir}/fedx/virtuoso-up.txt"
    output: "{benchDir}/fedx/virtuoso-batch{lastBatch}-ok.txt"
    run: 
        proc = subprocess.run(f"docker exec {SPARQL_CONTAINER_NAME} ls /usr/local/virtuoso-opensource/share/virtuoso/vad | wc -l", shell=True, capture_output=True)
        nFiles = int(proc.stdout.decode())
        expected_nFiles = len(glob.glob(f"{MODEL_DIR}/exported/*.nq"))
        if nFiles != expected_nFiles: raise RuntimeError(f"Expecting {expected_nFiles} *.nq files in virtuoso container, got {nFiles}!") 
        os.system(f'sh {input.vendor} bsbm && sh {input.person} && echo "OK" > {output}')

rule restart_virtuoso:
    priority: 5
    threads: 1
    output: "{benchDir}/fedx/virtuoso-up.txt"
    run: restart_virtuoso(output)

