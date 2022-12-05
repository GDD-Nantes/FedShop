from omegaconf import OmegaConf as yaml
import pandas as pd
import os

SPARQL_ENDPOINT = os.environ["RSFB__SPARQL_ENDPOINT"]
GENERATOR_ENDPOINt = os.environ["RSFB__GENERATOR_ENDPOINT"]

SPARQL_CONTAINER_NAME = os.environ["RSFB__SPARQL_CONTAINER_NAME"]
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
BENCH_DIR = f"{WORK_DIR}/benchmark/evaluation"

config = yaml.load(f"{WORK_DIR}/config.yaml")["evaluation"]

rule all:
    input: expand("{benchDir}/metrics.csv", benchDir=BENCH_DIR)

rule merge_metrics:
    input: 
        expand(
            "{{benchDir}}/{engine}/{query}/{instance_id}/batch{batch_id}/{mode}/exec_time.csv", 
            engine=config["engines"],
            query=[Path(os.path.join(QUERY_DIR, f)).resolve().stem for f in os.listdir(QUERY_DIR) if "_" not in f],
            instance_id=range(N_VARIATIONS),
            batch_id=range(N_BATCH),
            mode=["default", "ideal"]
        )
    output: "{benchDir}/metrics.csv"
    run: pd.concat((pd.read_csv(f) for f in input)).to_csv(f"{output}", index=False)

rule measure_default_exec_time:
    threads: 1
    input: 
        query=expand("{workDir}/benchmark/generation/{{query}}/{{instance_id}}/injected.sparql", workDir=WORK_DIR),
        ideal_ss=expand("{workDir}/benchmark/generation/{{query}}/{{instance_id}}/batch{{batch_id}}/provenance.csv", workDir=WORK_DIR),
        configfile="{benchDir}/{engine}/{batch_id}/fed.config"
    output: 
        results="{benchDir}/{engine}/{query}/{instance_id}/batch{batch_id}/default/results.csv"
        exec_time="{benchDir}/{engine}/{query}/{instance_id}/batch{batch_id}/default/exec_time.csv"
    shell:
        "python scripts/engines/{engine}.py run-benchmark {input.configfile} {input.query} {output.results} {output.exec_time} --ideal-ss {input.ideal_ss}"
 
rule measure_ideal_source_selection_exec_time:
    threads: 1
    input: 
        query=expand("{workDir}/benchmark/generation/{{query}}/{{instance_id}}/injected.sparql", workDir=WORK_DIR),
        ideal_ss=expand("{workDir}/benchmark/generation/{{query}}/{{instance_id}}/batch{{batch_id}}/provenance.csv", workDir=WORK_DIR),
        configfile="{benchDir}/{engine}/{batch_id}/fed.config"
    output: 
        results="{benchDir}/{engine}/{query}/{instance_id}/batch{batch_id}/ideal/results.csv"
        exec_time="{benchDir}/{engine}/{query}/{instance_id}/batch{batch_id}/ideal/exec_time.csv"
    shell: 
        "python scripts/engines/{engine}.py run-benchmark {input.configfile} {input.query} {output.results} {output.exec_time} --ideal-ss {input.ideal_ss}"


rule generate_federation_declaration:
    output: "{benchDir}/{engine}/{batch_id}/fed.config"
    run: 
        person_data_files = [ f"{MODEL_DIR}/exported/person{i}.nq" for i in range(TOTAL_REVIEWER) ]
        vendor_data_files = [ f"{MODEL_DIR}/exported/vendor{i}.nq" for i in range(TOTAL_VENDOR) ]

        batchId = int(wildcards.batch_id)
        personSliceId = list(range(N_BATCH, (N_REVIEWER + 1)*N_BATCH, N_BATCH))[batchId]
        vendorSliceId = list(range(N_BATCH, (N_VENDOR + 1)*N_BATCH, N_BATCH))[batchId]
        batch_files = person_data_files[:personSliceId] + vendor_data_files[:vendorSliceId]

        os.system(f"python scripts/engines/{wildcards.engine}.py generate-config-file {batch_files} {output} {SPARQL_ENDPOINT}")

