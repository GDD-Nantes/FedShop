import os
from pathlib import Path
import click

import sys
directory = os.path.abspath(__file__)
sys.path.append(os.path.join(Path(directory).parent.parent, "rsfb"))

@click.group
def cli():
    pass

from utils import load_config

WORK_DIR = "experiments/bsbm"
CONFIG = load_config(f"{WORK_DIR}/config.yaml")["generation"]

SPARQL_COMPOSE_FILE = CONFIG["sparql"]["compose-file"]
GENERATOR_COMPOSE_FILE = CONFIG["generator"]["compose-file"]

N_BATCH=CONFIG["n_batch"]

GENERATION_SNAKEFILE="workflow/generate-batch.smk"
EVALUATION_SNAKEFILE="workflow/evaluate.smk"

N_CORES=1 # any number or "all"
N_ENGINES=1

CLEAN_SCRIPT=f"{WORK_DIR}/clean.sh"

@cli.command()
@click.argument("mode", type=click.Choice(["generate", "evaluate"]))
@click.option("--op", type=click.Choice(["debug", "clean"]))
def benchmark(mode, op):
    """Run the benchmark

    Args:
        mode (_type_): Either "generate" or "evaluate"
        op (_type_): Either "debug" or "clean"
    """

    # FUNCTIONS
    def help():
        print('sh benchmark.sh MODE(["generate", "evaluate"]) DEBUG(["debug"])')

    def syntax_error():
        help()  
        exit(1)

    SNAKEMAKE_OPTS = f"-p --cores {N_CORES} --rerun-incomplete"
    WORKFLOW_DIR = f"{WORK_DIR}/rulegraph"
    os.makedirs(name=WORKFLOW_DIR, exist_ok=True)

    # If in generate mode
    if mode == "generate":
        if op == "clean":
            print("Cleaning...")
            if os.system(f"docker-compose -f {GENERATOR_COMPOSE_FILE} down -v --remove-orphans") != 0 : exit(1)
            if os.system(f"docker-compose -f {SPARQL_COMPOSE_FILE} down -v --remove-orphans") != 0 : exit(1)
            if os.system(f"sh {CLEAN_SCRIPT} deep") != 0 : exit(1)

        for batch in range(1, N_BATCH+1):
            if op == "debug":
                print("Producing rulegraph...")
                RULEGRAPH_FILE = f"{WORKFLOW_DIR}/rulegraph_{mode}_batch{batch}"
                if os.system(f"snakemake {SNAKEMAKE_OPTS} --snakefile {GENERATION_SNAKEFILE} --debug-dag --batch merge_metrics={batch}/{N_BATCH}") != 0 : exit(1)
                if os.system(f"snakemake {SNAKEMAKE_OPTS} --snakefile {GENERATION_SNAKEFILE} --rulegraph > {RULEGRAPH_FILE}.dot") != 0 : exit(1)
                if os.system(f"dot -Tpng {RULEGRAPH_FILE}.dot > {RULEGRAPH_FILE}.png") != 0 : exit(1)
            else:
                print(f"Producing metrics for batch {batch}/{N_BATCH}...")
                if os.system(f"snakemake {SNAKEMAKE_OPTS} --snakefile {GENERATION_SNAKEFILE} --batch merge_metrics={batch}/{N_BATCH}") != 0 : exit(1)
            
    # if in evaluate mode
    elif mode == "evaluate":
        if op == "clean" :
            print("Cleaning...")
            #docker-compose -f {SPARQL_COMPOSE_FILE} down &&
            os.system(f"rm -rf {WORK_DIR}/benchmark/evaluation")

        for batch in range(1, N_ENGINES+1):
            if op == "debug":
                print("Producing rulegraph...")
                RULEGRAPH_FILE = f"{WORKFLOW_DIR}/rulegraph_{mode}_batch{batch}"
                if os.system(f"snakemake {SNAKEMAKE_OPTS} --snakefile {EVALUATION_SNAKEFILE} --debug-dag --batch merge_metrics={batch}/{N_ENGINES}") != 0 : exit(1)
                if os.system(f"snakemake {SNAKEMAKE_OPTS} --snakefile {EVALUATION_SNAKEFILE} --rulegraph > {RULEGRAPH_FILE}.dot") != 0 : exit(1)
                if os.system(f"dot -Tpng {RULEGRAPH_FILE}.dot > {RULEGRAPH_FILE}.png") != 0 : exit(1)
            else:
                print(f"Producing metrics for batch {batch}/{N_ENGINES}...")
                if os.system(f"snakemake {SNAKEMAKE_OPTS} --snakefile {EVALUATION_SNAKEFILE} --batch merge_metrics={batch}/{N_ENGINES}") != 0 : exit(1)
    else:
        syntax_error()

if __name__ == "__main__":
    cli()