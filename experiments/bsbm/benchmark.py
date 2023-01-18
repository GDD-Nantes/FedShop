import os
from pathlib import Path
import shutil
import click

import sys
directory = os.path.abspath(__file__)
sys.path.append(os.path.join(Path(directory).parent.parent.parent, "rsfb"))

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

WORKFLOW_DIR = f"{WORK_DIR}/rulegraph"
os.makedirs(name=WORKFLOW_DIR, exist_ok=True)

@cli.command()
@click.option("--debug", is_flag=True, default=False)
@click.option("--clean", type=click.Choice(["all", "model", "benchmark"]))
@click.option("--rerun-incomplete", is_flag=True, default=False)
@click.pass_context
def generate(ctx: click.Context, debug, clean, rerun_incomplete):
    """Run the benchmark

    Args:
        mode (_type_): Either "generate" or "evaluate"
        op (_type_): Either "debug" or "clean"
    """

    SNAKEMAKE_OPTS = f"-p --cores {N_CORES}"
    if rerun_incomplete: SNAKEMAKE_OPTS += " --rerun-incomplete"

    # If in generate mode
    if clean is not None:
        print("Cleaning...")
        if os.system(f"docker-compose -f {GENERATOR_COMPOSE_FILE} down -v --remove-orphans") != 0 : exit(1)
        if os.system(f"docker-compose -f {SPARQL_COMPOSE_FILE} down -v --remove-orphans") != 0 : exit(1)
        ctx.invoke(wipe, level=clean)

    for batch in range(1, N_BATCH+1):
        if debug:
            print("Producing rulegraph...")
            RULEGRAPH_FILE = f"{WORKFLOW_DIR}/rulegraph_generate_batch{batch}"
            if os.system(f"snakemake {SNAKEMAKE_OPTS} --snakefile {GENERATION_SNAKEFILE} --debug-dag --batch merge_metrics={batch}/{N_BATCH}") != 0 : exit(1)
            if os.system(f"snakemake {SNAKEMAKE_OPTS} --snakefile {GENERATION_SNAKEFILE} --rulegraph > {RULEGRAPH_FILE}.dot") != 0 : exit(1)
            if os.system(f"dot -Tpng {RULEGRAPH_FILE}.dot > {RULEGRAPH_FILE}.png") != 0 : exit(1)
        else:
            print(f"Producing metrics for batch {batch}/{N_BATCH}...")
            if os.system(f"snakemake {SNAKEMAKE_OPTS} --snakefile {GENERATION_SNAKEFILE} --batch merge_metrics={batch}/{N_BATCH}") != 0 : exit(1)

@cli.command()
@click.option("--debug", is_flag=True, default=False)
@click.option("--clean", type=click.Choice(["all", "model"]))
@click.option("--rerun-incomplete", is_flag=True, default=False)
@click.pass_context
def evaluate(ctx: click.Context, debug, clean, rerun_incomplete):

    SNAKEMAKE_OPTS = f"-p --cores {N_CORES}"
    if rerun_incomplete: SNAKEMAKE_OPTS += " --rerun-incomplete"

    # if in evaluate mode
    if clean is not None :
        print("Cleaning...")
        os.system(f"docker-compose -f {SPARQL_COMPOSE_FILE} down")
        shutil.rmtree(f"{WORK_DIR}/benchmark/evaluation")

    for batch in range(1, N_ENGINES+1):
        if debug:
            print("Producing rulegraph...")
            RULEGRAPH_FILE = f"{WORKFLOW_DIR}/rulegraph_generate_batch{batch}"
            if os.system(f"snakemake {SNAKEMAKE_OPTS} --snakefile {EVALUATION_SNAKEFILE} --debug-dag --batch merge_metrics={batch}/{N_ENGINES}") != 0 : exit(1)
            if os.system(f"snakemake {SNAKEMAKE_OPTS} --snakefile {EVALUATION_SNAKEFILE} --rulegraph > {RULEGRAPH_FILE}.dot") != 0 : exit(1)
            if os.system(f"dot -Tpng {RULEGRAPH_FILE}.dot > {RULEGRAPH_FILE}.png") != 0 : exit(1)
        else:
            print(f"Producing metrics for batch {batch}/{N_ENGINES}...")
            if os.system(f"snakemake {SNAKEMAKE_OPTS} --snakefile {EVALUATION_SNAKEFILE} --batch merge_metrics={batch}/{N_ENGINES}") != 0 : exit(1)

@cli.command()
@click.option("--level", type=click.Choice(["all", "model", "benchmark"]), default="benchmark")
def wipe(level):
    def remove_model():
        shutil.rmtree("experiments/bsbm/model/exported", ignore_errors=True)
        shutil.rmtree("experiments/bsbm/model/virtuoso", ignore_errors=True)
    
    def remove_benchmark():
        shutil.rmtree("experiments/bsbm/benchmark", ignore_errors=True)
        shutil.rmtree("experiments/bsbm/rulegraph", ignore_errors=True)

    if level == "all":
        Path("experiments/bsbm/generator-ok.txt").unlink(missing_ok=True)
        shutil.rmtree("experiments/bsbm/model/tmp", ignore_errors=True)
        remove_model()
        remove_benchmark()

    elif level == "model":
        remove_model()
        remove_benchmark()
    elif level == "benchmark":
        remove_benchmark()


if __name__ == "__main__":
    cli()