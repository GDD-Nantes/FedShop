import os
from pathlib import Path
import shutil
import subprocess
import click
from utils import load_config

@click.group
def cli():
    pass

@cli.command()
@click.argument("experiment", type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.option("--debug", is_flag=True, default=False)
@click.option("--clean", type=click.Choice(["all", "model", "benchmark"]))
@click.option("--cores", type=click.INT, default=1, help="The number of cores used allocated. -1 if use all cores.")
@click.option("--rerun-incomplete", is_flag=True, default=False)
@click.pass_context
def generate(ctx: click.Context, experiment, debug, clean, cores, rerun_incomplete):
    """Run the benchmark

    Args:
        mode (_type_): Either "generate" or "evaluate"
        op (_type_): Either "debug" or "clean"
    """

    if cores == -1: cores = "all"

    configfile = os.path.join(experiment, "config.yaml")

    CONFIG = load_config(configfile)["generation"]
    WORK_DIR = CONFIG["workdir"]

    SPARQL_COMPOSE_FILE = CONFIG["sparql"]["compose-file"]
    GENERATOR_COMPOSE_FILE = CONFIG["generator"]["compose-file"]

    N_BATCH=CONFIG["n_batch"]

    GENERATION_SNAKEFILE=f"{WORK_DIR}/generate-batch.smk"

    WORKFLOW_DIR = f"{WORK_DIR}/rulegraph"
    os.makedirs(name=WORKFLOW_DIR, exist_ok=True)

    SNAKEMAKE_OPTS = f"-p --cores {cores}"
    if rerun_incomplete: SNAKEMAKE_OPTS += " --rerun-incomplete"

    # If in generate mode
    if clean is not None:
        print("Cleaning...")
        if os.system(f"docker-compose -f {GENERATOR_COMPOSE_FILE} down -v --remove-orphans --volumes") != 0 : exit(1)
        if os.system(f"docker-compose -f {SPARQL_COMPOSE_FILE} down -v --remove-orphans --volumes") != 0 : exit(1)
        ctx.invoke(wipe, experiment=experiment, level=clean)

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
@click.argument("experiment", type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.option("--debug", is_flag=True, default=False)
@click.option("--clean", type=click.Choice(["all", "model"]))
@click.option("--rerun-incomplete", is_flag=True, default=False)
@click.pass_context
def evaluate(ctx: click.Context, experiment, debug, clean, rerun_incomplete):

    configfile = os.path.join(experiment, "config.yaml")

    GEN_CONFIG = load_config(configfile)["generation"]
    EVAL_CONFIG = load_config(configfile)["evaluation"]
    WORK_DIR = GEN_CONFIG["workdir"]

    SPARQL_COMPOSE_FILE = GEN_CONFIG["sparql"]["compose-file"]
    EVALUATION_SNAKEFILE=f"{WORK_DIR}/evaluate.smk"
    N_ENGINES = len(EVAL_CONFIG["evaluation"]["engines"])

    WORKFLOW_DIR = f"{WORK_DIR}/rulegraph"
    os.makedirs(name=WORKFLOW_DIR, exist_ok=True)

    SNAKEMAKE_OPTS = f"-p --cores 1"
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
@click.argument("level", type=click.Choice(["all", "model", "benchmark"]))
def save(level):
    if level == "benchmark":
        subprocess.run("")

@cli.command()
@click.argument("experiment", type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.option("--level", type=click.Choice(["all", "model", "benchmark"]), default="benchmark")
def wipe(experiment, level):

    configfile = os.path.join(experiment, "config.yaml")

    CONFIG = load_config(configfile)["generation"]
    WORK_DIR = CONFIG["workdir"]

    def remove_model():
        shutil.rmtree(f"{WORK_DIR}/model/dataset", ignore_errors=True)
        
    def remove_benchmark():
        shutil.rmtree(f"{WORK_DIR}/model/virtuoso", ignore_errors=True)
        shutil.rmtree(f"{WORK_DIR}/benchmark", ignore_errors=True)
        shutil.rmtree(f"{WORK_DIR}/rulegraph", ignore_errors=True)

    if level == "all":
        Path(f"{WORK_DIR}/generator-ok.txt").unlink(missing_ok=True)
        shutil.rmtree(f"{WORK_DIR}/model/tmp", ignore_errors=True)
        remove_model()
        remove_benchmark()

    elif level == "model":
        remove_model()
        remove_benchmark()
        
    elif level == "benchmark":
        remove_benchmark()


if __name__ == "__main__":
    cli()