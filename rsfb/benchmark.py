import os
from pathlib import Path
import re
import shutil
import subprocess
import click
from utils import load_config, rsfb_logger

logger = rsfb_logger(Path(__file__).name)

@click.group
def cli():
    pass

@cli.command()
@click.argument("configfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.option("--debug", is_flag=True, default=False)
@click.option("--clean", type=click.STRING, help="[all, model, benchmark] + db + [metrics|metrics_batchk]")
@click.option("--cores", type=click.INT, default=1, help="The number of cores used allocated. -1 if use all cores.")
@click.option("--rerun-incomplete", is_flag=True, default=False)
@click.pass_context
def generate(ctx: click.Context, configfile, debug, clean, cores, rerun_incomplete):
    """Run the benchmark

    Args:
        mode (_type_): Either "generate" or "evaluate"
        op (_type_): Either "debug" or "clean"
    """

    if cores == -1: cores = "all"

    CONFIG = load_config(configfile)["generation"]
    WORK_DIR = CONFIG["workdir"]

    N_BATCH=CONFIG["n_batch"]

    GENERATION_SNAKEFILE=f"{WORK_DIR}/generate.smk"

    WORKFLOW_DIR = f"{WORK_DIR}/rulegraph"
    os.makedirs(name=WORKFLOW_DIR, exist_ok=True)

    SNAKEMAKE_OPTS = f"-p --cores {cores} --config configfile={configfile}"
    if rerun_incomplete: SNAKEMAKE_OPTS += " --rerun-incomplete"

    # If in generate mode
    if clean is not None:
        logger.info("Cleaning...")
        ctx.invoke(wipe, configfile=configfile, level=clean)

    for batch in range(1, N_BATCH+1):
        if debug:
            logger.info("Producing rulegraph...")
            RULEGRAPH_FILE = f"{WORKFLOW_DIR}/rulegraph_generate_batch{batch}"
            if os.system(f"snakemake {SNAKEMAKE_OPTS} --snakefile {GENERATION_SNAKEFILE} --debug-dag --batch merge_metrics={batch}/{N_BATCH}") != 0 : exit(1)
            if os.system(f"snakemake {SNAKEMAKE_OPTS} --snakefile {GENERATION_SNAKEFILE} --rulegraph > {RULEGRAPH_FILE}.dot") != 0 : exit(1)
            if os.system(f"dot -Tpng {RULEGRAPH_FILE}.dot > {RULEGRAPH_FILE}.png") != 0 : exit(1)
        else:
            logger.info(f"Producing metrics for batch {batch}/{N_BATCH}...")
            if os.system(f"snakemake {SNAKEMAKE_OPTS} --snakefile {GENERATION_SNAKEFILE} --batch merge_metrics={batch}/{N_BATCH}") != 0 : exit(1)

@cli.command()
@click.argument("configfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.option("--debug", is_flag=True, default=False)
@click.option("--clean", type=click.STRING, help="[all, model, benchmark] + db")
@click.option("--cores", type=click.INT, default=1, help="The number of cores used allocated. -1 if use all cores.")
@click.option("--rerun-incomplete", is_flag=True, default=False)
@click.pass_context
def evaluate(ctx: click.Context, configfile, debug, clean, cores, rerun_incomplete):

    CONFIG = load_config(configfile)
    GEN_CONFIG = CONFIG["generation"]
    WORK_DIR = GEN_CONFIG["workdir"]

    EVALUATION_SNAKEFILE=f"{WORK_DIR}/evaluate.smk"
    N_BATCH = GEN_CONFIG["n_batch"]

    WORKFLOW_DIR = f"{WORK_DIR}/rulegraph"
    os.makedirs(name=WORKFLOW_DIR, exist_ok=True)

    SNAKEMAKE_OPTS = f"-p --cores {cores} --config configfile={configfile}"
    if rerun_incomplete: SNAKEMAKE_OPTS += " --rerun-incomplete"

    # if in evaluate mode
    if clean is not None :
        logger.info("Cleaning...")
        if clean == "all":
            shutil.rmtree(f"{WORK_DIR}/benchmark/evaluation", ignore_errors=True)
        elif clean == "metrics":
            os.system(f"rm {WORK_DIR}/benchmark/evaluation/*.csv")
    
    for batch in range(1, N_BATCH+1):
        if debug:
            logger.info("Producing rulegraph...")
            RULEGRAPH_FILE = f"{WORKFLOW_DIR}/rulegraph_generate_batch{batch}"
            if os.system(f"snakemake {SNAKEMAKE_OPTS} --snakefile {EVALUATION_SNAKEFILE} --debug-dag --batch merge_metrics={batch}/{N_BATCH}") != 0 : exit(1)
            if os.system(f"snakemake {SNAKEMAKE_OPTS} --snakefile {EVALUATION_SNAKEFILE} --rulegraph > {RULEGRAPH_FILE}.dot") != 0 : exit(1)
            if os.system(f"dot -Tpng {RULEGRAPH_FILE}.dot > {RULEGRAPH_FILE}.png") != 0 : exit(1)
        else:
            logger.info(f"Producing metrics for batch {batch}/{N_BATCH}...")
            if os.system(f"snakemake {SNAKEMAKE_OPTS} --snakefile {EVALUATION_SNAKEFILE} --batch merge_metrics={batch}/{N_BATCH}") != 0 : exit(1)
            
@cli.command()
@click.argument("level", type=click.Choice(["all", "model", "benchmark"]))
def save(level):
    if level == "benchmark":
        subprocess.run("")

@cli.command()
@click.argument("configfile", type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.option("--level", type=click.STRING, default="benchmark")
def wipe(configfile, level: str):
    
    args = level.split("+")

    CONFIG = load_config(configfile)["generation"]
    WORK_DIR = CONFIG["workdir"]
    
    SPARQL_COMPOSE_FILE = CONFIG["virtuoso"]["compose_file"]
    GENERATOR_COMPOSE_FILE = CONFIG["generator"]["compose_file"]

    def remove_model():
        shutil.rmtree(f"{WORK_DIR}/model/dataset", ignore_errors=True)
        
    def remove_benchmark(including_db=False):
        shutil.rmtree(f"{WORK_DIR}/model/virtuoso", ignore_errors=True)
        if including_db:
            shutil.rmtree(f"{WORK_DIR}/benchmark", ignore_errors=True)
        else: 
            shutil.rmtree(f"{WORK_DIR}/benchmark/generation", ignore_errors=True)
        shutil.rmtree(f"{WORK_DIR}/rulegraph", ignore_errors=True)
        
    if "db" in args:
        logger.info("Cleaning all databases...")
        if os.system(f"docker-compose -f {GENERATOR_COMPOSE_FILE} down --remove-orphans --volumes") != 0 : exit(1)
        if os.system(f"docker-compose -f {SPARQL_COMPOSE_FILE} down --remove-orphans --volumes") != 0 : exit(1)  
        if os.system("docker volume prune --force") != 0: exit(1)
        os.system(f"{WORK_DIR}/benchmark/generation/virtuoso_batch*.csv")
        os.system(f"{WORK_DIR}/benchmark/generation/virtuoso-*.csv")
        
    if "metrics" in args:
        logger.info("Cleaning all metrics...")
        Path(f"{WORK_DIR}/benchmark/generation/metrics.csv").unlink(missing_ok=True)   
        os.system(f"rm {WORK_DIR}/benchmark/generation/metrics_batch*.csv")
    elif "metrics_" in level:
        Path(f"{WORK_DIR}/benchmark/generation/metrics.csv").unlink(missing_ok=True)   
        matched = re.search(r"metrics_batch((\\d+%)*(\\d+))", level)
        if matched is not None:
            batches = matched.group(1).split("%")
            for batch in batches:
                logger.info(f"Cleaning metrics for batch {batch}")
                os.system(f"rm {WORK_DIR}/benchmark/generation/metrics_batch{batch}.csv")
                
    if "instances" in args:
        logger.info("Cleaning all instances...")
        Path(f"{WORK_DIR}/benchmark/generation/metrics.csv").unlink(missing_ok=True)   
        os.system(f"rm -r {WORK_DIR}/benchmark/generation/q*/instance_*/")
    elif "instance_" in level:
        Path(f"{WORK_DIR}/benchmark/generation/metrics.csv").unlink(missing_ok=True)   
        matched = re.search(r"instance_((\\d+%)*(\\d+))", level)
        if matched is not None:
            instances = matched.group(1).split("%")
            for instance in instances:
                logger.info(f"Cleaning instance {instance}")
                os.system(f"rm -r {WORK_DIR}/benchmark/generation/q*/instance_{instance}/")

    if "all" in args:
        logger.info("Cleaning all databases...")
        if os.system(f"docker-compose -f {GENERATOR_COMPOSE_FILE} down --remove-orphans --volumes") != 0 : exit(1)
        if os.system(f"docker-compose -f {SPARQL_COMPOSE_FILE} down --remove-orphans --volumes") != 0 : exit(1)  
        if os.system("docker volume prune --force") != 0: exit(1)
        Path(f"{WORK_DIR}/generator-ok.txt").unlink(missing_ok=True)
        shutil.rmtree(f"{WORK_DIR}/model/tmp", ignore_errors=True)
        remove_model()
        remove_benchmark()

    elif "model" in args:
        remove_model()
        remove_benchmark()
        
    elif "benchmark" in args:
        remove_benchmark()


if __name__ == "__main__":
    cli()