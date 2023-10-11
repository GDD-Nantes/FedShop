from itertools import product
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
@click.argument("category", type=click.Choice(["data", "queries"]))
@click.argument("configfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.option("--debug", is_flag=True, default=False)
@click.option("--clean", type=click.STRING, help="[all, model, benchmark] + db + [metrics|metrics_batchk]")
@click.option("--cores", type=click.INT, default=1, help="The number of cores used allocated. -1 if use all cores.")
@click.option("--rerun-incomplete", is_flag=True, default=False)
@click.option("--touch", is_flag=True, default=False)
@click.option("--no-cache", is_flag=True, default=False)
@click.pass_context
def generate(ctx: click.Context, category, configfile, debug, clean, cores, rerun_incomplete, touch, no_cache):
    """Run the benchmark

    Args:
        mode (_type_): Either "generate" or "evaluate"
        op (_type_): Either "debug" or "clean"
    """
    
    if no_cache:
        shutil.rmtree(".snakemake")

    if cores == -1: cores = "all"

    CONFIG = load_config(configfile)["generation"]
    WORK_DIR = CONFIG["workdir"]

    N_BATCH=CONFIG["n_batch"]

    GENERATION_SNAKEFILE=f"{WORK_DIR}/generate-{category}.smk"

    WORKFLOW_DIR = f"{WORK_DIR}/rulegraph"
    os.makedirs(name=WORKFLOW_DIR, exist_ok=True)

    SNAKEMAKE_OPTS = f"-p --cores {cores} --config configfile={configfile}"
    if rerun_incomplete: SNAKEMAKE_OPTS += " --rerun-incomplete"
    
    if touch:
        logger.info("Marking files as completed...")
        shutil.rmtree(".snakemake", ignore_errors=True)
        SNAKEMAKE_OPTS += " --touch"

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
@click.argument("experiment-dir", type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.option("--update", is_flag=True, default=False)
def save_model(experiment_dir, update):
    oldir = os.getcwd()
    os.chdir(experiment_dir)
    
    if not update:
        Path("eval-model.zip").unlink(missing_ok=True)
        Path("gen-model.zip").unlink(missing_ok=True)
    
    logger.info(f"Packaging {experiment_dir}/benchmark/evaluation/")
    os.system("zip -r eval-model.zip benchmark/evaluation")
    logger.info(f"Packaging {experiment_dir}/benchmark/generation/")
    os.system("zip -r gen-model.zip benchmark/generation")
    os.chdir(oldir)
    
@cli.command()
@click.argument("modelfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("experiment-dir", type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.option("--clean", is_flag=True, default=False)
def load_model(modelfile, experiment_dir, clean):
    
    if clean:
        shutil.rmtree(f"{experiment_dir}/benchmark/evaluation/")
        shutil.rmtree(f"{experiment_dir}/benchmark/generation/")
        
    os.system(f"unzip {modelfile} -d {experiment_dir}")

@cli.command()
@click.argument("configfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.option("--config", type=click.STRING, default=None)
@click.option("--debug", is_flag=True, default=False)
@click.option("--clean", type=click.STRING, help="[all, model, benchmark] + db")
@click.option("--cores", type=click.INT, default=1, help="The number of cores used allocated. -1 if use all cores.")
@click.option("--rerun-incomplete", is_flag=True, default=False)
@click.option("--touch", is_flag=True, default=False)
@click.option("--no-cache", is_flag=True, default=False)
@click.option("--noexec", is_flag=True, default=False)
@click.pass_context
def evaluate(ctx: click.Context, configfile, config, debug, clean, cores, rerun_incomplete, touch, no_cache, noexec):

    CONFIG = load_config(configfile)
    GEN_CONFIG = CONFIG["generation"]
    WORK_DIR = GEN_CONFIG["workdir"]
    BENCH_DIR = f"{WORK_DIR}/benchmark/evaluation"

    EVALUATION_SNAKEFILE=f"{WORK_DIR}/evaluate.smk"
    SNAKEMAKE_CONFIGS = f"configfile={configfile} "
    SINGLE_QUERY_MODE = False
    SNAKEMAKE_CONFIG_MATCHER = None

    N_BATCH = GEN_CONFIG["n_batch"]

    CONFIG_EVAL = CONFIG["evaluation"]
    QUERY_DIR = f"{WORK_DIR}/queries"

    config_dict = {}
    
    if config is not None:
        config = config.strip()
        SNAKEMAKE_CONFIG_MATCHER = re.match(r"(\w+\=\w+(,\w+)*(\s+)?)+", config)
        if SNAKEMAKE_CONFIG_MATCHER is None:
            raise RuntimeError(f"Syntax error: config option should be 'name1=value1 name2=value2'")
        
        for c in config.split():
            k, v = c.split("=")
            config_dict[k] = v.split(",")

        if "batch" not in config_dict.keys():
            config_dict["batch"] = list(range(N_BATCH))

        if "engine" not in config_dict.keys():
            config_dict["batch"] = CONFIG_EVAL["engines"]

        if "query" not in config_dict.keys():
            config_dict["query"] = [Path(os.path.join(QUERY_DIR, f)).resolve().stem for f in os.listdir(QUERY_DIR) if f.endswith(".sparql")]

        if "instance" not in config_dict.keys():
            config_dict["instance"] = list(range(GEN_CONFIG["n_query_instances"]))
    
        SNAKEMAKE_CONFIGS += config
        if "query" in config or "engine" in config or "instance" in config or "batch" in config:
            SINGLE_QUERY_MODE = True
            EVALUATION_SNAKEFILE=f"{WORK_DIR}/evaluate_one.smk"
        
    if noexec:
        EVALUATION_SNAKEFILE=f"{WORK_DIR}/evaluate_noexec.smk"
    
    WORKFLOW_DIR = f"{WORK_DIR}/rulegraph"
    os.makedirs(name=WORKFLOW_DIR, exist_ok=True)

    if cores == -1: cores = "all"
    SNAKEMAKE_OPTS = f"-p --cores {cores} --config {SNAKEMAKE_CONFIGS}"
    if rerun_incomplete: SNAKEMAKE_OPTS += " --rerun-incomplete"
    
    if no_cache:
        shutil.rmtree(".snakemake")
    
    if touch:
        logger.info("Marking files as completed...")
        shutil.rmtree(".snakemake", ignore_errors=True)
        SNAKEMAKE_OPTS += " --touch"

    # if in evaluate mode
    if clean is not None :
        logger.info("Cleaning...")
        if SINGLE_QUERY_MODE:     
            keys, values = zip(*config_dict.items())
            for comb in product(*values):
                path_dict = dict(zip(keys, comb))
                shutil.rmtree(f"{BENCH_DIR}/{path_dict['engine']}/{path_dict['query']}/instance_{path_dict['instance']}/batch_{path_dict['batch']}/debug/", ignore_errors=True)
        else:
            if clean == "all":
                shutil.rmtree(f"{WORK_DIR}/benchmark/evaluation", ignore_errors=True)
            elif clean == "metrics":
                os.system(f"rm {WORK_DIR}/benchmark/evaluation/*.csv")
    
    batch_size = len(config_dict["batch"])
    if batch_size == 1:
        if os.system(f"snakemake {SNAKEMAKE_OPTS} --snakefile {EVALUATION_SNAKEFILE}") != 0 : exit(1)

    else:
        for batch in range(1, batch_size):
            if debug:
                logger.info("Producing rulegraph...")
                RULEGRAPH_FILE = f"{WORKFLOW_DIR}/rulegraph_generate_batch{batch}"
                if os.system(f"snakemake {SNAKEMAKE_OPTS} --snakefile {EVALUATION_SNAKEFILE} --debug-dag --batch merge_metrics={batch}/{batch_size}") != 0 : exit(1)
                if os.system(f"snakemake {SNAKEMAKE_OPTS} --snakefile {EVALUATION_SNAKEFILE} --rulegraph > {RULEGRAPH_FILE}.dot") != 0 : exit(1)
                if os.system(f"dot -Tpng {RULEGRAPH_FILE}.dot > {RULEGRAPH_FILE}.png") != 0 : exit(1)
            else:
                logger.info(f"Producing metrics for batch {batch}/{batch_size}...")
                if os.system(f"snakemake {SNAKEMAKE_OPTS} --snakefile {EVALUATION_SNAKEFILE} --batch merge_metrics={batch}/{batch_size}") != 0 : exit(1)
            
@cli.command()
@click.argument("configfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
def generate_statistics(configfile):

    WORK_DIR = load_config(configfile)["generation"]["workdir"]
    EVALUATION_SNAKEFILE=f"{WORK_DIR}/stats.smk"
    SNAKEMAKE_OPTS = f"-p --cores 1 --config configfile={configfile}"

    if os.system(f"snakemake {SNAKEMAKE_OPTS} --snakefile {EVALUATION_SNAKEFILE}") != 0 : exit(1)

@cli.command()
@click.argument("configfile", type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.option("--level", type=click.STRING, default="benchmark")
def wipe(configfile, level: str):
    
    args = level.split("+")

    CONFIG = load_config(configfile)["generation"]
    WORK_DIR = CONFIG["workdir"]
    
    SPARQL_COMPOSE_FILE = CONFIG["virtuoso"]["compose_file"]

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
                
    if "instances-root" in args:
        logger.info("Cleaning all te...")
        Path(f"{WORK_DIR}/benchmark/generation/metrics.csv").unlink(missing_ok=True)   
        os.system(f"rm -r {WORK_DIR}/benchmark/generation/q*/")
    
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