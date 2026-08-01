from itertools import product
import os
from pathlib import Path
import re
import shutil
import subprocess
import click
from .utils import load_config, fedshop_logger
import requests
import time

logger = fedshop_logger(Path(__file__).name)

@click.group
def cli():
    pass

@cli.command()
@click.argument("configfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.option("--clean", is_flag=True, default=False)
@click.option("--cores", type=click.INT, default=1, help="The number of cores used allocated. -1 if use all cores.")
@click.option("--config", type=click.STRING, default=None, help='Params for snakemake file. Example: --config="batches=0,1,2"')
@click.option("--rerun-incomplete", is_flag=True, default=False)
@click.option("--touch", is_flag=True, default=False)
@click.option("--no-cache", is_flag=True, default=False)
@click.option("--dry-run", is_flag=True, default=False)
@click.option("--force", is_flag=True, default=False)
@click.pass_context
def ingest(ctx: click.Context, configfile, clean, cores, config, rerun_incomplete, touch, no_cache, dry_run, force):
    """Ingest the data

    Args:
        mode (_type_): Either "generate" or "evaluate"
        op (_type_): Either "debug" or "clean"
    """
    
    if no_cache:
        shutil.rmtree(".snakemake")

    if cores == -1: cores = "all"

    CONFIG = load_config(configfile)["generation"]
    WORK_DIR = CONFIG["workdir"]

    INGESTION_SNAKEFILE = f"snakemake/ingest-data.smk"
    WORKFLOW_DIR = f"{WORK_DIR}/rulegraph"
    os.makedirs(name=WORKFLOW_DIR, exist_ok=True)

    config_dict = {
        "configfile": [configfile],
    }
    
    if config is not None:
        config = config.strip()
        SNAKEMAKE_CONFIG_MATCHER = re.match(r"(\w+\=\w+(,\w+)*(\s+)?)+", config)
        if SNAKEMAKE_CONFIG_MATCHER is None:
            raise RuntimeError(f"Syntax error: config option should be 'name1=value1 name2=value2'")
        
        for c in config.split():
            k, v = c.split("=")

            if k in ["debug", "explain"]:
                v = eval(v)

            config_dict[k] = v.split(",")
    
    SNAKEMAKE_CONFIGS = " ".join([f"{k}={','.join(v)}" for k, v in config_dict.items()])
    
    SNAKEMAKE_OPTS = ""
    
    if force:
        SNAKEMAKE_OPTS += " -F"

    SNAKEMAKE_OPTS += f" -p --cores {cores} --config {SNAKEMAKE_CONFIGS}"
    if rerun_incomplete: SNAKEMAKE_OPTS += " --rerun-incomplete"
    if dry_run: SNAKEMAKE_OPTS += " --dry-run"
    
    if touch:
        logger.info("Marking files as completed...")
        shutil.rmtree(".snakemake", ignore_errors=True)
        SNAKEMAKE_OPTS += " --touch"

    if clean:
        logger.info("Cleaning...")
        ctx.invoke(wipe, configfile=configfile, level="all")

    logger.info("Ingesting data...")
    cmd = f"snakemake {SNAKEMAKE_OPTS} --snakefile {INGESTION_SNAKEFILE}"
    logger.debug(cmd)
    if os.system(cmd) != 0 : exit(1)

@cli.command()
@click.argument("category", type=click.Choice(["data", "queries"]))
@click.argument("configfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.option("--config", type=click.STRING, default=None, help='Params for snakemake file. Example: --config="engine=costfed attempt=0 query=q04 instance=7 batch=0"')
@click.option("--debug", is_flag=True, default=False)
@click.option("--clean", type=click.STRING, help="[all, model, benchmark] + db + [metrics|metrics_batchk]")
@click.option("--cores", type=click.INT, default=1, help="The number of cores used allocated. -1 if use all cores.")
@click.option("--rerun-incomplete", is_flag=True, default=False)
@click.option("--touch", is_flag=True, default=False)
@click.option("--no-cache", is_flag=True, default=False)
@click.option("--dry-run", is_flag=True, default=False)
@click.option("--force", is_flag=True, default=False)
@click.pass_context
def generate(ctx: click.Context, category, configfile, config, debug, clean, cores, rerun_incomplete, touch, no_cache, dry_run, force):
    """Run the benchmark

    Args:
        mode (_type_): Either "generate" or "evaluate"
        op (_type_): Either "debug" or "clean"
    """
    
    if no_cache:
        shutil.rmtree(".snakemake")

    if cores == -1: cores = "all"

    CONFIG = load_config(configfile)
    CONFIG_GEN = CONFIG["generation"]
    CONFIG_EVAL = CONFIG["evaluation"]
    WORK_DIR = CONFIG_GEN["workdir"]
    N_BATCH=CONFIG_GEN["n_batch"]

    GENERATION_SNAKEFILE= (
        f"{WORK_DIR}/snakefile/generate-data.smk" 
        if category == "data" 
        else "snakemake/generate-queries.smk"
    )

    WORKFLOW_DIR = f"{WORK_DIR}/rulegraph"
    os.makedirs(name=WORKFLOW_DIR, exist_ok=True)
    
    QUERY_DIR = f"{WORK_DIR}/queries"
    
    config_dict = {
        "configfile": [configfile]
    }
    
    if config is not None:
        config = config.strip()
        SNAKEMAKE_CONFIG_MATCHER = re.match(r"(\w+\=\w+(,\w+)*(\s+)?)+", config)
        if SNAKEMAKE_CONFIG_MATCHER is None:
            raise RuntimeError(f"Syntax error: config option should be 'name1=value1 name2=value2'")
        
        for c in config.split():
            k, v = c.split("=")

            if k in ["debug", "explain"]:
                v = eval(v)

            config_dict[k] = v.split(",")

        if "batch" not in config_dict.keys():
            config_dict["batch"] = list(map(str, range(N_BATCH)))

        if "query" not in config_dict.keys():
            config_dict["query"] = [Path(os.path.join(QUERY_DIR, f)).resolve().stem for f in os.listdir(QUERY_DIR) if f.endswith(".sparql")]

        if "instance" not in config_dict.keys():
            config_dict["instance"] = list(map(str, range(CONFIG_GEN["n_query_instances"])))
            
        if "attempt" not in config_dict.keys():
            config_dict["attempt"] = list(map(str, range(CONFIG_EVAL["n_attempts"])))
    
    SNAKEMAKE_CONFIGS = " ".join([f"{k}={','.join(v)}" for k, v in config_dict.items()])
    
    SNAKEMAKE_OPTS = ""
    
    if force:
        SNAKEMAKE_OPTS += " -F"
        

    SNAKEMAKE_OPTS += f" -p --cores {cores} --config {SNAKEMAKE_CONFIGS}"
    if rerun_incomplete: SNAKEMAKE_OPTS += " --rerun-incomplete"
    if dry_run: SNAKEMAKE_OPTS += " --dry-run"
    
    if touch:
        logger.info("Marking files as completed...")
        shutil.rmtree(".snakemake", ignore_errors=True)
        SNAKEMAKE_OPTS += " --touch"

    # If in generate mode
    if clean is not None:
        logger.info("Cleaning...")
        ctx.invoke(wipe, configfile=configfile, level=clean)

    for batch in range(1, N_BATCH+1):
        logger.info(f"Generating instances for batch {batch}/{N_BATCH}...")
        if os.system(f"snakemake {SNAKEMAKE_OPTS} --snakefile {GENERATION_SNAKEFILE} --batch all={batch}/{N_BATCH}") != 0 : exit(1)

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
@click.option("--config", type=click.STRING, default=None, help='Params for snakemake file. Example: --config="engine=costfed attempt=0 query=q04 instance=7 batch=0"')
@click.option("--debug", is_flag=True, default=False)
@click.option("--clean", type=click.STRING, help="[all, model, benchmark] + db")
@click.option("--cores", type=click.INT, default=1, help="The number of cores used allocated. -1 if use all cores.")
@click.option("--rerun-incomplete", is_flag=True, default=False)
@click.option("--touch", is_flag=True, default=False)
@click.option("--no-cache", is_flag=True, default=False)
@click.option("--noexec", is_flag=True, default=False)
@click.option("--dry-run", is_flag=True, default=False)
@click.option("--force", is_flag=True, default=False)
@click.pass_context
def evaluate(ctx: click.Context, configfile, config, debug, clean, cores, rerun_incomplete, touch, no_cache, noexec, dry_run, force):

    CONFIG = load_config(configfile)
    CONFIG_GEN = CONFIG["generation"]
    WORK_DIR = CONFIG_GEN["workdir"]
    BENCH_DIR = f"{WORK_DIR}/benchmark/evaluation"

    EVALUATION_SNAKEFILE=f"snakemake/evaluate.smk"
    SINGLE_QUERY_MODE = False
    SNAKEMAKE_CONFIG_MATCHER = None

    N_BATCH = CONFIG_GEN["n_batch"]

    CONFIG_EVAL = CONFIG["evaluation"]
    QUERY_DIR = f"{WORK_DIR}/queries"

    config_dict = {
        "configfile": [configfile],
    }
    
    if config is not None:
        config = config.strip()
        SNAKEMAKE_CONFIG_MATCHER = re.match(r"(\w+\=\w+(,\w+)*(\s+)?)+", config)
        if SNAKEMAKE_CONFIG_MATCHER is None:
            raise RuntimeError(f"Syntax error: config option should be 'name1=value1 name2=value2'")
        
        for c in config.split():
            k, v = c.split("=")

            if k in ["debug", "explain"]:
                v = eval(v)

            config_dict[k] = v.split(",")

        if "batch" not in config_dict.keys():
            config_dict["batch"] = list(map(str, range(N_BATCH)))

        if "query" not in config_dict.keys():
            config_dict["query"] = [Path(os.path.join(QUERY_DIR, f)).resolve().stem for f in os.listdir(QUERY_DIR) if f.endswith(".sparql")]

        if "instance" not in config_dict.keys():
            config_dict["instance"] = list(map(str, range(CONFIG_GEN["n_query_instances"])))
            
        if "attempt" not in config_dict.keys():
            config_dict["attempt"] = list(map(str, range(CONFIG_EVAL["n_attempts"])))
    
        SNAKEMAKE_CONFIGS = " ".join([f"{k}={','.join(v)}" for k, v in config_dict.items()])
        SINGLE_QUERY_MODE = eval(config_dict["debug"]) if config_dict.get("debug") is not None else False
    
    WORKFLOW_DIR = f"{WORK_DIR}/rulegraph"
    os.makedirs(name=WORKFLOW_DIR, exist_ok=True)
    
    SNAKEMAKE_OPTS = ""

    if force:
        SNAKEMAKE_OPTS += " -F"

    if cores == -1: cores = "all"
    SNAKEMAKE_OPTS += f" -p --cores {cores} --config {SNAKEMAKE_CONFIGS}"
    if rerun_incomplete: SNAKEMAKE_OPTS += " --rerun-incomplete"
    
    if no_cache:
        shutil.rmtree(".snakemake")
    
    if touch:
        logger.info("Marking files as completed...")
        shutil.rmtree(".snakemake", ignore_errors=True)
        SNAKEMAKE_OPTS += " --touch"
        
    if dry_run:
        SNAKEMAKE_OPTS += " --dry-run"

    # if in evaluate mode
    if clean is not None :
        if len(config_dict) > 0:
            keys, values = zip(*config_dict.items())
            for comb in product(*values):
                path_dict = dict(zip(keys, comb))
                if SINGLE_QUERY_MODE:
                    shutil.rmtree(f"{BENCH_DIR}/{path_dict['engine']}/{path_dict['query']}/instance_{path_dict['instance']}/batch_{path_dict['batch']}/debug/", ignore_errors=True)
                else:
                    shutil.rmtree(f"{BENCH_DIR}/{path_dict['engine']}/{path_dict['query']}/instance_{path_dict['instance']}/batch_{path_dict['batch']}/attempt_{path_dict['attempt']}/", ignore_errors=True)
        if clean == "all":
            shutil.rmtree(f"{WORK_DIR}/benchmark/evaluation", ignore_errors=True)
        elif clean == "metrics":
            os.system(f"rm {WORK_DIR}/benchmark/evaluation/*.csv")
    
    batch_size = len(config_dict["batch"]) if "batch" in config_dict.keys() else N_BATCH
    
    for batch in range(1, batch_size+1):
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
        if os.system(f"docker compose -f {SPARQL_COMPOSE_FILE} down --remove-orphans --volumes") != 0 : exit(1)  
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
        if os.system(f"docker compose -f {SPARQL_COMPOSE_FILE} down --remove-orphans --volumes") != 0 : exit(1)  
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

@cli.command()
@click.argument("configfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
def setup(configfile):
    # Launch virtuoso
    config = load_config(configfile)
    virtuoso_config = config["generation"]["virtuoso"]
    virtuoso_endpoint = virtuoso_config["default_endpoint"]

    try:
        while requests.get(virtuoso_endpoint).status_code != 200:
            print("Virtuoso is not running. Please turn it on manually.")
            time.sleep(5)
    except: pass

    # Launch the proxy server
    proxy_config = config["evaluation"]["proxy"]
    proxy_endpoint = proxy_config["endpoint"]
    proxy_compose_file = proxy_config["compose_file"]
    proxy_service_name = proxy_config["service_name"]

    try:
        while requests.get(proxy_endpoint).status_code != 200:
            print("Proxy server is not running. Please turn it on manually.")
            time.sleep(5)
    except: pass

    click.echo("Setup completed!")

if __name__ == "__main__":
    cli()