# Import part
from io import BytesIO
import json
import os
import random
import re
import shutil
import tempfile
import time
import click
import glob
import subprocess
import pandas as pd
import numpy as np
from pathlib import Path
import signal

import sys

from rdflib import URIRef
import requests
from tqdm import tqdm
sys.path.append(str(os.path.join(Path(__file__).parent.parent)))

from utils import check_container_status, load_config, rsfb_logger, wait_for_container, create_stats, write_empty_result
from query import write_query, exec_query_on_endpoint

logger = rsfb_logger(Path(__file__).name)

import fedx

# How to use
# 1. Duplicate this file and rename the new file with <engine>.py
# 2. Implement all functions
# 3. Register the engine in config.yaml, under evaluation.engines section
# 
# Note: when you update the signature of any of these functions, you also have to update their signature in other engines

@click.group
def cli():
    pass

@cli.command()
@click.argument("eval-config", type=click.Path(exists=True, file_okay=True, dir_okay=True))
@click.pass_context
def prerequisites(ctx: click.Context, eval_config):
    """Obtain prerequisite artifact for engine, e.g, compile binaries, setup dependencies, etc.

    Args:
        eval_config (_type_): _description_
    """
    config = load_config(eval_config)

    # Download and install Jena
    current_pwd = os.getcwd()
    os.chdir(config["evaluation"]["engines"]["ideal"]["dir"])
    os.system("sh setup.sh")
    os.chdir(current_pwd)
    
    # Start fuseki server
    compose_file = config["evaluation"]["engines"]["ideal"]["compose_file"]
    service_name = config["evaluation"]["engines"]["ideal"]["service_name"]
    container_name = config["evaluation"]["engines"]["ideal"]["container_name"]
    if check_container_status(compose_file, service_name, container_name) != "running":
        if os.system(f"docker-compose -f {compose_file} up -d --force-recreate jena-fuseki") != 0:
            raise RuntimeError("Could not launch Jena server...")
        
    wait_for_container(config["generation"]["virtuoso"]["endpoints"][-1], "/dev/null", logger)
    #ctx.invoke(warmup, eval_config=eval_config)
    
@cli.command()
@click.argument("eval-config", type=click.Path(exists=True, file_okay=True, dir_okay=True))
@click.option("--engine-config", type=click.Path(exists=False, file_okay=True, dir_okay=True), default="/dev/null") # Engine config is not needed
@click.option("--repeat", type=click.INT, default=1)
@click.option("--batch-id", type=click.INT, default=-1)
@click.pass_context
def warmup(ctx: click.Context, eval_config, engine_config, repeat, batch_id):
    # Warm up the server
    config = load_config(eval_config) 
    queries = glob.glob("experiments/bsbm/benchmark/generation/q*/instance*/injected.sparql")
    random.shuffle(queries)
    for query in tqdm(queries):
        for batch_id in range(config["generation"]["n_batch"]):
            for _ in range(repeat):
                ctx.invoke(run_benchmark, eval_config=eval_config, engine_config=engine_config, query=query, batch_id=batch_id)

def __create_service_query(config, query, query_plan, force_source_selection):
    opt_source_selection_file = f"{Path(force_source_selection).parent}/{Path(force_source_selection).stem}.csv"
    source_selection_df = pd.read_csv(opt_source_selection_file)
    
    internal_endpoint_prefix=str(config["evaluation"]["engines"]["ideal"]["internal_endpoint_prefix"])
    port = re.search(r":(\d+)", config["generation"]["virtuoso"]["endpoints"][-1]).group(1)
    
    internal_endpoint_prefix=internal_endpoint_prefix.replace("localhost", "host.docker.internal")
    internal_endpoint_prefix=internal_endpoint_prefix.replace("8890", port, 1)
    
    values_clauses = []
    
    for bgp in source_selection_df.columns:
        source_selection_combinations = source_selection_df[bgp] \
            .dropna() \
            .drop_duplicates() \
            .apply(lambda x: URIRef(f"{internal_endpoint_prefix}{x}").n3()) \
            .to_list()
        
        if len(source_selection_combinations) == 0:
            source_selection_combinations = [ URIRef(f"{internal_endpoint_prefix}nan").n3() ]    
        
        values_clauses.append(f"    VALUES ?{bgp} {{ {' '.join(source_selection_combinations)} }}\n")
    
    opt_source_selection_query_file = f"{Path(query).parent}/provenance.sparql"
    service_query_file = f"{Path(query_plan).parent}/service.sparql"
    if query_plan == "/dev/null":
        service_query_file = "/tmp/service.sparql"
    Path(service_query_file).touch()
    
    with    open(opt_source_selection_query_file, "r") as opt_source_selection_qfs, \
            open(query, "r") as query_fs:

        # Create SERVICE query
        query_text = query_fs.read()
        select_clause = re.search(r"(SELECT(.*)[\S\s]+WHERE)", query_text).group(1)
        
        lines = opt_source_selection_qfs.readlines()
        insert_idx = [ line_idx for line_idx, line in enumerate(lines) if "WHERE" in line ][0]
        for i, values_clause in enumerate(values_clauses):
            lines.insert(insert_idx+i+1, values_clause)
                
        out_query_text = "".join(lines)
        out_query_text = re.sub(r"SELECT(.*)[\S\s]+WHERE", select_clause, out_query_text)
        out_query_text = re.sub(r"(regex|REGEX)\s*\(\s*(\?\w+)\s*,", r"\1(lcase(str(\2)),", out_query_text)
        out_query_text = re.sub(r"(#)*(FILTER\s*\(\!bound)", r"\2", out_query_text)
        out_query_text = re.sub(r"#*(DEFINE|OFFSET)", r"##\1", out_query_text)
        out_query_text = re.sub(r"#*(ORDER|LIMIT)", r"\1", out_query_text)
        out_query_text = re.sub("GRAPH", "SERVICE", out_query_text)
                        
        # Write service query
        out_query_text = write_query(out_query_text, service_query_file)
        return out_query_text

@cli.command()
@click.argument("eval-config", type=click.Path(exists=True, file_okay=True, dir_okay=True))
@click.argument("engine-config", type=click.Path(exists=False, file_okay=True, dir_okay=True)) # Engine config is not needed
@click.argument("query", type=click.Path(exists=True, file_okay=True, dir_okay=True))
@click.option("--out-result", type=click.Path(exists=False, file_okay=True, dir_okay=True), default="/dev/null")
@click.option("--out-source-selection", type=click.Path(exists=False, file_okay=True, dir_okay=True), default="/dev/null")
@click.option("--query-plan", type=click.Path(exists=False, file_okay=True, dir_okay=True), default="/dev/null")
@click.option("--stats", type=click.Path(exists=False, file_okay=True, dir_okay=True), default="/dev/null")
@click.option("--force-source-selection", type=click.Path(exists=False, file_okay=True, dir_okay=True))
@click.option("--batch-id", type=click.INT, default=-1)
@click.pass_context
def run_benchmark(ctx: click.Context, eval_config, engine_config, query, out_result, out_source_selection, query_plan, stats, force_source_selection, batch_id):
    """Execute the workload instance then its associated source selection query.
    
    Expected output:
    - results.txt: containing the results for the query
    - source_selection.txt: containing the source selection for the query
    - stats.csv: containing the execution time, http_requests for the query

    Args:
        ctx (click.Context): _description_
        eval_config (_type_): _description_
        engine_config (_type_): _description_
        query (_type_): _description_
        out_result (_type_): _description_
        out_source_selection (_type_): _description_
        query_plan (_type_): _description_
        stats (_type_): _description_
        force_source_selection (_type_): _description_
        batch_id (_type_): _description_
    """
    
    if force_source_selection is None:
        force_source_selection = f"{Path(query).parent}/batch_{batch_id}/provenance.csv"
        #logger.info(f"force_source_selection = {force_source_selection}")      
    
    config = load_config(eval_config)

    Path(query_plan).touch(exist_ok=True)
   
    # Execute results
    endpoint = config["evaluation"]["engines"]["ideal"]["endpoint"]
    timeout = config["evaluation"]["timeout"]
    http_req = "N/A"
    exec_time = None
    source_selection_time = None
    planning_time = None
    
    force_source_selection_df = pd.read_csv(force_source_selection).dropna(axis=1, how="all")
    response, result = None, None
    
    startTime = time.time()
    
    def timeout_handler(signum, frame):
        raise TimeoutError("Query timed out!")

    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(5)
    
    try:

        # In case there is only one source for all triple patterns, send the original query to Virtuoso.
        # In such case, it doesn't make sense to send a federated version of the query, i.e, with SERVICE clause.
        if len(force_source_selection_df) == 1 and force_source_selection_df.iloc[0, :].nunique() == 1 : 
            virtuoso_endpoint = config["generation"]["virtuoso"]["endpoints"][-1]
            default_graph = force_source_selection_df.iloc[0, :].unique().item()
            with open(query, "r") as qfs:
                query_text = qfs.read()
                response, result = exec_query_on_endpoint(query_text, virtuoso_endpoint, error_when_timeout=True, timeout=timeout, default_graph=default_graph)
        else:
            out_query_text = __create_service_query(config, query, query_plan, force_source_selection)        
            response, result = exec_query_on_endpoint(out_query_text, endpoint, error_when_timeout=True, timeout=timeout)
    
    except TimeoutError:
        logger.exception(f"{query} timed out!")
        create_stats(stats, "timeout")
        write_empty_result(out_result)
        Path(query_plan).touch()     
    else:
        endTime = time.time()
        exec_time = (endTime - startTime)*1e3
        
        with BytesIO(result) as header_stream, BytesIO(result) as data_stream: 
            header = header_stream.readline().decode().strip().replace('"', '').split(",")
            csvOut = pd.read_csv(data_stream, parse_dates=[h for h in header if "date" in h])                
            csvOut.to_csv(out_result, index=False)
            
        if csvOut.empty:
            raise RuntimeError("Query yields no results")
        
        # Write stats
        if stats != "/dev/null":
            with open(f"{Path(stats).parent}/exec_time.txt", "w") as exec_time_fs:
                exec_time_fs.write(str(exec_time))
            logger.info(f"Writing stats to {stats}")
            create_stats(stats)
    
     # Write output source selection
    shutil.copyfile(force_source_selection, out_source_selection)

@cli.command()
@click.argument("infile", type=click.Path(exists=False, file_okay=True, dir_okay=False))
@click.argument("outfile", type=click.Path(exists=False, file_okay=True, dir_okay=False))
@click.pass_context
def transform_results(ctx: click.Context, infile, outfile):
    """Transform the result from the engine's specific format to virtuoso csv format

    Args:
        ctx (click.Context): _description_
        infile (_type_): Path to engine result file
        outfile (_type_): Path to the csv file
    """
    shutil.copy(infile, outfile)

@cli.command()
@click.argument("infile", type=click.Path(exists=False, file_okay=True, dir_okay=False))
@click.argument("outfile", type=click.Path(exists=False, file_okay=True, dir_okay=False))
@click.argument("prefix-cache", type=click.Path(exists=False, file_okay=True, dir_okay=False))
@click.pass_context
def transform_provenance(ctx: click.Context, infile, outfile, prefix_cache):
    """Transform the source selection from engine's specific format to virtuoso csv format

    Args:
        ctx (click.Context): _description_
        infile (_type_): _description_
        outfile (_type_): _description_
        prefix_cache (_type_): _description_
    """
    shutil.copy(infile, outfile)
    
@cli.command()
@click.argument("datafiles", type=click.Path(exists=True, dir_okay=False, file_okay=True), nargs=-1)
@click.argument("outfile", type=click.Path(exists=False, file_okay=True, dir_okay=False))
@click.argument("eval-config", type=click.Path(exists=True, dir_okay=False, file_okay=True))
@click.argument("batch_id", type=click.INT)
@click.argument("endpoint", type=str)
@click.pass_context
def generate_config_file(ctx: click.Context, datafiles, outfile, eval_config, batch_id, endpoint):
    """Generate the config file for the engine

    Args:
        ctx (click.Context): _description_
        datafiles (_type_): _description_
        outfile (_type_): _description_
        endpoint (_type_): _description_
    """
    pass

if __name__ == "__main__":
    cli()