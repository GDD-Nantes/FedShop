# Import part
from io import BytesIO
import json
import os
import re
import time
import click
import glob
import subprocess
import pandas as pd
import numpy as np
from pathlib import Path

import sys

from rdflib import URIRef
import requests
sys.path.append(str(os.path.join(Path(__file__).parent.parent)))

from utils import load_config, rsfb_logger, write_empty_stats, write_empty_result
from query import write_query

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
    os.chdir(config["evaluation"]["engines"]["arq"]["dir"])
    os.system("sh setup.sh")
    os.chdir(current_pwd)
    
    # # Start fuseki server
    # compose_file = config["evaluation"]["engines"]["arq"]["compose_file"]
    # os.system(f"docker-compose -f {compose_file} up -d --force-recreate jena-fuseki")

@cli.command()
@click.argument("eval-config", type=click.Path(exists=True, file_okay=True, dir_okay=True))
@click.argument("engine-config", type=click.Path(exists=False, file_okay=True, dir_okay=True)) # Engine config is not needed
@click.argument("query", type=click.Path(exists=True, file_okay=True, dir_okay=True))
@click.option("--out-result", type=click.Path(exists=False, file_okay=True, dir_okay=True), default="/dev/null")
@click.option("--out-source-selection", type=click.Path(exists=False, file_okay=True, dir_okay=True), default="/dev/null")
@click.option("--query-plan", type=click.Path(exists=False, file_okay=True, dir_okay=True), default="/dev/null")
@click.option("--stats", type=click.Path(exists=False, file_okay=True, dir_okay=True), default="/dev/null")
@click.option("--force-source-selection", type=click.Path(exists=False, file_okay=True, dir_okay=True), default="")
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
    
    opt_source_selection_file = f"{Path(force_source_selection).parent}/{Path(force_source_selection).stem}.opt.csv"
    source_selection_df = pd.read_csv(opt_source_selection_file)
    
    config = load_config(eval_config)
    internal_endpoint_prefix=str(config["evaluation"]["engines"]["arq"]["internal_endpoint_prefix"])
    endpoint = re.search(r":(\d+)", config["generation"]["virtuoso"]["endpoints"][-1]).group(1)
    internal_endpoint_prefix=internal_endpoint_prefix.replace("8890", endpoint, 1)
    
    source_selection_combinations = source_selection_df \
        .applymap(lambda x: URIRef(f"{internal_endpoint_prefix}{x}").n3()) \
        .apply(lambda x: f"( {' '.join(x)} )", axis=1) \
        .to_list()
        
    values_clause_vars = [ f"?{col}" for col in source_selection_df.columns ]   
    values_clause = f"    VALUES ( {' '.join(values_clause_vars)} ) {{ {' '.join(source_selection_combinations)} }}\n"
    
    opt_source_selection_query_file = f"{Path(query).parent}/provenance.sparql.opt"
    service_query_file = f"{Path(query_plan).parent}/service.sparql"
    Path(service_query_file).touch()
    
    with    open(opt_source_selection_query_file, "r") as opt_source_selection_qfs, \
            open(query, "r") as query_fs:

        # Create SERVICE query
        query_text = query_fs.read()
        select_clause = re.search(r"(SELECT(.*)[\S\s]+WHERE)", query_text).group(1)
        
        lines = opt_source_selection_qfs.readlines()
        insert_idx = [ line_idx for line_idx, line in enumerate(lines) if "WHERE" in line ][0]
        lines.insert(insert_idx+1, values_clause)
        
        out_query_text = "".join(lines)
        out_query_text = re.sub(r"SELECT(.*)[\S\s]+WHERE", select_clause, out_query_text)
        out_query_text = re.sub(r"(regex|REGEX)\s*\(\s*(\?\w+)\s*,", r"\1(lcase(str(\2)),", out_query_text)
        out_query_text = re.sub(r"(#)*(FILTER\s*\(\!bound)", r"\2", out_query_text)
        out_query_text = re.sub(r"#*(DEFINE|OFFSET)", r"##\1", out_query_text)
        out_query_text = re.sub(r"#*(ORDER|LIMIT)", r"\1", out_query_text)
        out_query_text = re.sub("GRAPH", "SERVICE", out_query_text)
                
        # Write service query
        out_query_text = write_query(out_query_text, service_query_file)
            
        # Explain query
        with open(query_plan, "w") as query_plan_fs:
            arq = f'{config["evaluation"]["engines"]["arq"]["dir"]}/jena/bin/arq'
            explain_cmd = f"{arq} --explain --query {service_query_file}"
            
            logger.debug("==== ARQ EXPLAIN ====")
            logger.debug(explain_cmd)
            logger.debug("=====================")
            
            arq_proc = subprocess.run(explain_cmd, shell=True, capture_output=True)
            if arq_proc.returncode == 0:
                query_plan_fs.write(arq_proc.stderr.decode())
            else:
                raise RuntimeError("ARQ explain reported error!")
   
        # Execute results
        endpoint = config["evaluation"]["engines"]["arq"]["endpoint"]
        timeout = config["evaluation"]["timeout"]
        http_req = "N/A"
        exec_time = None
        source_selection_time = None
        planning_time = None
        
        arq = f'{config["evaluation"]["engines"]["arq"]["dir"]}/jena/bin/arq'
        exec_cmd = f"{arq} --query {service_query_file} --results=CSV --time"
            
        logger.debug("==== ARQ EXEC ====")
        logger.debug(exec_cmd)
        logger.debug("==================")
            
        try: 
            arq_proc = subprocess.run(exec_cmd, shell=True, capture_output=True, timeout=timeout)
            if arq_proc.returncode == 0:
                logger.info(f"{query} benchmarked sucessfully")
                result_df = pd.read_csv(BytesIO(arq_proc.stdout))
                                
                exec_time = float(re.search(r"Time: (\d+(\.\d+)?) sec.*", arq_proc.stderr.decode()).group(1)) * 1e3
                
                if result_df.empty:
                    logger.error(f"{query} yield no results!")
                    write_empty_result(out_result)
                    raise RuntimeError(f"{query} yield no results!")
                else:
                    # Some post processing
                    if "*" in select_clause:
                        result_df = result_df[[col for col in result_df.columns if "bgp" not in col]]
                    result_df.to_csv(out_result, index=False)
            else:
                logger.error(f"{query} reported error")    
                write_empty_result(out_result)
                write_empty_stats(stats, "error_runtime")                  
                # raise RuntimeError(f"{query} reported error")
                
        except subprocess.TimeoutExpired: 
            logger.exception(f"{query} timed out!")
            write_empty_stats(stats, "timeout")
            write_empty_result(out_result)            
            
        # Write stats
        logger.info(f"Writing stats to {stats}")
        with open(stats, "w") as stats_fs:
            stats_fs.write("query,engine,instance,batch,attempt,exec_time,ask,source_selection_time,planning_time\n")
            basicInfos = re.match(r".*/(\w+)/(q\w+)/instance_(\d+)/batch_(\d+)/attempt_(\d+)/stats.csv", stats)
            engine = basicInfos.group(1)
            queryName = basicInfos.group(2)
            instance = basicInfos.group(3)
            batch = basicInfos.group(4)
            attempt = basicInfos.group(5)
            stats_fs.write(",".join([
                queryName, engine, instance, batch, attempt, 
                str(exec_time), str(http_req), str(source_selection_time), str(planning_time)
            ])+"\n") 
        
        # Write output source selection
        os.system(f"cp {force_source_selection} {out_source_selection}")

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
    os.system(f"cp {infile} {outfile}")

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
    os.system(f"cp {infile} {outfile}")
    
@cli.command()
@click.argument("datafiles", type=click.Path(exists=True, dir_okay=False, file_okay=True), nargs=-1)
@click.argument("outfile", type=click.Path(exists=False, file_okay=True, dir_okay=False))
@click.argument("eval-config", type=click.Path(exists=True, dir_okay=False, file_okay=True))
@click.option("--endpoint", type=str, default="http://localhost:8890/sparql", help="URL to a SPARQL endpoint")
@click.pass_context
def generate_config_file(ctx: click.Context, datafiles, outfile, eval_config, endpoint):
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