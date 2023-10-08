# Import part
from io import BytesIO, StringIO
import json
import os
import re
import shutil
import time
import click
import glob
import subprocess
import pandas as pd
import numpy as np
from pathlib import Path

import sys
import requests
sys.path.append(str(os.path.join(Path(__file__).parent.parent)))

from utils import create_stats, kill_process, load_config, rsfb_logger, str2n3
logger = rsfb_logger(Path(__file__).name)

import fedx

mvn_bin = shutil.which("mvn")

# How to use
# 1. Duplicate this file and rename the new file with <engine>.py
# 2. Implement all functions
# 3. Register the engine in config.yaml, under evaluation.engines section
# 
# Note: when you update the signature of any of these functions, you also have to update their signature in other engines

@click.group
def cli():
    pass

def __compile_fedup():
    if os.system(f"{mvn_bin} -Dmaven.repo.local={os.getcwd()}/.m2/repository clean install -Dmaven.test.skip=true") != 0:
        raise RuntimeError("Could not compile HiBISCuS!")

@cli.command()
@click.argument("eval-config", type=click.Path(exists=True, file_okay=True, dir_okay=True))
@click.pass_context
def prerequisites(ctx: click.Context, eval_config):
    """Obtain prerequisite artifact for engine, e.g, compile binaries, setup dependencies, etc.

    Args:
        eval_config (_type_): _description_
    """
    config = load_config(eval_config)
    app_dir = config["evaluation"]["engines"]["hibiscus"]["dir"]
    
    os.chdir(app_dir)
    
    if os.system('conda info --envs | grep "fedupxp"') != 0:
        if os.system("conda env create -f environment.yml") != 0:
            raise RuntimeError("Could not setup Python environment for HiBISCuS")
    
    if not os.path.exists("apache-jena-4.7.0"):
        if os.system("wget https://archive.apache.org/dist/jena/binaries/apache-jena-4.7.0.tar.gz") != 0:
            raise RuntimeError("Could not download Apache Jena 4.7.0")
        else:
            os.system("tar -zxf apache-jena-4.7.0.tar.gz")
    
    __compile_fedup()

@cli.command()
@click.argument("eval-config", type=click.Path(exists=True, file_okay=True, dir_okay=True))
@click.argument("engine-config", type=click.Path(exists=True, file_okay=True, dir_okay=True))
@click.argument("query", type=click.Path(exists=True, file_okay=True, dir_okay=True))
@click.option("--out-result", type=click.Path(exists=False, file_okay=True, dir_okay=True), default="/dev/null")
@click.option("--out-source-selection", type=click.Path(exists=False, file_okay=True, dir_okay=True), default="/dev/null")
@click.option("--query-plan", type=click.Path(exists=False, file_okay=True, dir_okay=True), default="/dev/null")
@click.option("--stats", type=click.Path(exists=False, file_okay=True, dir_okay=True), default="/dev/null")
@click.option("--force-source-selection", type=click.Path(exists=False, file_okay=True, dir_okay=True), default="")
@click.option("--batch-id", type=click.INT, default=-1)
@click.option("--noexec", is_flag=True, default=False)
@click.option("--approach", type=click.Choice(["h0", "id", "id-optimal"]), default="h0")
@click.pass_context
def run_benchmark(ctx: click.Context, eval_config, engine_config, query, out_result, out_source_selection, query_plan, stats, force_source_selection, batch_id, noexec, approach):
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
    
    if noexec: 
        Path(out_result).touch()
        Path(out_source_selection).touch()
        return
    
    config = load_config(eval_config)
    proxy_server = config["evaluation"]["proxy"]["endpoint"]
    app_dir = config["evaluation"]["engines"]["hibiscus"]["dir"]

    olddir = Path(os.getcwd()).absolute()
    
    # Reset the proxy stats
    if requests.get(proxy_server + "reset").status_code != 200:
        raise RuntimeError("Could not reset statistics on proxy!")
    
    # Get env-specific executable for python
    get_python_proc = subprocess.run('conda info --envs | grep "fedupxp"', shell=True, capture_output=True)
    if get_python_proc.returncode != 0:
        raise RuntimeError("Could not get the correct executable for HiBISCuS")
    
    env_dir = get_python_proc.stdout.decode()
    snakemake_bin = os.path.join(re.sub(r"(.*)\s+(\*)?\s+(.*)", r"\3", env_dir).strip(), "bin", "snakemake")
    
    # Run engine for one query
    timeout = int(config["evaluation"]["timeout"])
    failed_reason = None
    
    query_name = re.search(r"(q\d+)", query).group(1)
    output_file = f"output/fedshop/fedup-{approach}/{query_name}.1.csv"

    elapsed_time = None

    try:        
        # Pre-process queries (malformed queries error, etc...)
        query_file = app_dir + f"/queries/fedshop/{query_name}.sparql"
        shutil.rmtree(Path(query_file).parent, ignore_errors=True)
        Path(query_file).parent.mkdir(parents=True, exist_ok=True)
        with open(query, "r") as in_qfs, open(query_file, "w") as out_qfs:
            query_lines = in_qfs.readlines()
            for line in query_lines:         
                # Remove all comments
                if line.strip().startswith("#"):
                    query_lines.pop(query_lines.index(line))
            
            query_text = "".join(query_lines)
            out_qfs.write(query_text)
        
        # Run Snakemake 
        virtuoso_port = re.search(r":(\d+)", proxy_server).group(1)
        os.chdir(app_dir)
        
        # Clean run Snakemake
        cmd = f'rm -rf .snakemake/ output/ && {snakemake_bin} -C virtuoso_port={virtuoso_port} workload="[fedshop]" approach="[hibiscus]" timeout={timeout} -c1'
        logger.debug(cmd)        
        
        start_time = time.time()
        os.system(cmd)
        end_time = time.time()
        elapsed_time = (end_time - start_time)
        
        logger.debug(f"FedUp terminated in {elapsed_time}s!")

    finally:
        # =============
        # Handle every edge cases
        # =============
        
        # No output 
        if not os.path.exists(output_file):
            # Kill all HiBISCuS instance
            if os.system("lsof -t -i :8080 -sTCP:LISTEN | xargs -r kill -9") != 0:
                logger.warn("FedUp is either already killed (GOOD) or cannot be killed (BAD)")
            raise RuntimeError("FedUp did not produce any output!")
                
        # Write output
        output = pd.read_csv(output_file)
        status = output["status"].item()
        
        exec_time = output["executionTime"].item()
        
        if status == "OK":
            # Write source assignment
            source_assignments_str = str(output["assignments"].item()).replace("'", '"').replace("\\n", "")
            source_assignments_in = json.loads(source_assignments_str)
            tpAliases = json.loads(str(output["tpAliases"].item()).replace("'", '"').replace("\\n", ""))

            prefix_cache = os.path.join("../../", Path(query).parent, "prefix_cache.json")
            comp = json.load(open(os.path.join(Path(prefix_cache).parent, "provenance.sparql.comp"), "r"))
            prefix2alias = json.load(open(prefix_cache, "r"))

            inv_comp = {f"{' '.join(v)}": k for k, v in comp.items()}
            records = []
                        
            for sa in source_assignments_in:
                record = {}
                for tp, source in sa.items():
                    alias = tpAliases[tp]
                    triple = extract_triple(alias, prefix2alias)
                    record[inv_comp.get(triple)] = source
                    source = re.sub(r"http://(www\.\w+\.fr)/", r"\1", source)
                    
                records.append(record)
                
            source_assignments_df = pd.DataFrame.from_records(records)
            source_assignments_df = source_assignments_df.reindex(sorted(source_assignments_df.columns, key=lambda x: int(x[2:])), axis=1)
            source_assignments_df.to_csv("../../" + out_source_selection, index=False)
            
            # Write results
            solutions_str = str(output["solutions"].item()).replace('"', '\\"').replace("'[", '"[').replace("]'", ']"').replace("\\'", "'").replace('""', '"')
            solutions = json.loads(solutions_str)
            #pd.DataFrame(solutions).to_csv("../../" + out_result, header=False, index=False)
            with open("../../" + out_result, "w") as out_result_fs:
                out_result_fs.write("\n".join(solutions))
                
            # Write metrics
            source_selection_time = output["sourceSelectionTime"].item()
            source_selection_time_file =  "../../" + str(Path(out_result).parent) + "/source_selection_time.txt"
            with open(source_selection_time_file, "w") as source_selection_time_fs:
                source_selection_time_fs.write(str(source_selection_time))
            
            exec_time_file = "../../" + str(Path(out_result).parent) + "/exec_time.txt"
            with open(exec_time_file, "w") as exec_time_file_fs:
                exec_time_file_fs.write(str(exec_time + source_selection_time))
                
        elif status == "TIMEOUT":  
            
            # Elapsed time too short         
            if elapsed_time < timeout:
                # __compile_fedup()
                raise RuntimeError(f"FedUp terminated in {elapsed_time}s which is less than {timeout}s!")
            
            failed_reason = "timeout"
            
            # Path("../../" + out_result).touch()
            # Path("../../" + out_source_selection).touch()

            
        elif status == "ERROR":
            raise RuntimeError("Something went wrong while running benchmark!")
            failed_reason = "error"  
            Path("../../" + out_result).touch()
            Path("../../" + out_source_selection).touch()
        
        # if os.system("lsof -t -i :8080 -sTCP:LISTEN | xargs -r kill -9") != 0:
        #     raise RuntimeError("Could not kill HiBISCuS after execution!")
        
        # Write stats
        os.chdir(olddir)
        if stats != "/dev/null":            
            # Write proxy stats
            proxy_stats = json.loads(requests.get(proxy_server + "get-stats").text)
            
            with open(f"{Path(stats).parent}/http_req.txt", "w") as http_req_fs:
                http_req = proxy_stats["NB_HTTP_REQ"]
                http_req_fs.write(str(http_req))
                
            with open(f"{Path(stats).parent}/ask.txt", "w") as http_ask_fs:
                http_ask = proxy_stats["NB_ASK"]
                http_ask_fs.write(str(http_ask))
                
            with open(f"{Path(stats).parent}/data_transfer.txt", "w") as data_transfer_fs:
                data_transfer = proxy_stats["DATA_TRANSFER"]
                data_transfer_fs.write(str(data_transfer))
        
            logger.info(f"Writing stats to {stats}")
            create_stats(stats, failed_reason)

def extract_triple(x, prefix2alias):
    fedx_pattern = r"StatementPattern\s+(\(new scope\)\s+)?Var\s+\((name=\w+,\s+value=(.*),\s+anonymous|name=(\w+))\)\s+Var\s+\((name=\w+,\s+value=(.*),\s+anonymous|name=(\w+))\)\s+Var\s+\((name=\w+,\s+value=(.*),\s+anonymous|name=(\w+))\)"
    match = re.match(fedx_pattern, x)
        
    s = match.group(3)
    if s is None: s = f"?{match.group(4)}"
        
    p = match.group(6)
    if p is None: p = f"?{match.group(7)}"
        
    o = match.group(9) 
    if o is None: o = f"?{match.group(10)}"
        
    result = " ".join([s, p, o])
                
    for prefix, alias in prefix2alias.items():
        result = result.replace(prefix, f"{alias}:")
            
    if s.startswith("http"):
        result = result.replace(s, str2n3(s))
            
    if o.startswith("http"):
        result = result.replace(o, str2n3(o))
        
    return result 

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
    ctx.invoke(fedx.transform_results, infile=infile, outfile=outfile)

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
    #ctx.invoke(fedx.transform_provenance, infile=infile, outfile=outfile, prefix_cache=prefix_cache)
    shutil.copyfile(infile, outfile)

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
    
    config = load_config(eval_config)
    app_dir = config["evaluation"]["engines"]["hibiscus"]["dir"]
    
    olddir = Path(os.getcwd()).absolute()
    os.chdir(app_dir)
    
    # Update the endpoints.txt
    sources = set()
    for datafile in datafiles:
        with open(f"../../{datafile}", "r") as file:
            line = file.readline()
            source = line.rsplit()[-2]
            source = source[1:-1]
            sources.add(source)
    
    endpoints_file = "config/fedshop/endpoints.txt"
    with open(endpoints_file, "w") as epf:
        #virtuoso_endpoint = config["generation"]["virtuoso"]["endpoints"]
        proxy_server = config["evaluation"]["proxy"]["endpoint"] + "sparql"
        endpoints = [ proxy_server + f"?default-graph-uri={source}" for source in sources ]
        epf.write('\n'.join(endpoints))
    
    # Create hibiscus summary
    summary_file = f"summaries/fedshop/batch{batch_id}/hibiscus.n3"
    batch_file = f"datasets/fedshop/fedup-batch{batch_id}.nq"
    
    # Generate data if not exists
    if not os.path.exists(batch_file):
        Path(batch_file).parent.mkdir(parents=True, exist_ok=True)
        if os.system(f"cat {' '.join(datafiles)} > {batch_file}") != 0:
            raise RuntimeError("Could not generate batch file for HiBISCuS")
    
    # Generate summary files if not exists
    if not os.path.exists(summary_file):    
        Path(summary_file).parent.mkdir(parents=True, exist_ok=True)
        cmd = f'{mvn_bin} exec:java -Dmaven.repo.local={os.getcwd()}/.m2/repository -Dexec.mainClass="fr.univnantes.gdd.fedup.startup.GenerateSummaries" -Dexec.args="hibiscus -e {endpoints_file} -o {summary_file}" -pl fedup'
        logger.debug(cmd)
        if os.system(cmd) != 0:
            raise RuntimeError("Could not generate summary file for HiBISCuS")
        
    # Update the relevant configfiles
    hibiscus_props = open("config/fedshop/hibiscus.props").read()
    hibiscus_props = re.sub(r"batch\d+", f"batch{batch_id}", hibiscus_props)
    with open("config/fedshop/hibiscus.props", "w") as h0fs:
        h0fs.write(hibiscus_props)
    
    os.chdir(olddir)
    Path(outfile).parent.mkdir(parents=True, exist_ok=True)
    with open(outfile, "w") as ofs:
        ofs.write(hibiscus_props)
    
if __name__ == "__main__":
    cli()