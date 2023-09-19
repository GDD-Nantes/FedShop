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
    if os.system(f"{mvn_bin} -Dmaven.repo.local={os.getcwd()}/.m2/repository clean install") != 0:
        raise RuntimeError("Could not compile FedUP!")

@cli.command()
@click.argument("eval-config", type=click.Path(exists=True, file_okay=True, dir_okay=True))
@click.pass_context
def prerequisites(ctx: click.Context, eval_config):
    """Obtain prerequisite artifact for engine, e.g, compile binaries, setup dependencies, etc.

    Args:
        eval_config (_type_): _description_
    """
    config = load_config(eval_config)
    app_dir = config["evaluation"]["engines"]["fedup_h0"]["dir"]
    
    os.chdir(app_dir)
    
    if os.system('conda info --envs | grep "fedupxp"') != 0:
        if os.system("conda env create -f environment.yml") != 0:
            raise RuntimeError("Could not setup Python environment for FedUP")
    
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
    app_dir = config["evaluation"]["engines"]["fedup_h0"]["dir"]

    olddir = Path(os.getcwd()).absolute()
    
    # Reset the proxy stats
    if requests.get(proxy_server + "reset").status_code != 200:
        raise RuntimeError("Could not reset statistics on proxy!")
    
    # Get env-specific executable for python
    get_python_proc = subprocess.run('conda info --envs | grep "fedupxp"', shell=True, capture_output=True)
    if get_python_proc.returncode != 0:
        raise RuntimeError("Could not get the correct executable for FedUP")
    
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
        cmd = f'rm -rf .snakemake/ output/ && {snakemake_bin} -C virtuoso_port={virtuoso_port} workload="[fedshop]" approach="[fedup-{approach}]" timeout={timeout} -c1'
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
            # Kill all FedUP instance
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
            
            source_assignments_out = {}
            for sa in source_assignments_in:
                for tp, source in sa.items():
                    alias = tpAliases[tp]
                    source = re.sub(r"http://(www\.\w+\.fr)/", r"\1", source)
                    source = f"StatementSource (id=sparql_{source}_, type=REMOTE)"
                    if source_assignments_out.get(alias) is None:
                        source_assignments_out[alias] = []
                    
                    if source not in source_assignments_out[alias]:
                        source_assignments_out[alias].append(source)
            
            source_assignments_out = {k: str(v) for k, v in source_assignments_out.items()}
            source_assignments_df = pd.DataFrame.from_dict(source_assignments_out, orient="index").reset_index()
            source_assignments_df.columns = ["triple", "source_selection"]
            
            source_assignments_df.to_csv("../../" + out_source_selection, index=False)
            
            # Write results
            solutions_str = str(output["solutions"].item()).replace('"', '\\"').replace("'[", '"[').replace("]'", ']"').replace("\\'", "'").replace('""', '"')
            solutions = json.loads(solutions_str)
            #pd.DataFrame(solutions).to_csv("../../" + out_result, header=False, index=False)
            with open("../../" + out_result, "w") as out_result_fs:
                out_result_fs.write("\n".join(set(solutions)))
                
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
            
            Path("../../" + out_result).touch()
            Path("../../" + out_source_selection).touch()

            
        elif status == "ERROR":
            raise RuntimeError("Something went wrong while running benchmark!")
            failed_reason = "error"  
            Path("../../" + out_result).touch()
            Path("../../" + out_source_selection).touch()
        
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
    ctx.invoke(fedx.transform_provenance, infile=infile, outfile=outfile, prefix_cache=prefix_cache)

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
    app_dir = config["evaluation"]["engines"]["fedup_h0"]["dir"]
    
    olddir = Path(os.getcwd()).absolute()
    os.chdir(app_dir)
    
    # Create fedup summary
    summary_file = f"summaries/fedshop/batch{batch_id}/fedup-h0.nq"
    batch_file = f"datasets/fedshop/fedup-batch{batch_id}.nq"
    modulo = 0
    
    # Generate data if not exists
    if not os.path.exists(batch_file):
        Path(batch_file).parent.mkdir(parents=True, exist_ok=True)
        if os.system(f"cat {' '.join(datafiles)} > {batch_file}") != 0:
            raise RuntimeError("Could not generate batch file for FedUP")
    
    # Generate summary files if not exists
    if not os.path.exists(summary_file):    
        Path(summary_file).parent.mkdir(parents=True, exist_ok=True)
        cmd = f'{mvn_bin} exec:java -Dmaven.repo.local={os.getcwd()}/.m2/repository -Dexec.mainClass="fr.univnantes.gdd.fedup.startup.GenerateSummaries" -Dexec.args="fedup -d {batch_file} -o {summary_file} -m {modulo}" -pl fedup'
        logger.debug(cmd)
        if os.system(cmd) != 0:
            raise RuntimeError("Could not generate summary file for FedUP")
                
    # Load the summaries into Apache Jena
    apache_summary_file = f"summaries/fedshop/batch{batch_id}/fedup-h0"
    if not os.path.exists(apache_summary_file):
        Path(apache_summary_file).parent.mkdir(parents=True, exist_ok=True)
        if os.system(f'./apache-jena-4.7.0/bin/tdb2.xloader --loc {apache_summary_file} {summary_file}') != 0:
            raise RuntimeError("Could not load summary file into Apache Jena")
    
    # Load the ideal summaries into Apache Jena
    apache_summary_id = f"summaries/fedshop/batch{batch_id}/fedup-id"
    if not os.path.exists(apache_summary_id):
        Path(apache_summary_id).parent.mkdir(parents=True, exist_ok=True)
        if os.system(f'./apache-jena-4.7.0/bin/tdb2.xloader --loc {apache_summary_id} {batch_file}') != 0:
            raise RuntimeError("Could not load ideal summary file into Apache Jena")
        
    # Update the endpoints.txt
    sources = set()
    for datafile in datafiles:
        with open(f"../../{datafile}", "r") as file:
            line = file.readline()
            source = line.rsplit()[-2]
            source = source[1:-1]
            sources.add(source)
    
    with open("config/fedshop/endpoints.txt", "w") as epf:
        #virtuoso_endpoint = config["generation"]["virtuoso"]["endpoints"]
        proxy_server = config["evaluation"]["proxy"]["endpoint"] + "sparql"
        endpoints = [ proxy_server + f"?default-graph-uri={source}" for source in sources ]
        epf.write('\n'.join(endpoints))
        
    # Update the relevant configfiles
    fedup_h0_props = open("config/fedshop/fedup-h0.props").read()
    fedup_h0_props = re.sub(r"fedup\.summary=\.\./summaries/fedshop/batch\d+/fedup-h0", f"fedup.summary=../summaries/fedshop/batch{batch_id}/fedup-h0", fedup_h0_props)
    with open("config/fedshop/fedup-h0.props", "w") as h0fs:
        h0fs.write(fedup_h0_props)
    
    fedup_id_props = open("config/fedshop/fedup-id.props").read()
    fedup_id_props = re.sub(r"fedup\.summary=\.\./summaries/fedshop/batch\d+/fedup-id", f"fedup.summary=../summaries/fedshop/batch{batch_id}/fedup-id", fedup_id_props)
    with open("config/fedshop/fedup-id.props", "w") as idfs:
        idfs.write(fedup_id_props)
        
    fedup_id_opt_props = open("config/fedshop/fedup-id-optimal.props").read()
    fedup_id_opt_props = re.sub(r"fedup\.summary=\.\./summaries/fedshop/batch\d+/fedup-id", f"fedup.summary=../summaries/fedshop/batch{batch_id}/fedup-id", fedup_id_opt_props)
    with open("config/fedshop/fedup-id-optimal.props", "w") as id_opt_fs:
        id_opt_fs.write(fedup_id_opt_props)
    
    os.chdir(olddir)
    Path(outfile).parent.mkdir(parents=True, exist_ok=True)
    with open(outfile, "w") as ofs:
        ofs.write(fedup_h0_props)
    
if __name__ == "__main__":
    cli()