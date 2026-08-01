# Import part
from io import BytesIO
import json
import os
import re
import click
import time
import glob
import subprocess
import pandas as pd
import numpy as np
from pathlib import Path
import requests
from sklearn.preprocessing import LabelEncoder
import psutil

import sys

from ..utils import load_config, fedshop_logger, str2n3, create_stats
from . import fedx

logger = fedshop_logger(Path(__file__).name)

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

    app_config = load_config(eval_config)["evaluation"]["engines"]["costfed"]
    app = app_config["dir"]

    oldcwd = os.getcwd()
    os.chdir(Path(app))
    if os.system("mvn clean && mvn install dependency:copy-dependencies package") != 0:
        raise RuntimeError("Could not compile CostFed")
    os.chdir(oldcwd)

@cli.command()
@click.argument("eval-config", type=click.Path(exists=True, file_okay=True, dir_okay=True))
@click.argument("query", type=click.Path(exists=True, file_okay=True, dir_okay=True))
@click.option("--out-result", type=click.Path(exists=False, file_okay=True, dir_okay=True), default="/dev/null")
@click.option("--out-source-selection", type=click.Path(exists=False, file_okay=True, dir_okay=True), default="/dev/null")
@click.option("--query-plan", type=click.Path(exists=False, file_okay=True, dir_okay=True), default="/dev/null")
@click.option("--stats", type=click.Path(exists=False, file_okay=True, dir_okay=True), default="/dev/null")
@click.option("--batch-id", type=click.INT, default=-1)
@click.option("--noexec", is_flag=True, default=False)
@click.pass_context
def run_benchmark(ctx: click.Context, eval_config, query, out_result, out_source_selection, query_plan, stats, batch_id, noexec):
    """Execute the workload instance then its associated source selection query.
    
    Expected output:
    - results.txt: containing the results for the query
    - source_selection.txt: containing the source selection for the query
    - stats.csv: containing the execution time, http_requests for the query

    Args:
        ctx (click.Context): _description_
        eval_config (_type_): _description_
        query (_type_): _description_
        out_result (_type_): The file that holds results of the input query
        out_source_selection (_type_): The file that holds engine's source selection
        query_plan (_type_): _description_
        stats (_type_): _description_
        batch_id (_type_): _description_
    """
    
    config = load_config(eval_config)
    engine_dir = config["evaluation"]["engines"]["costfed"]["dir"]
    proxy_host = config["evaluation"]["proxy"]["host"]
    proxy_port = config["evaluation"]["proxy"]["port"]
    
    timeout = int(config["evaluation"]["timeout"])
    
    proxy_server = config["evaluation"]["proxy"]["endpoint"]
    endpoints_file = f"summaries/endpoints_batch{batch_id}.txt"
    
    # Reset the proxy stats
    if requests.get(proxy_server + "reset").status_code != 200:
        raise RuntimeError("Could not reset statistics on proxy!")

    oldcwd = os.getcwd()
    summary_file = f"summaries/sum_fedshop_batch{batch_id}.n3"   

    # Prepare args for semagrow
    Path(out_result).touch()
    Path(out_source_selection).touch()
    Path(query_plan).touch()

    timeoutCmd = f'timeout --signal=SIGKILL {timeout}' if timeout != 0 else ""
    cmd = f'{timeoutCmd} mvn exec:java -Dhttp.proxyHost="{proxy_host}" -Dhttp.proxyPort="{proxy_port}" -Dhttp.nonProxyHosts="" -Dexec.mainClass="org.aksw.simba.start.QueryEvaluation" -Dexec.args="costfed/costfed.props ../../{out_result} ../../{out_source_selection} ../../{query_plan} {timeout+10} {summary_file} ../../{query} {str(noexec).lower()} {endpoints_file}" -pl costfed'

    logger.debug("=== CostFed ===")
    logger.debug(cmd)
    logger.debug("============")

    os.chdir(Path(engine_dir))
    costfed_proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    os.chdir(oldcwd)
    
    failed_reason = None
    
    try:        
        costfed_proc.wait(timeout)
        if costfed_proc.returncode == 0:
            logger.info(f"{query} benchmarked sucessfully")
            
        else:
            logger.error(f"{query} reported error")    
            failed_reason = "error_runtime"
            
    except subprocess.TimeoutExpired: 
        logger.exception(f"{query} timed out!")        
        failed_reason = "timeout"
    finally:
        os.system('pkill -9 -f "costfed/target"')
        cache_file = f"{engine_dir}/cache.db"
        Path(cache_file).unlink(missing_ok=True)
        #kill_process(fedx_proc.pid)     
        
    # Write proxy stats
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
    
    if os.stat(infile).st_size == 0:
        Path(outfile).touch()
        return
    
    raw_result_df = pd.read_csv(infile)
    
    if raw_result_df.empty:
        Path(outfile).unlink(missing_ok=True)
        Path(outfile).touch()
        return
    
    def extract_result_col(x):
        result = re.sub(r"[\[\]\"]", "", x)
        return result.split(";")
    
    def make_columns(row):
        colname, result = row["results"].split("=")
        row["results"] = result
        row["column"] = colname
        return row
    
    out_df = raw_result_df.T
    out_df.columns = ["results"]
    out_df["results"] = out_df["results"].apply(extract_result_col)
    out_df = out_df.explode("results")
    out_df = out_df.apply(make_columns, axis=1)
    out_df = out_df.pivot(columns="column", values="results").reset_index(drop=True)
    out_df.to_csv(outfile, index=False)

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
    
    with open(infile, "r") as ifs:
        if len(ifs.read().strip()) == 0:
            Path(outfile).touch()
            logger.debug(f"{infile} is empty!")
            return

    def extract_triple(x):
        fedx_pattern = r"{StatementPattern\s+?Var\s+\((name=\w+;\s+value=(.*);\s+anonymous|name=(\w+))\)\s+Var\s+\((name=\w+;\s+value=(.*);\s+anonymous|name=(\w+))\)\s+Var\s+\((name=\w+;\s+value=(.*);\s+anonymous|name=(\w+))\)"
        match = re.match(fedx_pattern, x)
        
        s = match.group(2) or match.group(3)
        p = match.group(5) or match.group(6)
        o = match.group(8) or match.group(9)
        
        result = " ".join([s, p, o])
        return result

    def extract_source_selection(x):
        # Find all instances of "StatementSource (id=sparql_localhost:1234_ratingsite0_sparql; type=REMOTE);" 
        fex_pattern = r"StatementSource\s+\(id=((\w|:)+);\s+type=[A-Z]+\)"
        
        # Convert sparql_localhost:34218_vendor8_sparql into http://localhost:34218/sparql
        result = [ 
            cgroup[0].replace("sparql_", "http://").replace("_", "/")
            for cgroup in re.findall(fex_pattern, x) 
        ]
        return result
    
    def lookup_composition(x: str):
        return inv_composition[x]
    
    def pad(x):
        encoder = LabelEncoder()
        encoded = encoder.fit_transform(x)
        result = np.pad(encoded, (0, max_length-len(x)), mode="constant", constant_values=-1)                
        decoded = [ encoder.inverse_transform([item]).item() if item != -1 else "" for item in result ]
        return decoded
    
    in_df = pd.read_csv(infile)
    
    with open(prefix_cache, "r") as prefix_cache_fs, open(os.path.join(Path(prefix_cache).parent, "composition.json"), "r") as comp_fs:
        prefix2alias = json.load(prefix_cache_fs)    
        composition = json.load(comp_fs)
        inv_composition = {f"{' '.join(v)}": k for k, v in composition.items()}
            
        out_df = None
        for key in in_df.keys():
                        
            in_df[f"triple{key}"] = in_df[str(key)].apply(extract_triple)
            in_df[f"tp_name{key}"] = in_df[f"triple{key}"].apply(lookup_composition)
            in_df[f"tp_number{key}"] = in_df[f"tp_name{key}"].str.replace("tp", "", regex=False).astype(int)
            in_df.sort_values(f"tp_number{key}", inplace=True)
            in_df[f"source_selection{key}"] = in_df[str(key)].apply(extract_source_selection)

            # If unequal length (as in union, optional), fill with nan
            max_length = in_df[f"source_selection{key}"].apply(len).max()
            #in_df[f"source_selection{key}"] = in_df[f"source_selection{key}"].apply(pad)
            
            if str(key) == "Result #0":
                out_df = in_df.set_index(f"tp_name{key}")[f"source_selection{key}"] \
                    .to_frame().T \
                    .apply(pd.Series.explode) \
                    .reset_index(drop=True) 
            else: 
                out_temp_df = in_df.set_index(f"tp_name{key}")[f"source_selection{key}"] \
                    .to_frame().T \
                    .apply(pd.Series.explode) \
                    .reset_index(drop=True) 
                out_df = pd.concat([out_df, out_temp_df], axis=1)
            out_df.to_csv(outfile, index=False)

@cli.command()
@click.argument("eval-config", type=click.Path(exists=True, dir_okay=False, file_okay=True))
@click.argument("batch_id", type=click.INT)
@click.pass_context
def generate_config_file(ctx: click.Context, eval_config, batch_id):
    """Generate the config file for the engine

    Args:
        ctx (click.Context): _description_
        datafiles (_type_): _description_
        outfile (_type_): _description_
        endpoint (_type_): _description_
    """
    
    # Load the config file
    config = load_config(eval_config)
    proxy_mapping_file = os.path.realpath(
        os.path.join(config["generation"]["workdir"], f"virtuoso-proxy-mapping-batch{batch_id}.json")
    )
    
    endpoints_file = f"summaries/endpoints_batch{batch_id}.txt"
    summary_file = f"summaries/sum_fedshop_batch{batch_id}.n3"     
    engine_dir = config["evaluation"]["engines"]["costfed"]["dir"]   
    
    oldcwd = os.getcwd()
    logger.debug(f"Switching to {engine_dir}...")
    os.chdir(Path(engine_dir))  

    Path("summaries").mkdir(parents=True, exist_ok=True)
    
    # Generate the endpoints file
    endpoints = []
    with open(endpoints_file, "w") as efs, open(proxy_mapping_file, "r") as pmfs:
        proxy_mapping = json.load(pmfs)
        federation_members = config["generation"]["virtuoso"]["federation_members"]
        for federation_member_iri in federation_members[f"batch{batch_id}"].values():
            target_endpoint = proxy_mapping[federation_member_iri]
            endpoints.append(target_endpoint)
            efs.write(f"{target_endpoint}\n")
    
    # Generate summary      
    require_update = False 
    logger.debug(f"Checking {summary_file}...")
    if os.path.exists(summary_file):
        if os.stat(summary_file).st_size == 0:
            logger.debug("{summary_file} is empty! Regenerating...")
            require_update = True
        else:
            with open(summary_file, "r") as sfs:
                content = sfs.read()
                for endpoint in endpoints:
                    if endpoint not in content:
                        logger.debug(f"{endpoint} not found in {summary_file}")
                        require_update = True
                        break
    else:
        require_update = True

    if require_update:
        try:
            logger.info(f"Generating summary for batch {batch_id}")
            cmd = f'mvn exec:java -Dexec.mainClass="org.aksw.simba.quetsal.util.TBSSSummariesGenerator" -Dexec.args="{summary_file} {endpoints_file}" -pl costfed'
            logger.debug(cmd)
            proc = subprocess.run(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
            if proc.returncode != 0: raise RuntimeError(f"Could not generate {summary_file}")
        except InterruptedError:
            Path(summary_file).unlink(missing_ok=True)
    
    # Modify costfed/costfed.props
    cmd = f'sed -Ei "s#quetzal\.fedSummaries=summaries/sum_fedshop_batch[0-9]+\.n3#quetzal.fedSummaries={summary_file}#g" costfed/costfed.props'
    logger.debug(cmd)
    proc = subprocess.run(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    if proc.returncode != 0: raise RuntimeError("Could not modify costfed/costfed.props")
    #if os.system(cmd) != 0: raise RuntimeError("Could not modify costfed/costfed.props")
    
    os.chdir(oldcwd)

if __name__ == "__main__":
    cli()