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
from sklearn.preprocessing import LabelEncoder
import psutil

import sys
sys.path.append(str(os.path.join(Path(__file__).parent.parent)))

from utils import check_container_status, load_config, rsfb_logger, str2n3, write_empty_result, create_stats, create_stats
import fedx

logger = rsfb_logger(Path(__file__).name)

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
    os.system("rm -rf costfed/target && mvn compile && mvn package --also-make")
    os.chdir(oldcwd)

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
@click.pass_context
def run_benchmark(ctx: click.Context, eval_config, engine_config, query, out_result, out_source_selection, query_plan, stats, force_source_selection, batch_id, noexec):
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
        out_result (_type_): The file that holds results of the input query
        out_source_selection (_type_): The file that holds engine's source selection
        query_plan (_type_): _description_
        stats (_type_): _description_
        force_source_selection (_type_): _description_
        batch_id (_type_): _description_
    """
    
    config = load_config(eval_config)
    app_config = config["evaluation"]["engines"]["costfed"]
    app = app_config["dir"]
    last_batch = int(config["generation"]["n_batch"]) - 1
    
    endpoint = config["generation"]["virtuoso"]["endpoints"][last_batch]
    compose_file = config["generation"]["virtuoso"]["compose_file"]
    service_name = config["generation"]["virtuoso"]["service_name"]
    container_name = config["generation"]["virtuoso"]["container_names"][last_batch]
    timeout = int(config["evaluation"]["timeout"])

    oldcwd = os.getcwd()
    summary_file = f"summaries/sum_fedshop_batch{batch_id}.n3"     
    cmd = f"./costfed.sh costfed/costfed.props {endpoint} ../../{out_result} ../../{out_source_selection} ../../{query_plan} {timeout} ../../{query} {out_result.split('/')[7].split('_')[1]} false true {summary_file}"

    logger.debug("=== CostFed ===")
    logger.debug(cmd)
    logger.debug("============")

    os.chdir(Path(app))
    costfed_proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    os.chdir(oldcwd)
    try:        
        costfed_proc.wait(timeout)
        if costfed_proc.returncode == 0:
            logger.info(f"{query} benchmarked sucessfully")
            
            # Write stats
            logger.info(f"Writing stats to {stats}")
                         
            # basicInfos = re.match(r".*/(\w+)/(q\w+)/instance_(\d+)/batch_(\d+)/attempt_(\d+)/stats.csv", stats)
            # queryName = basicInfos.group(2)
            # instance = basicInfos.group(3)
            # batch = basicInfos.group(4)
            # attempt = basicInfos.group(5)
            
            # stats_df = pd.read_csv(stats).rename({
            #     "Result #0": "query",
            #     "Result #1": "engine",
            #     "Result #2": "instance",
            #     "Result #3": "batch",
            #     "Result #4": "attempt",
            #     "Result #5": "exec_time",
            #     "Result #6": "ask",
            #     "Result #7": "source_selection_time",
            #     "Result #8": "planning_time"
            # }, axis=1)
    
            # stats_df = stats_df \
            #     .replace('injected.sparql',str(queryName)) \
            #     .replace('instance_id',str(instance)) \
            #     .replace('batch_id',str(batch)) \
            #     .replace('attempt_id',str(attempt))
                
            # stats_df.to_csv(stats, index=False)
                        
            results_df = pd.read_csv(out_result).replace("null", None)
            
            if results_df.dropna(how="all").empty or os.stat(out_result).st_size == 0:            
                logger.error(f"{query} yield no results!")
                write_empty_result(out_result)
                os.system(f"docker stop {container_name}")
                raise RuntimeError(f"{query} yield no results!")

            create_stats(stats)

        else:
            logger.error(f"{query} reported error")    
            write_empty_result(out_result)
            create_stats(stats, "error_runtime")
            
    except subprocess.TimeoutExpired: 
        logger.exception(f"{query} timed out!")
        if (container_status := check_container_status(compose_file, service_name, container_name)) != "running":
            logger.debug(container_status)
            raise RuntimeError("Backend is terminated!")
        
        # Counter-measure for issue #41 (https://github.com/mhoangvslev/FedShop/issues/41)
        if psutil.virtual_memory().percent >= 60:
            os.system(f"docker stop {container_name}")
        
        logger.info("Writing empty stats...")
        create_stats(stats, "timeout")
        write_empty_result(out_result)    
    finally:
        os.system('pkill -9 -f "costfed/target"')
        cache_file = f"{app}/cache.db"
        Path(cache_file).unlink(missing_ok=True)
        #kill_process(fedx_proc.pid)        

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

    def extract_triple(x):
        fedx_pattern = r"{StatementPattern\s+?Var\s+\((name=\w+;\s+value=(.*);\s+anonymous|name=(\w+))\)\s+Var\s+\((name=\w+;\s+value=(.*);\s+anonymous|name=(\w+))\)\s+Var\s+\((name=\w+;\s+value=(.*);\s+anonymous|name=(\w+))\)"
        match = re.match(fedx_pattern, x)
        
        s = match.group(2)
        if s is None: s = f"?{match.group(3)}"
        
        p = match.group(5)
        if p is None: p = f"?{match.group(6)}"
        
        o = match.group(8) 
        if o is None: o = f"?{match.group(9)}"
        
        result = " ".join([s, p, o])
                
        for prefix, alias in prefix2alias.items():
            result = result.replace(prefix, f"{alias}:")
            
        if s.startswith("http"):
            result = result.replace(s, str2n3(s))
            
        if o.startswith("http"):
            result = result.replace(o, str2n3(o))
        
        return result

    def extract_source_selection(x):
        fex_pattern = r"StatementSource\s+\(id=sparql_localhost\:[0-9]+_sparql_\?default-graph-uri=([a-z]+(\.\w+)+\.[a-z]+)_;\s+type=[A-Z]+\)"
        result = [ cgroup[0] for cgroup in re.findall(fex_pattern, x) ]
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
    #print(in_df)
    
    with open(prefix_cache, "r") as prefix_cache_fs, open(os.path.join(Path(prefix_cache).parent, "provenance.sparql.comp"), "r") as comp_fs:
        prefix2alias = json.load(prefix_cache_fs)    
        composition = json.load(comp_fs)
        inv_composition = {f"{' '.join(v)}": k for k, v in composition.items()}
            
        out_df = None
        for key in in_df.keys():

            in_df[str("triple"+str(key))] = in_df[str(key)].apply(extract_triple)
            in_df[str("tp_name"+str(key))] = in_df[str("triple"+str(key))].apply(lookup_composition)
            in_df[str("tp_number"+str(key))] = in_df[str("tp_name"+str(key))].str.replace("tp", "", regex=False).astype(int)
            in_df.sort_values(str("tp_number"+str(key)), inplace=True)
            in_df[str("source_selection"+str(key))] = in_df[str(key)].apply(extract_source_selection)

            # If unequal length (as in union, optional), fill with nan
            max_length = in_df[str("source_selection"+str(key))].apply(len).max()
            in_df[str("source_selection"+str(key))] = in_df[str("source_selection"+str(key))].apply(pad)

            if str(key) == "Result #0":
                out_df = in_df.set_index(str("tp_name"+str(key)))[str("source_selection"+str(key))] \
                    .to_frame().T \
                    .apply(pd.Series.explode) \
                    .reset_index(drop=True) 
            else: 
                out_temp_df = in_df.set_index(str("tp_name"+str(key)))[str("source_selection"+str(key))] \
                    .to_frame().T \
                    .apply(pd.Series.explode) \
                    .reset_index(drop=True) 
                out_df = pd.concat([out_df, out_temp_df], axis=1)
            out_df.to_csv(outfile, index=False)

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

    summary_file = f"summaries/sum_fedshop_batch{batch_id}.n3"     
    app_config = load_config(eval_config)["evaluation"]["engines"]["costfed"]
    app = app_config["dir"]   

    oldcwd = os.getcwd()
    os.chdir(Path(app))        
    
    update_summary = not os.path.exists(summary_file)
    if not update_summary:
        print(f"Looking for {endpoint} in {summary_file}")
        with open(summary_file, "r") as sfs:
            update_summary = endpoint not in sfs.read()

    if update_summary:
        try:
            logger.info(f"Generating summary for batch {batch_id}")
            cmd = f"./costfed.sh costfed/costfed.props {endpoint} ignore ignore ignore ignore ignore {batch_id} true false {summary_file}"
            logger.debug(cmd)
            if os.system(cmd) != 0: raise RuntimeError(f"Could not generate {summary_file}")
        except InterruptedError:
            Path(summary_file).unlink(missing_ok=True)
    
    os.chdir(oldcwd)
    ctx.invoke(fedx.generate_config_file, datafiles=datafiles, outfile=outfile, eval_config=eval_config, endpoint=endpoint)

if __name__ == "__main__":
    cli()