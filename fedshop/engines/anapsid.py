# Import part
from io import BytesIO, StringIO
import json
import os
import re
import resource
import shutil
import click
import glob
import subprocess
import pandas as pd
import numpy as np
from pathlib import Path
import requests

from sklearn.calibration import LabelEncoder

import sys

from ..utils import kill_process, load_config, fedshop_logger, create_stats, str2n3
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
    
    config = load_config(eval_config)
    app_dir = config["evaluation"]["engines"]["anapsid"]["dir"]
    
    old_dir = os.getcwd()
    os.chdir(app_dir)

    # Check if Python version is 2.7
    python2_bin = shutil.which("python2").replace("shims", "versions/2.7.18/bin")
    if python2_bin is None:
        raise RuntimeError("Python 2.7 is required to run ANAPSID.")
    
    pip2_bin = f"{python2_bin} -m pip"
    
    cmd = f"rm -rf build && {pip2_bin} install -r requirements.txt --no-cache --force-reinstall && {pip2_bin} install . --no-cache --force-reinstall"
    print(cmd)
    if os.system(cmd) != 0:
        raise RuntimeError("Could not compile ANAPSID")
    os.chdir(old_dir)

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
        engine_config (_type_): _description_
        query (_type_): _description_
        out_result (_type_): _description_
        out_source_selection (_type_): _description_
        query_plan (_type_): _description_
        stats (_type_): _description_
        batch_id (_type_): _description_
    """
    
    if batch_id > 4:
        # We do this because at batch 5, ANAPSID saturates re
        Path(out_result).touch()
        Path(out_source_selection).touch()
        Path(query_plan).touch()
        create_stats(stats)
        return
        
    summary_file = f"summaries/sum_fedshop_batch{batch_id}.txt"
    config = load_config(eval_config)
    app_dir = config["evaluation"]["engines"]["anapsid"]["dir"]
    compose_file = config["generation"]["virtuoso"]["compose_file"]
    service_name = config["generation"]["virtuoso"]["service_name"]
    timeout = int(config["evaluation"]["timeout"])
    old_dir = os.getcwd()
    
    proxy_server = config["evaluation"]["proxy"]["endpoint"]
    
    # Reset the proxy stats
    if requests.get(proxy_server + "reset").status_code != 200:
        raise RuntimeError("Could not reset statistics on proxy!")

    python2_bin = shutil.which("python2").replace("shims", "versions/2.7.18/bin")
    if python2_bin is None:
        raise RuntimeError("Python 2.7 is required to run ANAPSID.")
    
    environment_settings = f'HTTP_PROXY={proxy_server} HTTPS_PROXY={proxy_server} NO_PROXY=""'
    timeoutCmd = f'timeout --signal=SIGKILL {timeout}' if timeout != 0 else ""
    cmd = f"{environment_settings} {timeoutCmd} {python2_bin} scripts/run_anapsid -e {summary_file} -q ../../{query} -p naive -s False -o False -d SSGM -a True -r ../../{out_result} -z ../../{Path(stats).parent}/ask.txt -y ../../{Path(stats).parent}/planning_time.txt -x ../../{query_plan} -v ../../{out_source_selection} -u ../../{Path(stats).parent}/source_selection_time.txt -n ../../{Path(stats).parent}/exec_time.txt -c {str(noexec)}"

    print("=== ANAPSID ===")
    print(cmd)
    print("================")      

    #ANAPSID need to initialize following files
    Path(out_result).touch()
    Path(out_source_selection).touch()
    Path(query_plan).touch()   
    
    # Set the maximum amount of memory to be used by the subprocess in bytes
    os.chdir(app_dir)
    anapsid_proc = subprocess.Popen(cmd.strip(), shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    os.chdir(old_dir)
    
    failed_reason = None

    try: 
        anapsid_proc.wait(timeout=timeout)
        if anapsid_proc.returncode == 0:
            logger.info(f"{query} benchmarked sucessfully") 

            # Write stats
            logger.info(f"Writing stats to {stats}")
                        
            def report_error(reason):
                logger.error(f"{query} yield no results!")
                create_stats(stats, reason)
                
            try: 
                results_df = pd.read_csv(out_result).replace("null", None)
                if results_df.empty or os.stat(out_result).st_size == 0: 
                    report_error("error_runtime")       
            except pd.errors.EmptyDataError:
                report_error("error_runtime")
                        
        else:
            logger.error(f"{query} reported error {anapsid_proc.returncode}")    
            askFile = f"{Path(stats).parent}/ask.txt"
            errorFile = f"{Path(stats).parent}/error.txt"
            if os.path.exists(errorFile):
                with open(errorFile, "r") as f:
                    failed_reason = f.read()
                    if failed_reason == "type_error":
                        os.remove(askFile)
            else:
                failed_reason = "error_runtime"
    except subprocess.TimeoutExpired: 
        logger.exception(f"{query} timed out!")
        logger.info("Writing empty stats...")
        failed_reason = "timeout"

    finally:
        #kill_process(anapsid_proc.pid)
        os.system('pkill -9 -f "scripts/run_anapsid"')
        
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
    lines = []
    with open(infile) as in_file:
        content = in_file.read().strip()
        if len(content) == 0:
            Path(outfile).touch(exist_ok=False)
        else:
            lines = str(content)
            lines = re.findall("(?:[a-zA-Z0-9\-_\.:\^\/\'\",<># ]+){?", lines)
            dict_list = []
            for line in lines:
                dict_list.append(eval('{'+line+'}'))
            result_df = pd.DataFrame(dict_list)
            with open(outfile, "w+") as out_file:
                out_file.write(result_df.to_csv(index=False))

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
    lines = []
    with open(infile) as in_file:
        lines = "".join(in_file.readlines())
        lines = re.findall("((?:http\:\/\/www\.(?:vendor|ratingsite)[0-9]+\.fr\/))>', \[\n\s+((?:[a-zA-Z0-9\-_\.:\?\^\/\'\",<>\=#\n ]+))(?:\n\+)?", lines)
    mydict = dict()
    for line in lines:
        if bool(re.search("\n\s+", line[1])):
            triples = re.split(",\s+\n\s+", line[1])
            for triple in triples:
                if triple in mydict.keys():
                    mydict[triple].add(line[0])
                else:
                    mydict[triple] = set([line[0]])
        else:
            if line[1] in mydict.keys():
                mydict[line[1]].add(line[0])
            else:
                mydict[line[1]] = set([line[0]])
    for key, value in mydict.items():
        mydict[key] = list(value)
    result_df = pd.DataFrame([(key,val) for key, val in mydict.items()], columns=['triples','sources'])
    tmp_outfile = f"{outfile}.tmp"
    result_df.to_csv(tmp_outfile, index=False)

    raw_source_selection = pd.read_csv(tmp_outfile, sep=",")[["triples", "sources"]]
    
    tp_composition = f"{Path(prefix_cache).parent}/composition.json"
    with    open(tp_composition, "r") as comp_fs,\
            open(prefix_cache, "r") as prefix_cache_fs \
    :
        prefix_cache_dict = json.load(prefix_cache_fs)
        
        comp = { k: " ".join(v) for k, v in json.load(comp_fs).items() }
        inv_comp = {}
        for k,v in comp.items():
            if inv_comp.get(v) is None:
                inv_comp[v] = []
            inv_comp[v].append(k) 
        
        def get_triple_id(x):
            result = re.sub(r"[\[\]]", "", x).strip()
            for prefix, alias in prefix_cache_dict.items():
                result = re.sub(rf"<{re.escape(prefix)}(\w+)>", rf"{alias}:\1", result)
                        
            return inv_comp[result] 
        
        def pad(x, max_length):
            encoder = LabelEncoder()
            encoded = encoder.fit_transform(x)
            result = np.pad(encoded, (0, max_length-len(x)), mode="constant", constant_values=-1)                
            decoded = [ encoder.inverse_transform([item]).item() if item != -1 else "" for item in result ]
            return decoded
        
        raw_source_selection["triples"] = raw_source_selection["triples"].apply(lambda x: re.split(r"\s*,\s*", x))
        raw_source_selection = raw_source_selection.explode("triples")
        raw_source_selection["triples"] = raw_source_selection["triples"].apply(get_triple_id)
        raw_source_selection = raw_source_selection.explode("triples")
        raw_source_selection["tp_number"] = raw_source_selection["triples"].str.replace("tp", "", regex=False).astype(int)
        raw_source_selection.sort_values("tp_number", inplace=True)
        raw_source_selection["sources"] = raw_source_selection["sources"].apply(lambda x: re.split(r"\s*,\s*", re.sub(r"[\[\]]", "",x)))
        
        # If unequal length (as in union, optional), fill with nan
        max_length = raw_source_selection["sources"].apply(len).max()
        raw_source_selection["sources"] = raw_source_selection["sources"].apply(lambda x: pad(x, max_length))
               
        out_df = raw_source_selection.set_index("triples")["sources"] \
            .to_frame().T \
            .apply(pd.Series.explode) \
            .reset_index(drop=True) 
        
        out_df.to_csv(outfile, index=False)

    os.remove(tmp_outfile)

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
    summary_file = f"summaries/sum_fedshop_batch{batch_id}.txt"     
    engine_dir = config["evaluation"]["engines"]["anapsid"]["dir"]   
    
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

    update_required = False       
    if not os.path.exists(summary_file):
        update_required = True
    else:
        with open(summary_file, "r") as ofs:
            content = ofs.read()
            for source in endpoints:
                if source not in content:
                    update_required = True
                    logger.debug(f"{source} not in {summary_file}")
                    break
        
    if update_required:             
        Path(summary_file).parent.mkdir(parents=True, exist_ok=True)
        tmp_summaryfile = f"{summary_file}.tmp"
        with open(tmp_summaryfile, "w") as config_fs:
            for s in sorted(endpoints):
                config_fs.write(f"{s}\n")

        cmd = f"python scripts/get_predicates {tmp_summaryfile} {summary_file}"
        logger.debug(cmd)
        os.remove(tmp_summaryfile)
        os.system(cmd)
        os.chdir(oldcwd)

if __name__ == "__main__":
    cli()