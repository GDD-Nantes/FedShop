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

from sklearn.calibration import LabelEncoder

import sys
sys.path.append(str(os.path.join(Path(__file__).parent.parent)))

from utils import check_container_status, kill_process, load_config, rsfb_logger, write_empty_result, create_stats, str2n3
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
    
    config = load_config(eval_config)
    app_dir = config["evaluation"]["engines"]["anapsid"]["dir"]
    
    old_dir = os.getcwd()
    os.chdir(app_dir)
    os.system("rm -rf build && python setup.py install")
    os.chdir(old_dir)

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
        out_result (_type_): _description_
        out_source_selection (_type_): _description_
        query_plan (_type_): _description_
        stats (_type_): _description_
        force_source_selection (_type_): _description_
        batch_id (_type_): _description_
    """
    
    if batch_id > 4:
        # We do this because at batch 5, ANAPSID saturates re
        Path(out_result).touch()
        Path(out_source_selection).touch()
        Path(query_plan).touch()
        create_stats(stats)
        return
        
    in_query_file = f"{Path(out_result).parent}/injected.sparql"
    
    with    open(query, "r") as qfs, \
            open(in_query_file, "w") as in_qfs, \
            open(f"{Path(query).parent}/prefix_cache.json", "r") as prefix_cache_fs \
    :
        query_text = qfs.read()
        prefix_cache = json.load(prefix_cache_fs)
        prefix_cache_inv = {v: k for k, v in prefix_cache.items()}
        for alias, prefix in prefix_cache_inv.items():
            query_text = re.sub(re.escape(alias)+":", str2n3(prefix), query_text)
                    
        for line in query_text.splitlines(keepends=True):
            if line.strip().startswith("#") or line.strip().startswith("PREFIX"):
                continue
            
            in_qfs.write(re.sub(r">(\w+)", r"\1>", line))
    
    config = load_config(eval_config)
    app_dir = config["evaluation"]["engines"]["anapsid"]["dir"]
    last_batch = config["generation"]["n_batch"] - 1
    compose_file = config["generation"]["virtuoso"]["compose_file"]
    service_name = config["generation"]["virtuoso"]["service_name"]
    container_name = config["generation"]["virtuoso"]["container_names"][last_batch]
    timeout = int(config["evaluation"]["timeout"])
    old_dir = os.getcwd()

    cmd = f"python scripts/run_anapsid -e ../../{engine_config} -q ../../{in_query_file} -p naive -s False -o False -d SSGM -a True -r ../../{out_result} -z ../../{Path(stats).parent}/ask.txt -y ../../{Path(stats).parent}/planning_time.txt -x ../../{query_plan} -v ../../{out_source_selection} -u ../../{Path(stats).parent}/source_selection_time.txt -n ../../{Path(stats).parent}/exec_time.txt"

    print("=== ANAPSID ===")
    print(cmd)
    print("================")      

    #ANAPSID need to initialize following files
    Path(out_result).touch() #result.txt
    Path(query_plan).touch() #query_plan.txt
    
    
    # Set the maximum amount of memory to be used by the subprocess in bytes
    max_mem = 100000000  # 100 MB
    resource.setrlimit(resource.RLIMIT_AS, (max_mem, max_mem))

    os.chdir(app_dir)
    anapsid_proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    os.chdir(old_dir)

    try: 
        anapsid_proc.wait(timeout=timeout)
        if anapsid_proc.returncode == 0:
            logger.info(f"{query} benchmarked sucessfully") 

            # Write stats
            logger.info(f"Writing stats to {stats}")
            
            create_stats(stats)
            
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
                    reason = f.read()
                    if reason == "type_error":
                        os.remove(askFile)
                    create_stats(stats, reason)
            else:
                create_stats(stats, "error_runtime")
    except subprocess.TimeoutExpired: 
        logger.exception(f"{query} timed out!")
        if (container_status := check_container_status(compose_file, service_name, container_name)) != "running":
            logger.debug(container_status)
            #raise RuntimeError("Backend is terminated!")
        logger.info("Writing empty stats...")
        create_stats(stats, "timeout")

    finally:
        #kill_process(anapsid_proc.pid)
        os.system('pkill -9 -f "scripts/run_anapsid"')

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
    
    tp_composition = f"{Path(prefix_cache).parent}/provenance.sparql.comp"
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
    app_dir = config["evaluation"]["engines"]["anapsid"]["dir"]
    
    old_dir = os.getcwd()
    
    ssite = set()
    
    endpoints=f"{Path(outfile).parent}/endpoints.txt"
    
    if not Path(endpoints).exists():
        for data_file in datafiles:
            with open(data_file, "r") as file, open(endpoints, "w") as efs:
                t_file = file.readlines()
                for line in t_file:
                    site = line.rsplit()[-2]
                    site = re.search(r"<(.*)>", site).group(1)
                    ssite.add(site)
                efs.write("\n".join(ssite))
    else:
        with open(endpoints, "r") as efs:
            ssite = set(efs.readlines())
         
    update = False       
    if not os.path.exists(outfile):
        update = True
    else:
        with open(outfile, "r") as ofs:
            content = ofs.read()
            for source in ssite:
                if source not in content:
                    update = True
                    logger.debug(f"{source} not in {outfile}")
                    break
        
    if update:             
        Path(outfile).parent.mkdir(parents=True, exist_ok=True)
        tmp_outfile = f"{outfile}.tmp"
        with open(tmp_outfile, "w+") as config_fs:
            for s in sorted(ssite):
                config_fs.write(f"{endpoint}/?default-graph-uri={s}\n")

        os.chdir(app_dir)
        os.system(f"python scripts/get_predicates ../../{tmp_outfile} ../../{outfile}")
        os.chdir(old_dir)
        os.remove(tmp_outfile)

if __name__ == "__main__":
    cli()