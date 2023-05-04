# Import part
from io import BytesIO, StringIO
import json
import os
import re
import shutil
import click
import glob
import subprocess
import pandas as pd
import numpy as np
from pathlib import Path

import sys
sys.path.append(str(os.path.join(Path(__file__).parent.parent)))

from utils import kill_process, load_config, str2n3
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
    app_dir = config["evaluation"]["engines"]["anapsid"]["dir"]
    
    old_dir = os.getcwd()
    os.chdir(app_dir)
    if os.system("wget https://github.com/mhoangvslev/ANAPSID/releases/download/v1.0/anapsid.zip -O anapsid.zip") != 0:
        raise RuntimeError("Could not download anapsid.zip")
    if os.system("unzip -o anapsid.zip") != 0:
        raise RuntimeError("Could not extract from anapsid.zip")
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
    
    app_dir = load_config(eval_config)["engines"]["anapsid"]["dir"]
    old_dir = os.getcwd()
    os.chdir(app_dir)
    os.system(f"./dist/run_anapsid -e ../../{engine_config} -q ../../{in_query_file}")
    os.chdir()

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
    pass

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
    pass

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
    
    ssite = set()
    for data_file in datafiles:
        with open(data_file, "r") as file:
            t_file = file.readlines()
            for line in t_file:
                site = line.rsplit()[-2]
                site = re.search(r"<(.*)>", site).group(1)
                ssite.add(site)
                    
    Path(outfile).parent.mkdir(parents=True, exist_ok=True)
    with open(outfile, "w+") as config_fs:
        for s in sorted(ssite):
            config_fs.write(f"{endpoint}/?default-graph-uri={s}\n")

if __name__ == "__main__":
    cli()