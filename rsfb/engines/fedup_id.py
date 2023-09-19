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

import fedup_h0
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
    ctx.invoke(fedup_h0.prerequisites, eval_config=eval_config)

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
    
    ctx.invoke(
        fedup_h0.run_benchmark, eval_config=eval_config, engine_config=engine_config, 
        query=query, out_result=out_result, out_source_selection=out_source_selection, 
        query_plan=query_plan, stats=stats, force_source_selection=force_source_selection, 
        batch_id=batch_id, noexec=noexec, approach="id"
    )
        

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
    
    ctx.invoke(
        fedup_h0.generate_config_file, datafiles=datafiles, outfile=outfile, 
        eval_config=eval_config, batch_id=batch_id, endpoint=endpoint
    )
    
if __name__ == "__main__":
    cli()