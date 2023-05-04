# Import part
from io import BytesIO
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

# Example of use : 
# python3 utils/generate-engine-config-file.py experiments/bsbm/model/vendor test/out.ttl

# Goal : Generate a configuration file for RDF4J to set the use of named graph as endpoint thanks to data file

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
    ctx.invoke(fedx.prerequisites, eval_config=eval_config)

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
    """ Evaluate injected.sparql on Virtuoso. 
    1. Transform the injected.sparql into
        VALUES ?tp1 ... ?tpn { (s1, s2 ... sn) (s1, s2 ... sn) } .
        SERVICE ?tp1 { ... } .
        SERVICE ?tp2 { ... } .
        ...
    2. Execute the transformed query in virtuoso
    3. Mesure execution time, compare the results with results.csv in generation phase
    """
    
    source_selection_df = pd.read_csv(force_source_selection)
            
    intermediate_result_files = []
    intermediate_stats_files = []
    
    for row_id in range(len(source_selection_df)):
        intermediate_provenance = source_selection_df.loc[row_id, :].to_frame().T

        intermediate_provenance_file = Path(f"{force_source_selection}.r{row_id}")
        intermediate_provenance_file.touch()
        intermediate_provenance.to_csv(intermediate_provenance_file, index=False) 
            
        intermediate_result_file = Path(f"{out_result}.r{row_id}")
        intermediate_result_file.touch()
            
        intermediate_qplan_file = Path(f"{query_plan}.r{row_id}")
        intermediate_qplan_file.touch()
            
        intermediate_stats_file = Path(f"{stats}.r{row_id}")
        intermediate_stats_file.touch()
            
        fedx.exec_fedx(
            eval_config, engine_config, query, 
            str(intermediate_result_file), "/dev/null", str(query_plan), 
            str(intermediate_stats_file), str(intermediate_provenance_file), batch_id
        )
             
        os.system(f"rm {intermediate_provenance_file}")
        
        intermediate_result_files.append(str(intermediate_result_file))
        intermediate_stats_files.append(str(intermediate_stats_file))
            
    stats_df = pd.concat([ pd.read_csv(f) for f in intermediate_stats_files ], ignore_index=True)
    stats_df.groupby(["query", "batch", "instance", "engine", "attempt"]).sum() \
        .reset_index() \
        .to_csv(stats, index=False)
        
    os.system(f"cat {' '.join(intermediate_result_files)} > {out_result}")
    os.system(f"sed -i '/^\s*$/d' {out_result}")
    os.system(f"find {Path(out_result).parent} -type f -empty -print -delete")
    shutil.copy(force_source_selection, out_source_selection)
        

@cli.command()
@click.argument("infile", type=click.Path(exists=False, file_okay=True, dir_okay=False))
@click.argument("outfile", type=click.Path(exists=False, file_okay=True, dir_okay=False))
@click.pass_context
def transform_results(ctx: click.Context, infile, outfile):
    ctx.invoke(fedx.transform_results, infile=infile, outfile=outfile)

@cli.command()
@click.argument("infile", type=click.Path(exists=False, file_okay=True, dir_okay=False))
@click.argument("outfile", type=click.Path(exists=False, file_okay=True, dir_okay=False))
@click.argument("prefix-cache", type=click.Path(exists=False, file_okay=True, dir_okay=False))
@click.pass_context
def transform_provenance(ctx: click.Context, infile, outfile, prefix_cache):
    shutil.copy(infile, outfile)
    #ctx.invoke(fedx.transform_provenance, infile=infile, outfile=outfile, prefix_cache=prefix_cache)

@cli.command()
@click.argument("datafiles", type=click.Path(exists=True, dir_okay=False, file_okay=True), nargs=-1)
@click.argument("outfile", type=click.Path(exists=False, file_okay=True, dir_okay=False))
@click.argument("eval-config", type=click.Path(exists=True, dir_okay=False, file_okay=True))
@click.option("--endpoint", type=str, default="http://localhost:8890/sparql", help="URL to a SPARQL endpoint")
@click.pass_context
def generate_config_file(ctx: click.Context, datafiles, outfile, eval_config, endpoint):
    ctx.invoke(fedx.generate_config_file, datafiles=datafiles, outfile=outfile, eval_config=eval_config, endpoint=endpoint)
    

if __name__ == "__main__":
    cli()