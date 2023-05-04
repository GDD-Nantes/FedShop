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

from utils import load_config, rsfb_logger, str2n3, write_empty_result, create_stats
logger = rsfb_logger(Path(__file__).name)

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
    
    app_config = load_config(eval_config)["evaluation"]["engines"]["fedx_rwss"]
    app = app_config["dir"]
    jar = os.path.join(app, "FedX-1.0-SNAPSHOT.jar")
    lib = os.path.join(app, "lib/*")
    
    #if not os.path.exists(app) or not os.path.exists(jar) or os.path.exists(lib):
    oldcwd = os.getcwd()
    os.chdir(Path(app).parent)
    os.system("mvn clean && mvn install dependency:copy-dependencies package")
    os.chdir(oldcwd)

def exec_fedx(eval_config, engine_config, query, out_result, out_source_selection, query_plan, stats, force_source_selection, batch_id):
    config = load_config(eval_config)
    app_config = config["evaluation"]["engines"]["fedx_rwss"]
    app = app_config["dir"]
    jar = os.path.join(app, "FedX-1.0-SNAPSHOT.jar")
    lib = os.path.join(app, "lib/*")
    timeout = int(config["evaluation"]["timeout"])

    out_result_path = str(out_result).split("results.txt")[0]
    query_plan_path = str(query_plan).split("query_plan.txt")[0]
    stats_path = str(stats).split("stats.csv")[0]

    print(force_source_selection)
    args = [engine_config, query, out_result_path, out_source_selection, query_plan_path, stats_path, str(timeout)]+ force_source_selection
    print(args)
    args = " ".join(args)
    #timeoutCmd = f'timeout --signal=SIGKILL {timeout}' if timeout != 0 else ""
    timeoutCmd = ""
    cmd = f'{timeoutCmd} java -classpath "{jar}:{lib}" org.example.FedX {args}'.strip()

    logger.debug("=== FedX RWSS ===")
    logger.debug(cmd)
    logger.debug("============")
    
    fedx_proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    try:        
        fedx_proc.wait(timeout)
        if fedx_proc.returncode == 0:
            logger.info(f"{query} benchmarked sucessfully")
            #if os.stat(out_result).st_size == 0:
            #    logger.error(f"{query} yield no results!")
            #    write_empty_result(out_result)
            #    raise RuntimeError(f"{query} yield no results!")
            create_stats(stats)
        else:
            logger.error(f"{query} reported error")    
            #write_empty_result(out_result)
            if not os.path.exists(stats):
                create_stats(stats, "error_runtime")                  
    except subprocess.TimeoutExpired: 
        logger.exception(f"{query} timed out!")
        create_stats(stats, "timeout")
        #write_empty_result(out_result)                   
    finally:
        os.system('pkill -9 -f "FedX"')
        #kill_process(fedx_proc.pid)

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

    intermediate_provenance_files = []
    intermediate_result_files = []
    intermediate_qplan_files = []
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
            
        #fedx.exec_fedx(
        #    eval_config, engine_config, query, 
        #    str(intermediate_result_file), "/dev/null", str(query_plan), 
        #    str(intermediate_stats_file), str(intermediate_provenance_file), batch_id
        #)
             
        #os.system(f"rm {intermediate_provenance_file}")
        
        intermediate_provenance_files.append(str(intermediate_provenance_file))
        intermediate_result_files.append(str(intermediate_result_file))
        intermediate_qplan_files.append(str(intermediate_qplan_file))
        intermediate_stats_files.append(str(intermediate_stats_file))

    Path(out_result).touch()
    Path(query_plan).touch()
    Path(stats).touch()

    exec_fedx(
        eval_config, engine_config, query, 
        str(out_result), "/dev/null", str(query_plan), 
        str(stats), intermediate_provenance_files, batch_id
    )

    stats_df = pd.concat([ pd.read_csv(f) for f in intermediate_stats_files ], ignore_index=True)
    stats_df.groupby(["query", "batch", "instance", "engine", "attempt"]).sum() \
        .reset_index() \
        .to_csv(stats, index=False)
        
    os.system(f"cat {' '.join(intermediate_result_files)} > {out_result}")
    os.system(f"cat {' '.join(intermediate_qplan_files)} > {query_plan}")
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
@click.argument("batch_id", type=click.INT)
@click.argument("endpoint", type=str)
@click.pass_context
def generate_config_file(ctx: click.Context, datafiles, outfile, eval_config, batch_id, endpoint):
    ctx.invoke(fedx.generate_config_file, datafiles=datafiles, outfile=outfile, eval_config=eval_config, endpoint=endpoint)
    

if __name__ == "__main__":
    cli()