# Import part
from io import BytesIO, StringIO
import json
import os
import re
import psutil
import click
import glob
import subprocess
import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.preprocessing import LabelEncoder

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
    app_config = load_config(eval_config)["evaluation"]["engines"]["semagrow"]
    app = app_config["dir"]
    
    #if not os.path.exists(app) or not os.path.exists(jar) or os.path.exists(lib):
    oldcwd = os.getcwd()
    os.chdir(Path(app))
    #os.system("rm -rf **/target && mvn clean && mvn install dependency:copy-dependencies package")
    os.system("mvn clean && mvn install dependency:copy-dependencies package")
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
@click.pass_context
def run_benchmark(ctx: click.Context, eval_config, engine_config, query, out_result, out_source_selection, query_plan, stats, force_source_selection, batch_id):
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
    
    config = load_config(eval_config)
    app_config = config["evaluation"]["engines"]["semagrow"]
    app = app_config["dir"]
    last_batch = int(config["generation"]["n_batch"]) - 1
    
    endpoint = config["generation"]["virtuoso"]["endpoints"][last_batch]
    compose_file = config["generation"]["virtuoso"]["compose_file"]
    service_name = config["generation"]["virtuoso"]["service_name"]
    container_name = config["generation"]["virtuoso"]["container_names"][last_batch]
    timeout = int(config["evaluation"]["timeout"])

    oldcwd = os.getcwd()
    summary_file = f"metadata{batch_id}.ttl"   
    cmd = f"./semagrow.sh repository.ttl {endpoint} ../../{out_result} ../../{out_source_selection} ../../{query_plan} {timeout} ../../{query} {out_result.split('/')[7].split('_')[1]} false true {summary_file}"

    logger.debug("=== Semagrow ===")
    logger.debug(cmd)
    logger.debug("============")

    os.chdir(Path(app))
    semagrow_proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    os.chdir(oldcwd)
    try:        
        semagrow_proc.wait(timeout)
        if semagrow_proc.returncode == 0:
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
            
            if results_df.dropna().empty or os.stat(out_result).st_size == 0:            
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
        os.system('pkill -9 -f "rdf4j/target"')
        #cache_file = f"{app}/cache.db"
        #Path(cache_file).unlink(missing_ok=True)
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
    with open(infile, "r") as input_file:
        with open(outfile, "w") as output_file:
            for line in input_file:
                output_file.write(line)

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
        fedx_pattern = r"StatementPattern\s+?Var\s+\((name=\w+;\s+value=(.*),\s+anonymous|name=(\w+))\)\s+Var\s+\((name=\w+,\s+value=(.*),\s+anonymous|name=(\w+))\)\s+Var\s+\((name=\w+,\s+value=(.*),\s+anonymous|name=(\w+))\)"
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
        
        #print(result)
        return result
    
    def lookup_composition(x: str):
        result = re.sub(r"[\[\]]", "", x).strip()
        for prefix, alias in prefix2alias.items():
            result = re.sub(rf"<{re.escape(prefix)}(\w+)>", rf"{alias}:\1", result)
                        
        return inv_comp[result] 
    
    def pad(x):
        encoder = LabelEncoder()
        encoded = encoder.fit_transform(x)
        result = np.pad(encoded, (0, max_length-len(x)), mode="constant", constant_values=-1)                
        decoded = [ encoder.inverse_transform([item]).item() if item != -1 else "" for item in result ]
        #print(decoded)
        return decoded
    
    clean = "tps;sources\n"
    clean = clean + open(infile).read().replace(')\n', ')').replace('n\n', 'n')
    in_df = pd.read_csv(StringIO(clean), sep=';')
    in_df = in_df.groupby('tps')['sources'].apply(list).reset_index(name='sources')
    
    # df_new = in_df.groupby('tps')['sources'].apply(list).reset_index(name='sources')
    # #print(df_new)
    # os.remove(tmp_file)
    # #print(in_df)
    
    with    open(prefix_cache, "r") as prefix_cache_fs, \
            open(os.path.join(Path(prefix_cache).parent, "provenance.sparql.comp"), "r") as comp_fs \
    :
        prefix2alias = json.load(prefix_cache_fs)    
        composition = json.load(comp_fs)
                
        comp = { k: " ".join(v) for k, v in composition.items() }
        inv_comp = {}
        for k,v in comp.items():
            if inv_comp.get(v) is None:
                inv_comp[v] = []
            inv_comp[v].append(k) 
                                    
        in_df["tps"] = in_df["tps"].apply(extract_triple)
        in_df["tp_name"] = in_df["tps"].apply(lookup_composition)
        in_df = in_df.explode("tp_name")
        
        in_df["tp_number"] = in_df["tp_name"].str.replace("tp", "", regex=False).astype(int)
        in_df.sort_values("tp_number", inplace=True)
        
        # If unequal length (as in union, optional), fill with nan
        max_length = in_df["sources"].apply(len).max()
        in_df["sources"] = in_df["sources"].apply(pad)
        
        out_df = in_df.set_index("tp_name")["sources"] \
            .to_frame().T \
            .apply(pd.Series.explode) \
            .reset_index(drop=True) 
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
    
    summary_file = f"metadata{batch_id}.ttl"     
    app_config = load_config(eval_config)["evaluation"]["engines"]["semagrow"]
    app = app_config["dir"]   

    oldcwd = os.getcwd()
    os.chdir(Path(app))        

    with open("repository.ttl", "w+") as repo:
        repo.write("################################################################################\n")
        repo.write("# Sesame configuration for SemaGrow\n")
        repo.write("#\n")
        repo.write("# ATTENTION: the Sail implementing the sail:sailType must be published\n")
        repo.write("#            in META-INF/services/org.openrdf.sail.SailFactory\n")
        repo.write("################################################################################\n")
        repo.write("@prefix void: <http://rdfs.org/ns/void#>.\n")
        repo.write("@prefix rep:  <http://www.openrdf.org/config/repository#>.\n")
        repo.write("@prefix sr:   <http://www.openrdf.org/config/repository/sail#>.\n")
        repo.write("@prefix sail: <http://www.openrdf.org/config/sail#>.\n")
        repo.write("@prefix semagrow: <http://schema.semagrow.eu/>.\n")
        repo.write("@prefix quetsal: <http://quetsal.aksw.org/>.\n")
        repo.write("\n")
        repo.write("[] a rep:Repository ;\n")
        repo.write("\trep:repositoryTitle \"SemaGrow Repository\" ;\n")
        repo.write("\trep:repositoryID \"semagrow\" ;\n")
        repo.write("\trep:repositoryImpl [\n")
        repo.write("\t\trep:repositoryType \"semagrow:SemagrowRepository\" ;\n")
        repo.write("\t\tsr:sailImpl [\n")
        repo.write("\t\t\tsail:sailType \"semagrow:SemagrowSail\" ;\n")
        repo.write(f"\t\t\tsemagrow:metadataInit \"{summary_file}\" ;\n")
        repo.write("\t\t\tsemagrow:executorBatchSize \"8\"\n")
        repo.write("\t\t]\n")
        repo.write("\t] .")
    
    update_summary = not os.path.exists(summary_file)
    if not update_summary:
        print(f"Looking for {endpoint} in {summary_file}")
        with open(summary_file, "r") as sfs:
            update_summary = endpoint not in sfs.read()

    if update_summary:
        try:
            logger.info(f"Generating summary for batch {batch_id}")
            cmd = f"./semagrow.sh repository.ttl {endpoint} ignore ignore ignore ignore ignore ignore {batch_id} true false {summary_file}"
            logger.debug(cmd)
            if os.system(cmd) != 0: raise RuntimeError(f"Could not generate {summary_file}")
        except InterruptedError:
            Path(summary_file).unlink(missing_ok=True)
    
    os.chdir(oldcwd)
    ctx.invoke(fedx.generate_config_file, datafiles=datafiles, outfile=outfile, eval_config=eval_config, endpoint=endpoint)

if __name__ == "__main__":
    cli()