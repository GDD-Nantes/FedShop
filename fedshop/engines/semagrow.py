# Import part
from io import BytesIO, StringIO
import json
import os
import re
import shutil
import psutil
import click
import glob
import subprocess
import pandas as pd
import numpy as np
from pathlib import Path
import requests
from sklearn.preprocessing import LabelEncoder
from rdflib import ConjunctiveGraph

import sys

from ..query import execute_query
from ..utils import load_config, fedshop_logger, str2n3, create_stats, create_stats
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
    app_config = load_config(eval_config)["evaluation"]["engines"]["semagrow"]
    
    #if not os.path.exists(app) or not os.path.exists(jar) or os.path.exists(lib):
    oldcwd = os.getcwd()

    
    for build_loc in [Path(app_config["dir"]).absolute(), Path(app_config["summary_generator_dir"]).absolute()]:
        os.chdir(build_loc)   
        java_version = subprocess.check_output("jenv version", shell=True).decode().split()[0]
        java_home = f"{os.environ['HOME']}/.jenv/versions/{java_version}"
        if os.system(f"mvn clean && JAVA_HOME={java_home} mvn install dependency:copy-dependencies package -Dmaven.test.skip=true") != 0:
            raise RuntimeError(f"Could not compile {build_loc}")
        
    # os.chdir(Path(app_config["summary_generator_dir"]).absolute() + "/assembly/target/")
    # os.system("tar xzvf sevod-scraper-3-SNAPSHOT-dist.tar.gz")
    os.chdir(oldcwd)

@cli.command()
@click.argument("eval-config", type=click.Path(exists=True, file_okay=True, dir_okay=True))
@click.argument("query", type=click.Path(exists=True, file_okay=True, dir_okay=True))
@click.option("--out-result", type=click.Path(exists=False, file_okay=True, dir_okay=True), default="/dev/null")
@click.option("--out-source-selection", type=click.Path(exists=False, file_okay=True, dir_okay=True), default="/dev/null")
@click.option("--query-plan", type=click.Path(exists=False, file_okay=True, dir_okay=True), default="/dev/null")
@click.option("--stats", type=click.Path(exists=False, file_okay=True, dir_okay=True), default="/dev/null")
@click.option("--force-source-selection", type=click.Path(exists=False, file_okay=True, dir_okay=True), default="")
@click.option("--batch-id", type=click.INT, default=-1)
@click.option("--noexec", is_flag=True, default=False)
@click.pass_context
def run_benchmark(ctx: click.Context, eval_config, query, out_result, out_source_selection, query_plan, stats, force_source_selection, batch_id, noexec):
    """Execute the workload instance then its associated source selection query.
    
    Expected output:
    - results.txt: containing the results for the query
    - source_selection.txt: containing the source selection for the query
    - stats.csv: containing the execution time, http_requests for the query

    Args:
        ctx (click.Context): _description_
        eval_config (_type_): _description_
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
    
    timeout = int(config["evaluation"]["timeout"])
    
    summary_file = f"summaries/metadata-fedshop-batch{batch_id}.ttl"   
    repo_file = f"summaries/repo-fedshop-batch{batch_id}.ttl"
    
    proxy_host = config["evaluation"]["proxy"]["host"]
    proxy_port = config["evaluation"]["proxy"]["port"]
    proxy_server = config["evaluation"]["proxy"]["endpoint"]
    
    # Reset the proxy stats
    if requests.get(proxy_server + "reset").status_code != 200:
        raise RuntimeError("Could not reset statistics on proxy!")
    
    #cmd = f"./semagrow.sh "
    out_result = os.path.realpath(out_result)
    out_source_selection = os.path.realpath(out_source_selection)
    query_plan = os.path.realpath(query_plan)
    query = os.path.realpath(query)

    tmp_results_file = Path(out_result).with_suffix('.csv')
    noexec = "--noexec" if noexec else ""
    timeout_cmd = f'timeout --signal=SIGKILL {timeout}' if timeout != 0 else ""
    cmd = f'{timeout_cmd} mvn exec:java -Dhttp.proxyHost="{proxy_host}" -Dhttp.proxyPort="{proxy_port}" -Dhttp.nonProxyHosts="" -pl "rdf4j/" -Dexec.mainClass="org.semagrow.cli.CliMain" -Dexec.args="--query {query} --output {tmp_results_file} --config {repo_file} --metadata {summary_file} {noexec}"'

    logger.debug("=== Semagrow ===")
    logger.debug(cmd)
    logger.debug("============")

    os.chdir(Path(app))

    shutil.copy(repo_file, "repository.ttl")
    shutil.copy(summary_file, "metadata.ttl")
        
    semagrow_proc = subprocess.Popen(cmd.strip(), shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    
    failed_reason = None
    
    try:        
        semagrow_proc.wait(timeout)
        if semagrow_proc.returncode == 0:
            logger.info(f"{query} benchmarked sucessfully")
            
            shutil.copy(tmp_results_file, out_result)

            try: 
                results_df = pd.read_csv(out_result).replace("null", None)
                if results_df.empty or os.stat(out_result).st_size == 0: 
                    logger.error(f"{query} yield no results!")
                    Path(out_source_selection).touch()
                    failed_reason = "error_runtime"

            except pd.errors.EmptyDataError:
                logger.error(f"{query} yield no results!")
                Path(out_source_selection).touch()
                failed_reason = "error_runtime"

        else:
            logger.error(f"{query} reported error")    
            failed_reason = "error_runtime"
            
    except subprocess.TimeoutExpired: 
        logger.exception(f"{query} timed out!")        
        failed_reason = "timeout"
        
    finally:
        os.system('pkill -9 -f "mainClass=org.semagrow.cli.CliMain"')
        #cache_file = f"{app}/cache.db"
        #Path(cache_file).unlink(missing_ok=True)
        #kill_process(fedx_proc.pid)    
    
    # Write stats
    if stats != "/dev/null":            
        # Write proxy stats
        proxy_stats = json.loads(requests.get(proxy_server + "get-stats").text)

        stats_home = Path(stats).parent
        Path(stats_home).mkdir(parents=True, exist_ok=True)
        
        with open(f"{stats_home}/http_req.txt", "w") as http_req_fs:
            http_req = proxy_stats["NB_HTTP_REQ"]
            http_req_fs.write(str(http_req))
            
        with open(f"{stats_home}/ask.txt", "w") as http_ask_fs:
            http_ask = proxy_stats["NB_ASK"]
            http_ask_fs.write(str(http_ask))
            
        with open(f"{stats_home}/data_transfer.txt", "w") as data_transfer_fs:
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
    with open(infile, "r") as in_fs:
        content = in_fs.read().strip()
        if len(content) == 0:
            Path(outfile).touch(exist_ok=False)
        else:
            shutil.copy(infile, outfile)

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
        fedx_pattern = r"StatementPattern\s+?Var\s+\((name=\w+,\s+value=(.*),\s+anonymous|name=(\w+))\)\s+Var\s+\((name=\w+,\s+value=(.*),\s+anonymous|name=(\w+))\)\s+Var\s+\((name=\w+,\s+value=(.*),\s+anonymous|name=(\w+))\)"
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
            open(os.path.join(Path(prefix_cache).parent, "composition.json"), "r") as comp_fs \
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
      
    engine_dir = config["evaluation"]["engines"]["semagrow"]["dir"]
    summary_generator_dir = config["evaluation"]["engines"]["semagrow"]["summary_generator_dir"]

    summary_file = os.path.realpath(os.path.join(engine_dir, f"summaries/metadata-fedshop-batch{batch_id}.ttl"))
    repo_file = os.path.realpath(os.path.join(engine_dir, f"summaries/repo-fedshop-batch{batch_id}.ttl"))

    default_endpoint = config["generation"]["virtuoso"]["default_endpoint"]
    
    Path(summary_file).parent.mkdir(parents=True, exist_ok=True)
    can_create_repo = False
    
    if os.path.exists(repo_file):
        with open(repo_file, "r") as f:
            repo_txt = f.read()
            can_create_repo = summary_file not in repo_txt
    else:
        can_create_repo = True

    if can_create_repo:
        with open(repo_file, "w") as repo:
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
    
    oldcwd = os.getcwd()
    os.chdir(Path(summary_generator_dir))   
        
    proxy_mapping = {}
    with open(proxy_mapping_file, "r") as pmfs:
        proxy_mapping = json.load(pmfs)
    
    endpoints = list(proxy_mapping.values())
        
    update_summary = False

    if os.path.exists(summary_file):
        with open(summary_file, "r") as f:
            summary_txt = f.read()
            update_summary = not all([endpoint in summary_txt for endpoint in endpoints])
    else:
        update_summary = True

    if update_summary:

        summary_graph = ConjunctiveGraph()

        for i, (graph, endpoint) in enumerate(proxy_mapping.items()):
            logger.info(f"Generating summary for batch {batch_id}")
            tmp_summary_file = f"tmp{i}.ttl"
            cmd = f'mvn -q exec:java -pl "cli/" -Dexec.mainClass="org.semagrow.sevod.scraper.cli.Main" -Dexec.args="--sparql --input {endpoint} --output {tmp_summary_file}"'                  
            logger.debug(cmd)
            if os.system(cmd) != 0: raise RuntimeError(f"Could not generate {tmp_summary_file}")

            if os.system(f'sed -i "s|_:DatasetRoot|<http://example.org/DatasetRoot>|g" {tmp_summary_file}') != 0: raise RuntimeError()
            if os.system(f'sed -i "s|_:Dataset1|<http://example.org/Dataset{i}>|g" {tmp_summary_file}') != 0: raise RuntimeError()

            summary_graph.parse(tmp_summary_file, format="turtle")
            os.remove(tmp_summary_file)
        
        summary_graph.serialize(destination=summary_file, format="turtle")
    
    os.chdir(oldcwd)

if __name__ == "__main__":
    cli()