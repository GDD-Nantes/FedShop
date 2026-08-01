# Import part
import json
import os
import re
import textwrap
import click
import subprocess
import pandas as pd
import numpy as np
from pathlib import Path
import requests
from sklearn.preprocessing import LabelEncoder

import sys

from ..utils import load_config, fedshop_logger, str2n3, create_stats
logger = fedshop_logger(Path(__file__).name)

@click.group
def cli():
    pass

@cli.command()
@click.argument("eval-config", type=click.Path(exists=True, file_okay=True, dir_okay=True))
def prerequisites(eval_config):
    """Obtain prerequisite artifact for engine, e.g, compile binaries, setup dependencies, etc.

    Args:
        eval_config (_type_): _description_
    """
    
    config = load_config(eval_config)
    engine_dir = config["evaluation"]["engines"]["fedx"]["dir"]
    jar = os.path.join(engine_dir, "target", "FedX-1.0-SNAPSHOT.jar")
    lib = os.path.join(engine_dir, "target", "lib/*")
    
    #if not os.path.exists(app) or not os.path.exists(jar) or os.path.exists(lib):
    oldcwd = os.getcwd()
    os.chdir(engine_dir)
    if os.system("mvn clean && mvn install dependency:copy-dependencies package") != 0:
        raise RuntimeError("Could not compile FedX")
    os.chdir(oldcwd)
        
def exec_fedx(eval_config, query, out_result, out_source_selection, query_plan, stats, batch_id, noexec):
    config = load_config(eval_config)
    engine_dir = config["evaluation"]["engines"]["fedx"]["dir"]
    engine_config = f"target/config/config_batch{batch_id}.ttl"
    timeout = int(config["evaluation"]["timeout"])
    
    proxy_server = config["evaluation"]["proxy"]["endpoint"]
    proxy_host = config["evaluation"]["proxy"]["host"]
    proxy_port = config["evaluation"]["proxy"]["port"]
    
    # Reset the proxy stats
    if requests.get(proxy_server + "reset").status_code != 200:
        raise RuntimeError("Could not reset statistics on proxy!")

    args = [engine_config, query, out_result, out_source_selection, query_plan, str(timeout+10), str(noexec).lower()]
    args = " ".join(args)
    timeoutCmd = f'timeout --signal=SIGKILL {timeout}' if timeout != 0 else ""
    #timeoutCmd = ""
    cmd = f'{timeoutCmd} mvn exec:java -Dhttp.proxyHost="{proxy_host}" -Dhttp.proxyPort="{proxy_port}" -Dhttp.nonProxyHosts="" -Dexec.mainClass="org.example.FedX" -Dexec.args="{args}"'.strip()

    logger.debug("=== FedX ===")
    logger.debug(cmd)
    logger.debug("============")

    os.chdir(Path(engine_dir))
    fedx_proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    failed_reason = None
    try:        
        fedx_proc.wait(timeout)
        if fedx_proc.returncode == 0:
            logger.info(f"{query} benchmarked sucessfully")
        else:
            logger.error(f"{query} reported error")    
            if not os.path.exists(stats):
                failed_reason = "error_runtime"
    except subprocess.TimeoutExpired: 
        logger.exception(f"{query} timed out!")
        failed_reason = "timeout"
    finally:
        os.system('pkill -9 -f "FedX-1.0-SNAPSHOT.jar"')

    # Write stats
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
    Path(out_result).touch()
    Path(out_source_selection).touch()
    Path(query_plan).touch()

    query = os.path.realpath(query)
    out_result = os.path.realpath(out_result)
    out_source_selection = os.path.realpath(out_source_selection)
    query_plan = os.path.realpath(query_plan)
    stats = os.path.realpath(stats)

    exec_fedx(eval_config, query, out_result, out_source_selection, query_plan, stats, batch_id, noexec)

@cli.command()
@click.argument("infile", type=click.Path(exists=False, file_okay=True, dir_okay=False))
@click.argument("outfile", type=click.Path(exists=False, file_okay=True, dir_okay=False))
def transform_results(infile, outfile):
    if os.stat(infile).st_size == 0:
        Path(outfile).touch()
        return
            
    with open(infile, "r") as in_fs:
        records = []
        for line in in_fs.readlines():            
            bindings = re.sub(r"(\[|\])", "", line.strip()).split(";")
            record = dict()
            for binding in bindings:
                b = binding.split("=")
                key = b[0]
                value = "".join(b[1:])
                value = re.sub(r"\"(.*)\"(\^\^|@).*", r"\1", value)
                value = value.replace('"', "")
                record[key] = value
            records.append(record)
            
        result = pd.DataFrame.from_records(records)
        result.to_csv(outfile, index=False)

@cli.command()
@click.argument("infile", type=click.Path(exists=False, file_okay=True, dir_okay=False))
@click.argument("outfile", type=click.Path(exists=False, file_okay=True, dir_okay=False))
@click.argument("composition-file", type=click.Path(exists=False, file_okay=True, dir_okay=False))
def transform_provenance(infile, outfile, composition_file):
    
    with open(infile, "r") as ifs:
        if len(ifs.read().strip()) == 0:
            Path(outfile).touch()
            logger.debug(f"{infile} is empty!")
            return
    
    def extract_triple(x):
        fedx_pattern = r"StatementPattern\s+(\(new scope\)\s+)?Var\s+\((name=\w+,\s+value=(.*),\s+anonymous|name=(\w+))\)\s+Var\s+\((name=\w+,\s+value=(.*),\s+anonymous|name=(\w+))\)\s+Var\s+\((name=\w+,\s+value=(.*),\s+anonymous|name=(\w+))\)"
        match = re.match(fedx_pattern, x)
        
        s = match.group(3) or match.group(4)
        p = match.group(6) or match.group(7)
        o = match.group(9) or match.group(10)
        
        result = " ".join([s, p, o])
        
        return result

    def extract_source_selection(x):
        fex_pattern = r"StatementSource\s+\(id=sparql_([a-z]+(\.\w+)+\.[a-z]+)_,\s+type=[A-Z]+\)"
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
        
    with open(composition_file, "r") as comp_fs:
        composition = json.load(comp_fs)
        inv_composition = {f"{' '.join(v)}": k for k, v in composition.items()}
                        
        in_df["triple"] = in_df["triple"].apply(extract_triple)
        in_df["tp_name"] = in_df["triple"].apply(lookup_composition)
        in_df["tp_number"] = in_df["tp_name"].str.replace("tp", "", regex=False).astype(int)
        in_df.sort_values("tp_number", inplace=True)
        in_df["source_selection"] = in_df["source_selection"].apply(extract_source_selection)

        # If unequal length (as in union, optional), fill with nan
        max_length = in_df["source_selection"].apply(len).max()
        #in_df["source_selection"] = in_df["source_selection"].apply(pad)
                
        out_df = in_df.set_index("tp_name")["source_selection"] \
            .to_frame().T \
            .apply(pd.Series.explode) \
            .reset_index(drop=True) 
        out_df.to_csv(outfile, index=False)

@cli.command()
@click.argument("eval-config", type=click.Path(exists=True, dir_okay=False, file_okay=True))
@click.argument("batch_id", type=click.INT)
@click.pass_context
def generate_config_file(ctx: click.Context, eval_config, batch_id):
    
    # Load config
    conf = load_config(eval_config)
    proxy_mapping_file = os.path.realpath(
        os.path.join(conf["generation"]["workdir"], f"virtuoso-proxy-mapping-batch{batch_id}.json")
    )
    
    engine_dir = conf["evaluation"]["engines"]["fedx"]["dir"]   
    
    oldcwd = os.getcwd()
    os.chdir(Path(engine_dir))  
    logger.info(f"Generating endpoints file for batch {batch_id}...")

    endpoints_file = f"target/config/config_batch{batch_id}.ttl"
    Path(endpoints_file).parent.mkdir(parents=True, exist_ok=True)

    # Generate the endpoints file
    endpoints = {}
    with open(proxy_mapping_file, "r") as pmfs:
        proxy_mapping = json.load(pmfs)
        federation_members = conf["generation"]["virtuoso"]["federation_members"]
        for federation_member_iri in federation_members[f"batch{batch_id}"].values():
            target_endpoint = proxy_mapping[federation_member_iri]
            endpoints[federation_member_iri] = target_endpoint
    
    update_required = False
    if is_file_exists := os.path.exists(endpoints_file):
        with open(endpoints_file) as f:
            for endpoint in endpoints.values():
                search_string = f'sd:endpoint "{endpoint}'
                if update_required := search_string not in f.read():
                    break
                

    if update_required or not is_file_exists:
        Path(endpoints_file).parent.mkdir(parents=True, exist_ok=True)
        with open(endpoints_file, "w") as ffile:
            ffile.write(textwrap.dedent(
                f"""
                @prefix sd: <http://www.w3.org/ns/sparql-service-description#> .
                @prefix fedx: <http://rdf4j.org/config/federation#> .

                """
            ))

            for graph_uri, endpoint in endpoints.items():
                ffile.write(textwrap.dedent(
                    f"""
                    <{graph_uri}> a sd:Service ;
                        fedx:store "SPARQLEndpoint";
                        sd:endpoint "{endpoint}";
                        fedx:supportsASKQueries true .   

                    """
                ))

if __name__ == "__main__":
    cli()