# Import part
import json
import os
import re
import click
import subprocess
import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.preprocessing import LabelEncoder

import sys
sys.path.append(str(os.path.join(Path(__file__).parent.parent)))

from utils import load_config, rsfb_logger, str2n3, write_empty_result, create_stats
logger = rsfb_logger(Path(__file__).name)

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
    
    app_config = load_config(eval_config)["evaluation"]["engines"]["fedx"]
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
    app_config = config["evaluation"]["engines"]["fedx"]
    app = app_config["dir"]
    jar = os.path.join(app, "FedX-1.0-SNAPSHOT.jar")
    lib = os.path.join(app, "lib/*")
    timeout = int(config["evaluation"]["timeout"])

    args = [engine_config, query, out_result, out_source_selection, query_plan, str(timeout), force_source_selection]
    args = " ".join(args)
    #timeoutCmd = f'timeout --signal=SIGKILL {timeout}' if timeout != 0 else ""
    timeoutCmd = ""
    cmd = f'{timeoutCmd} java -classpath "{jar}:{lib}" org.example.FedX {args}'.strip()

    logger.debug("=== FedX ===")
    logger.debug(cmd)
    logger.debug("============")
    
    fedx_proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    try:        
        fedx_proc.wait(timeout)
        if fedx_proc.returncode == 0:
            logger.info(f"{query} benchmarked sucessfully")
            if os.stat(out_result).st_size == 0:
                logger.error(f"{query} yield no results!")
                write_empty_result(out_result)
                raise RuntimeError(f"{query} yield no results!")
            create_stats(stats)
        else:
            logger.error(f"{query} reported error")    
            write_empty_result(out_result)
            if not os.path.exists(stats):
                create_stats(stats, "error_runtime")                  
    except subprocess.TimeoutExpired: 
        logger.exception(f"{query} timed out!")
        create_stats(stats, "timeout")
        write_empty_result(out_result)                   
    finally:
        os.system('pkill -9 -f "FedX-1.0-SNAPSHOT.jar"')
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
    exec_fedx(eval_config, engine_config, query, out_result, out_source_selection, query_plan, stats, "", batch_id)

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
@click.argument("prefix-cache", type=click.Path(exists=False, file_okay=True, dir_okay=False))
def transform_provenance(infile, outfile, prefix_cache):
    
    def extract_triple(x):
        fedx_pattern = r"StatementPattern\s+(\(new scope\)\s+)?Var\s+\((name=\w+,\s+value=(.*),\s+anonymous|name=(\w+))\)\s+Var\s+\((name=\w+,\s+value=(.*),\s+anonymous|name=(\w+))\)\s+Var\s+\((name=\w+,\s+value=(.*),\s+anonymous|name=(\w+))\)"
        match = re.match(fedx_pattern, x)
        
        s = match.group(3)
        if s is None: s = f"?{match.group(4)}"
        
        p = match.group(6)
        if p is None: p = f"?{match.group(7)}"
        
        o = match.group(9) 
        if o is None: o = f"?{match.group(10)}"
        
        result = " ".join([s, p, o])
                
        for prefix, alias in prefix2alias.items():
            result = result.replace(prefix, f"{alias}:")
            
        if s.startswith("http"):
            result = result.replace(s, str2n3(s))
            
        if o.startswith("http"):
            result = result.replace(o, str2n3(o))
        
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
    
    with open(prefix_cache, "r") as prefix_cache_fs, open(os.path.join(Path(prefix_cache).parent, "provenance.sparql.comp"), "r") as comp_fs:
        prefix2alias = json.load(prefix_cache_fs)    
        composition = json.load(comp_fs)
        inv_composition = {f"{' '.join(v)}": k for k, v in composition.items()}
                        
        in_df["triple"] = in_df["triple"].apply(extract_triple)
        in_df["tp_name"] = in_df["triple"].apply(lookup_composition)
        in_df["tp_number"] = in_df["tp_name"].str.replace("tp", "", regex=False).astype(int)
        in_df.sort_values("tp_number", inplace=True)
        in_df["source_selection"] = in_df["source_selection"].apply(extract_source_selection)

        # If unequal length (as in union, optional), fill with nan
        max_length = in_df["source_selection"].apply(len).max()
        in_df["source_selection"] = in_df["source_selection"].apply(pad)
        
        out_df = in_df.set_index("tp_name")["source_selection"] \
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
    
    is_endpoint_updated = False
    if is_file_exists := os.path.exists(outfile):
        with open(outfile) as f:
            search_string = f'sd:endpoint "{endpoint}'
            is_endpoint_updated = search_string not in f.read()

    if is_endpoint_updated or not is_file_exists:
        ssite = set()
        #for data_file in glob.glob(f'{dir_data_file}/*.nq'):
        for data_file in datafiles:
            with open(data_file, "r") as file:
                t_file = file.readlines()
                for line in t_file:
                    site = line.rsplit()[-2]
                    site = re.search(r"<(.*)>", site).group(1)
                    ssite.add(site)
        
        outfile = Path(outfile)
        outfile.parent.mkdir(parents=True, exist_ok=True)
        outfile.touch()
        with outfile.open("w") as ffile:
            ffile.write(
"""
@prefix sd: <http://www.w3.org/ns/sparql-service-description#> .
@prefix fedx: <http://rdf4j.org/config/federation#> .

"""
            )
            for s in sorted(ssite):
                ffile.write(
f"""
<{s}> a sd:Service ;
    fedx:store "SPARQLEndpoint";
    sd:endpoint "{endpoint}/?default-graph-uri={s}";
    fedx:supportsASKQueries false .   

"""
                )

if __name__ == "__main__":
    cli()