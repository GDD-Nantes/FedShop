# Import part
import json
import os
import re
import click
import glob
import subprocess
import pandas as pd
import numpy as np
from pathlib import Path

import sys
sys.path.append(str(os.path.join(Path(__file__).parent.parent)))

from utils import kill_process, load_config, str2n3
# Example of use : 
# python3 utils/generate-fedx-config-file.py experiments/bsbm/model/vendor test/out.ttl

# Goal : Generate a configuration file for RDF4J to set the use of named graph as endpoint thanks to data file

@click.group
def cli():
    pass

@cli.command()
@click.argument("eval-config", type=click.Path(exists=True, file_okay=True, dir_okay=True))
def prerequisites(eval_config):
    app_config = load_config(eval_config)["evaluation"]["engines"]["fedx"]
    app = app_config["dir"]
    jar = os.path.join(app, "Federapp-1.0-SNAPSHOT.jar")
    lib = os.path.join(app, "lib/*")
    
    #if not os.path.exists(app) or not os.path.exists(jar) or os.path.exists(lib):
    oldcwd = os.getcwd()
    os.chdir(Path(app).parent)
    os.system("mvn clean && mvn install dependency:copy-dependencies package")
    os.chdir(oldcwd)

@cli.command()
@click.argument("eval-config", type=click.Path(exists=True, file_okay=True, dir_okay=True))
@click.argument("fedx-config", type=click.Path(exists=True, file_okay=True, dir_okay=True))
@click.argument("query", type=click.Path(exists=True, file_okay=True, dir_okay=True))
@click.argument("result", type=click.Path(exists=False, file_okay=True, dir_okay=True))
@click.argument("stats", type=click.Path(exists=False, file_okay=True, dir_okay=True))
@click.option("--ideal-ss", type=click.Path(exists=False, file_okay=True, dir_okay=True), default="")
@click.pass_context
def run_benchmark(ctx: click.Context, eval_config, fedx_config, query, result, stats, ideal_ss):

    app_config = load_config(eval_config)["evaluation"]["engines"]["fedx"]
    app = app_config["dir"]
    jar = os.path.join(app, "Federapp-1.0-SNAPSHOT.jar")
    lib = os.path.join(app, "lib/*")
    timeout = int(app_config["timeout"])

    r_root = Path(result).parent

    #stat = f"{r_root}/stats.csv"

    args = [fedx_config, query, result, stats, ideal_ss]
    args = " ".join(args)
    #timeoutCmd = f'timeout --signal=SIGKILL {timeout}' if timeout != 0 else ""
    timeoutCmd = ""
    cmd = f'{timeoutCmd} java -classpath "{jar}:{lib}" org.example.Federapp {args}'.strip()
    
    def write_empty_stats():
        with open(stats, "w+") as fout:
            fout.write("query;engine;instance;batch;mode;exec_time;distinct_ss\n")
            basicInfos = re.match(r".*/(\w+)/(q\d+)/instance_(\d+)/batch_(\d+)/(\w+)/results", result)
            engine = basicInfos.group(1)
            queryName = basicInfos.group(2)
            instance = basicInfos.group(3)
            batch = basicInfos.group(4)
            mode = basicInfos.group(5)
            fout.write(";".join([queryName, engine, instance, batch, mode, "nan", "nan"])+"\n")
            fout.close()
    
    def write_empty_result(msg):
        with open(result, "w+") as fout:
            fout.write(msg)
            fout.close()
    print("=== FedX ===")
    print(cmd)
    print("============")
    
    fedx_proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    try: 
        #output, error = fedx_proc.communicate(timeout=timeout)
        # with open(result + ".log", "w+") as watdivWriter:
        #     for line in iter(fedx_proc.stderr.readline, b''):
        #         watdivWriter.write(line.decode())
        #     watdivWriter.close()  
        
        fedx_proc.wait(timeout=timeout)
        #fedx_proc = subprocess.run(cmd, shell=True, timeout=timeout, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
        if fedx_proc.returncode == 0:
            print(f"{query} benchmarked sucessfully")
        else:
            print(f"{query} reported error")      
            # write_empty_stats()
            # write_empty_result("error")
            raise RuntimeError(f"{query} reported error")
            
    except subprocess.TimeoutExpired: 
        print(f"{query} timed out!")
        write_empty_stats()
        write_empty_result("timeout")            
        kill_process(fedx_proc.pid)

@cli.command()
@click.argument("infile", type=click.Path(exists=False, file_okay=True, dir_okay=False))
@click.argument("outfile", type=click.Path(exists=False, file_okay=True, dir_okay=False))
@click.argument("prefix-cache", type=click.Path(exists=False, file_okay=True, dir_okay=False))
def transform_result(infile, outfile, prefix_cache):
    in_df = pd.read_csv(infile)
    
    prefix2alias = json.load(open(prefix_cache, "r"))    
    composition = json.load(open(os.path.join(Path(prefix_cache).parent, "provenance.sparql.comp"), "r"))
    inv_composition = {f"{' '.join(v)}": k for k, v in composition.items()}
    
    def extract_triple(x):
        fedx_pattern = r"StatementPattern\s+(\(new scope\)\s+)?Var\s+\((name=\w+,\s+value=(.*),\s+anonymous|name=(\w+))\)\s+Var\s+\((name=\w+,\s+value=(.*),\s+anonymous|name=(\w+))\)\s+Var\s+\((name=\w+,\s+value=(.*),\s+anonymous|name=(\w+))\)"
        match = re.match(fedx_pattern, x)
        
        #print(x)

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
        
    
    in_df["triple"] = in_df["triple"].apply(extract_triple)
    in_df["tp_name"] = in_df["triple"].apply(lookup_composition)
    in_df["tp_number"] = in_df["tp_name"].str.replace("tp", "", regex=False).astype(int)
    in_df.sort_values("tp_number", inplace=True)
    in_df["source_selection"] = in_df["source_selection"].apply(extract_source_selection)
    
    # If unequal length (as in union, optional), fill with nan
    max_length = in_df["source_selection"].apply(len).max()
    in_df["source_selection"] = in_df["source_selection"].apply(lambda x: np.pad(x, (0, max_length-len(x)), mode="empty"))
        
    out_df = in_df.set_index("tp_name")["source_selection"] \
        .to_frame().T \
        .apply(pd.Series.explode) \
        .reset_index(drop=True) 
    out_df.to_csv(outfile, index=False)

@cli.command()
@click.argument("datafiles", type=click.Path(exists=True, dir_okay=False, file_okay=True), nargs=-1)
@click.argument("outfile", type=click.Path(exists=False, file_okay=True, dir_okay=False))
@click.option("--endpoint", type=str, default="http://localhost:8890/sparql", help="URL to a SPARQL endpoint")
def generate_config_file(datafiles, outfile, endpoint):
    ssite = set()
    #for data_file in glob.glob(f'{dir_data_file}/*.nq'):
    for data_file in datafiles:
        with open(data_file) as file:
            t_file = file.readlines()
            for line in t_file:
                site = line.rsplit()[-2]
                site = re.search(r"<(.*)>", site).group(1)
                ssite.add(site)
    
    with open(f'{outfile}', 'a') as ffile:
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
    sd:endpoint "{endpoint}?default-graph-uri={s}";
    fedx:supportsASKQueries false .   

"""
            )

if __name__ == "__main__":
    cli()