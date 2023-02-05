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

from utils import kill_process, load_config
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
    
    if not os.path.exists(app) or not os.path.exists(jar) or os.path.exists(lib):
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
def run_benchmark(eval_config, fedx_config, query, result, stats, ideal_ss):

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