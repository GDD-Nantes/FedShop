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

# Example of use : 
# python3 utils/generate-fedx-config-file.py bsbm/model/vendor test/out.ttl

# Goal : Generate a configuration file for RDF4J to set the use of named graph as endpoint thanks to data file

@click.group
def cli():
    pass

@cli.command()
@click.argument("config", type=click.Path(exists=True, file_okay=True, dir_okay=True))
@click.argument("query", type=click.Path(exists=True, file_okay=True, dir_okay=True))
@click.argument("result", type=click.Path(exists=False, file_okay=True, dir_okay=True))
@click.argument("stats", type=click.Path(exists=False, file_okay=True, dir_okay=True))
@click.option("--ideal-ss", type=click.Path(exists=False, file_okay=True, dir_okay=True), default="")
@click.option("--timeout", type=click.INT, default=300)
def run_benchmark(config, query, result, stats, ideal_ss, timeout):
    app = "Federapp/target"
    jar = os.path.join(app, "Federapp-1.0-SNAPSHOT.jar")
    lib = os.path.join(app, "lib/*")

    r_root = Path(result).parent

    #stat = f"{r_root}/stats.csv"
    sourceselection = f"{r_root}/provenance"
    httpreq = f"{r_root}/httpreq"

    args = [config, query, result, stats, sourceselection, httpreq, ideal_ss]
    args = " ".join(args)
    timeoutArgs = f'timeout --signal=SIGKILL "{timeout}"' if timeout != 0 else ""
    cmd = f'{timeoutArgs} java -classpath "{jar}:{lib}" org.example.Federapp {args}'.strip() 
    
    def write_empty_stats():
        with open(stats, "w+") as fout:
            fout.write("query,engine,instance,batch,mode,exec_time,distinct_ss,nb_http_request,total_ss\n")
            basicInfos = re.match(r".*/(\\w+)/(q\\d+)/(\\d+)/batch_(\\d+)/(\\w+)/results", query)
            engine = basicInfos.group(1)
            queryName = basicInfos.group(2)
            instance = basicInfos.group(3)
            batch = basicInfos.group(4)
            mode = basicInfos.group(5)
            fout.write(",".join([queryName, engine, instance, batch, mode, "nan", "nan", "nan", "nan"])+"\n")
            fout.close()
    
    def write_empty_result(msg):
        with open(result, "w+") as fout:
            fout.write(msg)
            fout.close()
    
    try: 
        fedx_proc = subprocess.run(cmd, capture_output=True, shell=True, timeout=timeout)
        if fedx_proc.returncode == 0:
            print(f"{query} benchmarked sucessfully")  
        else:
            print(f"{query} reported error")
            write_empty_stats()
            write_empty_result(fedx_proc.stdout.decode())
           
    except subprocess.TimeoutExpired: 
        print(f"{query} timed out!")
        write_empty_stats()
        write_empty_result("timeout")

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
                site = line.rsplit()[-1]
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