# Import part
import json
import os
import re
import click
import glob
import subprocess
import pandas as pd
import numpy as np

# Example of use : 
# python3 utils/generate-fedx-config-file.py bsbm/model/vendor test/out.ttl

# Goal : Generate a configuration file for RDF4J to set the use of named graph as endpoint thanks to data file

@click.group
def cli():
    pass

@cli.command()
@click.argument("app", type=click.Path(exists=True, file_okay=True, dir_okay=True))
@click.argument("config", type=click.Path(exists=True, file_okay=True, dir_okay=True))
@click.argument("query", type=click.Path(exists=True, file_okay=True, dir_okay=True))
@click.argument("result", type=click.Path(exists=False, file_okay=True, dir_okay=True))
@click.argument("stat", type=click.Path(exists=False, file_okay=True, dir_okay=True))
@click.argument("sourceselection", type=click.Path(exists=False, file_okay=True, dir_okay=True))
@click.argument("httpreq", type=click.Path(exists=False, file_okay=True, dir_okay=True))
@click.argument("output", type=click.Path(exists=False, file_okay=True, dir_okay=True))
@click.option("--ssopt", type=click.Path(exists=False, file_okay=True, dir_okay=True), default="")
@click.option("--timeout", type=click.INT, default=300)
def run_benchmark(app, config, query, result, stat, sourceselection, httpreq, output, ssopt, timeout):
    jar = os.path.join(app, "Federapp-1.0-SNAPSHOT.jar")
    lib = os.path.join(app, "lib/*")
    args = [config, query, result, stat]
    #args = [ os.path.abspath(fn) for fn in args ]
    args = " ".join(args)
    timeoutArgs = f'timeout --signal=SIGKILL "{timeout}"' if timeout != 0 else ""
    cmd = f'{timeoutArgs} java -classpath "{jar}:{lib}" org.example.Federapp {args}'.strip() 
    print(cmd)
    
    result = None
    try: 
        fedx_proc = subprocess.run(cmd, capture_output=True, shell=True, timeout=timeout)
        result = "OK" if fedx_proc.returncode == 0 else "ERROR"
        print(f"{query} benchmarked sucessfully")
        with open(output, "w") as fout:
            fout.write(result)
            fout.close()
    except subprocess.TimeoutExpired: 
        print(f"{query} timed out!")
        with open(output, "w") as fout:
            fout.write("TIMEOUT")
            fout.close()

@cli.command()
@click.argument("datafiles", type=click.Path(exists=True, dir_okay=False, file_okay=True), nargs=-1)
@click.argument("outfile", type=click.Path(exists=False, file_okay=True, dir_okay=False))
@click.option("--endpoint", type=str, default="http://localhost:8890/sparql/", help="URL to a SPARQL endpoint")
def generate_config_file(datafiles, outfile, endpoint):
    ssite = set()
    #for data_file in glob.glob(f'{dir_data_file}/*.nq'):
    for data_file in datafiles:
        with open(data_file) as file:
            t_file = file.readlines()
            for line in t_file:
                site = line.split()[-1]
                site = site.replace("<", "")
                site = site.replace(">.", "")
                ssite.add(site)
    
    with open(f'{outfile}', 'a') as ffile:
        ffile.write(
"""
@prefix sd: <http://www.w3.org/ns/sparql-service-description#> .
@prefix fedx: <http://rdf4j.org/config/federation#> .

"""
        )
        for s in ssite:
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