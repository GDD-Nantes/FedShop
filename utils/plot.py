import json
import os
from pathlib import Path
import click
import subprocess
from io import BytesIO, StringIO
import seaborn as sns
import pandas as pd
import ast

@click.group
def cli():
    pass

def execute_query(queryfile, endpoint):
    endpoint_proc = subprocess.run( 
        f"python utils/query.py execute-query {queryfile} --endpoint {endpoint}", 
        capture_output=True, shell=True
    )
    
    if endpoint_proc.returncode != 0:
        raise RuntimeError(endpoint_proc.stderr.decode())
    
    data = endpoint_proc.stdout.decode().splitlines()

    result = pd.read_csv(StringIO("\n".join(data[:-1])))
    records = ast.literal_eval(data[-1])

    return result, records

    

@cli.command()
@click.argument("queryfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.option("--csvout", type=click.Path(file_okay=True, dir_okay=False))
@click.option("--figout", type=click.Path(file_okay=True, dir_okay=False))
@click.option("--endpoint", type=str, default="http://localhost:8890/sparql/", help="SPARQL endpoint")
@click.option("--x", type=click.STRING)
@click.option("--y", type=click.STRING)
@click.option("--title", type=click.STRING)
def plot_entitytype_distribution(queryfile, csvout, figout, endpoint, x, y, title):
    result, _ = execute_query(queryfile, endpoint)
    
    if csvout is not None:
        result.to_csv(csvout, index=False)
    else:
        print(result)

    if figout is not None:
        Path(figout).parent.mkdir(parents=True, exist_ok=True)
        plot = sns.lineplot(result, x=result.columns[0], y=result.columns[1])
        plot.set(
            title=title,
            xticklabels=[]
        )
        plot.figure.savefig(figout)            


  
if __name__ == "__main__":
    cli()