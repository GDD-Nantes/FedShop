import json
import os
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
    data = subprocess.run( 
        f"python utils/query.py execute-query {queryfile} --endpoint {endpoint}", 
        capture_output=True, shell=True
    ).stdout.decode().splitlines()

    result = pd.read_csv(StringIO("\n".join(data[:-1])))
    records = ast.literal_eval(data[-1])

    return result, records

    

@cli.command()
@click.argument("queryfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.option("--output", type=click.Path(file_okay=True, dir_okay=False))
@click.option("--endpoint", type=str, default="http://localhost:8890/sparql/", help="SPARQL endpoint")
@click.option("--x", type=click.STRING)
@click.option("--y", type=click.STRING)
@click.option("--title", type=click.STRING)
def plot_entitytype_distribution(queryfile, output, endpoint, x, y, title):
    result, _ = execute_query(queryfile, endpoint)
    print(result)

    if output is not None:
        plot = sns.lineplot(result, x=x, y=y)
        plot.set(
            title=title,
            xticklabels=[]
        )
        plot.figure.savefig(output)            


  
if __name__ == "__main__":
    cli()