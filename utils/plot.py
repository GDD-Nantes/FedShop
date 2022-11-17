import os
import click
import subprocess
from io import BytesIO, StringIO

import pandas as pd

@click.group
def cli():
    pass

def execute_query(queryfile, endpoint):
    data = subprocess.run( 
        f"python utils/query.py execute-query {queryfile} --endpoint {endpoint}", 
        capture_output=True, shell=True
    ).stdout.decode().splitlines()

    result = "\n".join(data[:-1])
    records = data[-1]

    return result, records

    

@cli.command()
@click.argument("queryfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("outfile", type=click.Path(file_okay=True, dir_okay=False))
@click.option("--endpoint", type=str, default="http://localhost:8890/sparql/", help="SPARQL endpoint")
def plot_entitytype_distribution(queryfile, outfile, endpoint):
    
    result, _ = execute_query(queryfile, endpoint)

    result = pd.read_csv(StringIO(result))
    result.to_csv(outfile, index=False)

    print(result)


if __name__ == "__main__":
    cli()