from io import BytesIO
from pathlib import Path
from time import time
import click
import pandas as pd

from SPARQLWrapper import SPARQLWrapper, CSV
from rdflib import ConjunctiveGraph

@click.command()
@click.argument("query", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.option("--output", type=click.Path(exists=False, file_okay=True, dir_okay=False), default=None,
    help="The file in which the query result will be stored.")
@click.option("--records", type=click.Path(exists=False, file_okay=True, dir_okay=False), default=None,
    help="The file in which the stats will be stored.")
@click.option("--entrypoint", type=str, default="http://localhost:8890/sparql/",
    help="URL of the Virtuoso SPARQL endpoint")

def execute_query(query, output, records, entrypoint):

    query_name = Path(query).resolve().stem
    query_text = open(query, mode="r").read()

    startTime = None
    endTime = None
    
    if entrypoint.startswith("http"):
        sparql_endpoint = SPARQLWrapper(entrypoint)
        sparql_endpoint.setReturnFormat(CSV)   
        startTime = time()     
        sparql_endpoint.setQuery(query_text)
        result = sparql_endpoint.queryAndConvert()
        endTime = time()
    else:
        g = ConjunctiveGraph()
        g.parse(entrypoint, format="nquads")
        startTime = time()
        result = g.query(query_text).serialize(format="csv")
        endTime = time()  
    
    csvOut = pd.read_csv(BytesIO(result))
    if output is None: print(csvOut)
    else: csvOut.to_csv(output, index=False, quotechar='"')

    recordsOut = pd.DataFrame({
        "query": query_name,
        "exec_time": (endTime-startTime)*1e-3
    }, index=[0])

    if records is None: print(records)
    else: recordsOut.to_csv(records, index=False)

if __name__ == "__main__":
    execute_query()