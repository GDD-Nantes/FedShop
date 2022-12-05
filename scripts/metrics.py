import glob
import os
from pathlib import Path
import re
import click
import subprocess
from io import StringIO
import seaborn as sns
import pandas as pd
import ast
from fitter import Fitter, get_common_distributions, get_distributions
import pylab
import numpy as np
import matplotlib.pyplot as plt

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
@click.argument("workload", type=click.Path(exists=True, dir_okay=False, file_okay=True), nargs=-1)
@click.argument("fedcount", type=click.INT)
@click.argument("outfile", type=click.Path(exists=False, dir_okay=False, file_okay=True))
def compute_metrics(workload, fedcount, outfile):
    """Compute the metrics to evaluate source selection engines.

    TODO:
        [] tp_specific_relevant_sources
        [] bgp_restricted_source_level_tp_selectivity
        [] xfed_join_restricted_source_level_tp_selectivity

    Args:
        fedcount (_type_): Total number of federation members
        workload (_type_): List of all results obtained by executing provenance queries.
    """

    def get_relevant_sources_selectivity(df: pd.DataFrame, fedcount):
        return np.unique(df.dropna().values.ravel()).size /fedcount

    def get_tp_specific_relevant_sources(df: pd.DataFrame, fedcount) -> float:
        """Union set of all contacted federation member over total number of federation members there is.

        TODO:
            [] implement it
        Args:
            df (_type_): the source selection result
        """

        union_set = set().union(*df.apply(lambda x: x.dropna().unique()).values.tolist()) # make a set for each column, then set union all
        if np.nan in union_set:
            print(df[df.isna().any(axis=1)])
            raise RuntimeError(f"Something wrong")
        result = len(union_set)/fedcount
        return result

    def get_bgp_restricted_source_level_tp_selectivity(df):
        """Number of sources contributing to a triple pattern given other triple patterns, 
        over the number of sources contributing to that triple pattern alone.

        Args:
            df (_type_): the source selection result
        """
        pass

    def get_xfed_join_restricted_source_level_tp_selectivity(df):
        """Number of distinct join vertexes in federated context over the total number join vertexes.

        TODO:
            [] 

        Args:
            df (_type_): the source selection result
        """
        pass

    metrics_df = pd.DataFrame(columns=["query", "instance", "batch", "tp_specific_relevant_sources_selectivity", "relevant_sources_selectivity"])
    for provenance_file in workload:
        ss_df = pd.read_csv(provenance_file)
        name_search = re.search(r".*/benchmark/(q\d+)/(\d+)/batch_(\d+)/provenance.csv", provenance_file)
        query = name_search.group(1)
        instance = int(name_search.group(2))
        batch = int(name_search.group(3))

        new_row = {
            "query": query,
            "instance": instance,
            "batch": batch,
            "tp_specific_relevant_sources_selectivity": get_tp_specific_relevant_sources(ss_df, fedcount),
            "relevant_sources_selectivity": get_relevant_sources_selectivity(ss_df, fedcount)
            #"bgp_restricted_source_level_tp_selectivity": get_bgp_restricted_source_level_tp_selectivity(ss_df),
            #"xfed_join_restricted_source_level_tp_selectivity": get_xfed_join_restricted_source_level_tp_selectivity(ss_df)
        }

        metrics_df.loc[len(metrics_df)] = list(new_row.values())
    
    print(metrics_df)
    metrics_df.to_csv(outfile, index=False)

if __name__ == "__main__":
    cli()