import re
from typing import Dict, Tuple
import click
import pandas as pd
import numpy as np
from utils import load_config

@click.group
def cli():
    pass

# def __traverse_graph(max_size: int, const_count: int, constJoinVertexAllowed: bool, dupEdgesAllowed: bool, qmap: Dict[Tuple[str, str], Dict[str, str]]):
#     v_array = []
    

# def __get_query_structures(configfile, queryfile):
#     pass

@cli.command()
@click.argument("configfile", type=click.Path(exists=True, dir_okay=False, file_okay=True))
@click.argument("outfile", type=click.Path(exists=False, dir_okay=False, file_okay=True))
@click.argument("workload", type=click.Path(exists=True, dir_okay=False, file_okay=True), nargs=-1)
def compute_metrics(configfile, outfile, workload):
    """Compute the metrics to evaluate source selection engines.

    TODO:
        [] tp_specific_relevant_sources
        [] bgp_restricted_source_level_tp_selectivity
        [] xfed_join_restricted_source_level_tp_selectivity

    Args:
        workload (_type_): List of all results obtained by executing provenance queries.
    """
    
    def get_distinct_sources(df):
        return pd.Series(df.values.flatten()).nunique()

    def get_relevant_sources_selectivity(df: pd.DataFrame, total_number_sources):
        return get_distinct_sources(df) / total_number_sources

    def get_tp_specific_relevant_sources(df: pd.DataFrame) -> float:
        """Union set of all contacted federation member over total number of federation members there is.

        TODO:
            [] implement it
        Args:
            df (_type_): the source selection result
        """
        pass

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
    
    CONFIG = load_config(configfile)
    CONFIG_GEN = CONFIG["generation"]
    vendor_data = np.arange(CONFIG_GEN["schema"]["vendor"]["params"]["vendor_n"])
    ratingsite_data = np.arange(CONFIG_GEN["schema"]["ratingsite"]["params"]["ratingsite_n"])
    _, vendor_edges = np.histogram(vendor_data, CONFIG_GEN["n_batch"])
    _, ratingsite_edges = np.histogram(ratingsite_data, CONFIG_GEN["n_batch"])
    vendor_edges = vendor_edges[1:].astype(int) + 1
    ratingsite_edges = ratingsite_edges[1:].astype(int) + 1

    records = []        
    for provenance_file in workload:
        source_selection_result = pd.read_csv(provenance_file)
        name_search = re.search(r".*/(\w+)/(q\w+)/instance_(\d+)/batch_(\d+)/provenance.csv", provenance_file)
        print(provenance_file)
        engine = name_search.group(1)
        query = name_search.group(2)
        instance = int(name_search.group(3))
        batch = int(name_search.group(4))
        total_nb_sources = vendor_edges[batch] + ratingsite_edges[batch]
        
        record = {
            "query": query,
            "instance": instance,
            "batch": batch,
            "distinct_sources": get_distinct_sources(source_selection_result),
            "relevant_sources_selectivity": get_relevant_sources_selectivity(source_selection_result, total_nb_sources)
            #"tp_specific_relevant_sources_selectivity": get_tp_specific_relevant_sources(source_selection_result),
            #"bgp_restricted_source_level_tp_selectivity": get_bgp_restricted_source_level_tp_selectivity(source_selection_result),
            #"xfed_join_restricted_source_level_tp_selectivity": get_xfed_join_restricted_source_level_tp_selectivity(source_selection_result)
        }
        
        if engine in CONFIG["evaluation"]["engines"]:
            record.update({"engine": engine})
        
        records.append(record)
    
    metrics_df = pd.DataFrame.from_records(records)
    metrics_df.to_csv(outfile, index=False)

if __name__ == "__main__":
    cli()