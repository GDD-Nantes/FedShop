import re
import click
import pandas as pd
import numpy as np
from utils import load_config

@click.group
def cli():
    pass

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

    def get_relevant_sources_selectivity(df: pd.DataFrame, total_number_sources):
        return pd.Series(df.values.flatten()).nunique() / total_number_sources

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
    
    CONFIG = load_config(configfile)["generation"]
    vendor_data = np.arange(CONFIG["schema"]["vendor"]["params"]["vendor_n"])
    ratingsite_data = np.arange(CONFIG["schema"]["ratingsite"]["params"]["ratingsite_n"])
    _, vendor_edges = np.histogram(vendor_data, CONFIG["n_batch"])
    _, ratingsite_edges = np.histogram(ratingsite_data, CONFIG["n_batch"])
    vendor_edges = vendor_edges[1:].astype(int) + 1
    ratingsite_edges = ratingsite_edges[1:].astype(int) + 1
        
    metrics_df = pd.DataFrame(columns=["query", "instance", "batch", "relevant_sources_selectivity"])
    for provenance_file in workload:
        source_selection_result = pd.read_csv(provenance_file)
        name_search = re.search(r".*/(q\d+)/instance_(\d+)/batch_(\d+)/provenance.csv", provenance_file)
        query = name_search.group(1)
        instance = int(name_search.group(2))
        batch = int(name_search.group(3))

        total_nb_sources = vendor_edges[batch] + ratingsite_edges[batch]

        new_row = {
            "query": query,
            "instance": instance,
            "batch": batch,
            "relevant_sources_selectivity": get_relevant_sources_selectivity(source_selection_result, total_nb_sources)
            #"tp_specific_relevant_sources_selectivity": get_tp_specific_relevant_sources(source_selection_result),
            #"bgp_restricted_source_level_tp_selectivity": get_bgp_restricted_source_level_tp_selectivity(source_selection_result),
            #"xfed_join_restricted_source_level_tp_selectivity": get_xfed_join_restricted_source_level_tp_selectivity(source_selection_result)
        }

        metrics_df.loc[len(metrics_df)] = list(new_row.values())
    
    print(metrics_df)
    metrics_df.to_csv(outfile, index=False)

if __name__ == "__main__":
    cli()