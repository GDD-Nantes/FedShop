import os
from pathlib import Path
import re
from typing import Dict, Tuple
import click
import pandas as pd
import numpy as np
from utils import load_config, rsfb_logger
from tqdm import tqdm

logger = rsfb_logger(Path(__file__).name)

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
    
    def get_rwss(df: pd.DataFrame, agg, is_evaluation_mode):
        """Result-wise source-selection. Only has meaning on ideal source-selection, i.e, the source-selection calculated in generation

        Args:
            df (pd.DataFrame): _description_
        """
        
        if is_evaluation_mode:
            return None
        
        result = df.apply(pd.Series.nunique, axis=1).describe()
        return result[agg]
    
    def get_tpwss(df: pd.DataFrame):
        """The quantity relevant sources found for each triple pattern

        Args:
            df (_type_): _description_
        """
        
        return df.apply(pd.Series.nunique).sum()
    
    def get_distinct_sources(df: pd.DataFrame):
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
    for provenance_file in tqdm(workload):
        with open(provenance_file, "r") as ss_fs:
            name_search = re.search(r".*/(\w+)/(q\w+)/instance_(\d+)/batch_(\d+)/(attempt_(\d+)/)?provenance.csv", provenance_file)
            engine = name_search.group(1)
            query = name_search.group(2)
            instance = int(name_search.group(3))
            batch = int(name_search.group(4))
            attempt = name_search.group(6)
            total_nb_sources = vendor_edges[batch] + ratingsite_edges[batch]
            results_file = f"{Path(provenance_file).parent}/results.csv"
            
            is_evaluation_mode = ( (engine in CONFIG["evaluation"]["engines"]) and (attempt is not None) )       
            
            record = dict()
            
            if is_evaluation_mode:
                record.update({
                    "attempt": int(attempt),
                    "engine": engine
                })
            
            if len(ss_fs.read().strip()) == 0:
                logger.debug(f"{provenance_file} is empty!")
                record.update({
                    "query": query,
                    "instance": instance,
                    "batch": batch,
                    "nb_results": "error_runtime",
                    "nb_distinct_sources": "error_runtime",
                    "relevant_sources_selectivity": "error_runtime",
                    "tpwss": "error_runtime",
                    "avg_rwss": "error_runtime",
                    "min_rwss": "error_runtime",
                    "max_rwss": "error_runtime"
                    #"tp_specific_relevant_sources_selectivity": get_tp_specific_relevant_sources(source_selection_result),
                    #"bgp_restricted_source_level_tp_selectivity": get_bgp_restricted_source_level_tp_selectivity(source_selection_result),
                    #"xfed_join_restricted_source_level_tp_selectivity": get_xfed_join_restricted_source_level_tp_selectivity(source_selection_result)
                })
            else:
                source_selection_result = pd.read_csv(provenance_file)
                nb_results = np.nan if os.stat(results_file).st_size == 0 else len(pd.read_csv(results_file))
                record.update({
                    "query": query,
                    "instance": instance,
                    "batch": batch,
                    "nb_results": nb_results,
                    "nb_distinct_sources": get_distinct_sources(source_selection_result),
                    "relevant_sources_selectivity": get_relevant_sources_selectivity(source_selection_result, total_nb_sources),
                    "tpwss": get_tpwss(source_selection_result),
                    "avg_rwss": get_rwss(source_selection_result, "mean", is_evaluation_mode),
                    "min_rwss": get_rwss(source_selection_result, "min", is_evaluation_mode),
                    "max_rwss": get_rwss(source_selection_result, "max", is_evaluation_mode)
                    #"tp_specific_relevant_sources_selectivity": get_tp_specific_relevant_sources(source_selection_result),
                    #"bgp_restricted_source_level_tp_selectivity": get_bgp_restricted_source_level_tp_selectivity(source_selection_result),
                    #"xfed_join_restricted_source_level_tp_selectivity": get_xfed_join_restricted_source_level_tp_selectivity(source_selection_result)
                })
        
            records.append(record)
    
    metrics_df = pd.DataFrame.from_records(records)
    metrics_df.to_csv(outfile, index=False)

if __name__ == "__main__":
    cli()