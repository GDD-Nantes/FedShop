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

class PlotFitter(Fitter):
    def __init__(self, data, xmin=None, xmax=None, bins=100, distributions=None, timeout=30, density=True):
        super().__init__(data, xmin, xmax, bins, distributions, timeout, density)
        self._density = density
        
    def summary(self, Nbest=5, lw=2, plot=True, method="sumsquare_error", clf=True, figout=None):
        """Plots the distribution of the data and Nbest distribution"""
        if plot:
            if clf: pylab.clf()
            self.hist()
            self.plot_pdf(Nbest=Nbest, lw=lw, method=method)
            pylab.grid(True)
            pylab.xlabel("count_value")
            pylab.ylabel("frequency")
            if figout is not None:
                pylab.savefig(figout)

        Nbest = min(Nbest, len(self.distributions))
        try:
            names = self.df_errors.sort_values(by=method).index[0:Nbest]
        except:  # pragma: no cover
            names = self.df_errors.sort(method).index[0:Nbest]
        return self.df_errors.loc[names]

@cli.command()
@click.argument("queryfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.option("--csvout", type=click.Path(file_okay=True, dir_okay=False))
@click.option("--figout", type=click.Path(file_okay=True, dir_okay=False))
@click.option("--endpoint", type=str, default="http://localhost:8890/sparql/", help="SPARQL endpoint")
def entitytype_distribution(queryfile, csvout, figout, endpoint):
    """Plot distribution for given feature pairs. This function takes a plot query, execute it and export results.

    Args:
        queryfile (_type_): Path to the plot query
        csvout (_type_): Path to the output csv file. If None, print the result to stdout
        figout (_type_): Path to the output figure. Optional.
        endpoint (_type_): The SPARQL endpoint, default is http://localhost:8890/sparql/

    Raises:
        RuntimeError: Either the query has error or returns no result
    """
    result, _ = execute_query(queryfile, endpoint)
    if result.empty:
        raise RuntimeError(f"{queryfile} returns no result...")

    # if fitout is not None:
    #     data = result[result.columns[1]].values
    #     # label_to_number = defaultdict(partial(next, count(1)))
    #     # data = [label_to_number[label] for label in data]

    #     fitter = PlotFitter(data, distributions=get_common_distributions())
    #     fitter.fit()
    #     fit_result = fitter.summary(Nbest=5, plot=True, method="sumsquare_error", figout=fitfig)
    #     fit_result.to_csv(fitout)
    #     print(fitter.get_best(method="sumsquare_error"))
    
    if csvout is not None:
        result.to_csv(csvout, index=False)
    else:
        print(result)  

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

@cli.command()
@click.argument("benchdir", type=click.Path(exists=True, dir_okay=True, file_okay=False))
def fedx_summary(benchdir):
    """Summarize fedx execution results. This function merge all stats file of fedx results into one CSV with everything.

    Args:
        benchdir (_type_): The location of results
    """

    # virtuoso_records = glob.glob(os.path.join(benchdir, "*_noss.rec.csv"))
    # virtuoso_dumps = glob.glob(os.path.join(benchdir, "*_noss.dump.csv"))
    
    fed_query_list = pd.DataFrame(glob.glob(os.path.join(benchdir, "*_noss.sparql")), columns=["query"])
    fedx_dft_rec = glob.glob(os.path.join(benchdir, "*_dft.stat"))
    fedx_fss_rec = glob.glob(os.path.join(benchdir, "*_fss.stat"))
    fedx_logs = glob.glob(os.path.join(benchdir, "*.log"))

    fedx_dft_stats = pd.concat((pd.read_csv(f) for f in fedx_dft_rec if os.stat(f).st_size > 0))
    fedx_dft_stats["platform"] = "fedx_dft"
    fedx_fss_stats = pd.concat((pd.read_csv(f) for f in fedx_fss_rec if os.stat(f).st_size > 0))
    fedx_fss_stats["platform"] = "fedx_fss"
    
    status = pd.DataFrame.from_dict(
        { f: [open(f, "r").read()] for f in fedx_logs if os.stat(f).st_size > 0 }, 
        orient="index", columns=["status"]).reset_index().rename(columns={"index": "query"}
    )
    status["platform"] = status["query"].str.replace(r".*/q\d+_v\d_(\w+).log", r"fedx_\1")
    status["query"] = status["query"].str.replace(r"(q\d+_v\d)_\w+.log", r"\1_noss.sparql")
    fedx_stats = pd.concat([fedx_dft_stats, fedx_fss_stats]) \
        .merge(fed_query_list, on="query", how="outer") \
        .merge(status, on=["query", "platform"], how="outer")
    fedx_stats["group"] = fedx_stats["query"].str.replace(r".*q\d+_v(\d+).*", r"\1",regex=True)
    fedx_stats["query"] = fedx_stats["query"].str.replace(r".*(q\d+)_v\d+.*", r"\1",regex=True)
    fedx_stats.replace("failed", np.nan, inplace=True)
    fedx_stats.sort_values(by=["query", "group"], inplace=True)
    #fedx_stats.drop_duplicates(s", inplace=True)
    
    #print(fedx_stats[fedx_stats["status"] != "OK"])

    # virtuoso_stats = pd.concat((pd.read_csv(f) for f in virtuoso_records))
    # virtuoso_stats["query"] = virtuoso_stats["query"].apply(lambda x: os.path.join(benchdir, x))
    # virtuoso_stats["platform"] = "virtuoso"
    # overall_stats = pd.concat([fedx_stats, virtuoso_stats])
    # virtuoso_stats.set_index("query", inplace=True)

    for metric in fedx_stats.select_dtypes(exclude=["object"]).columns:
        for group in fedx_stats["group"].unique():
            print(f"Plotting {metric} for group {group}...")

            plt.figure(figsize=(10,6))
            plot = sns.barplot(data=fedx_stats.query("group == @group"), x="query", y=metric, hue="platform")
            plt.legend(loc='upper left', bbox_to_anchor=(1.02, 0.5), borderaxespad=0)
            
            for g in plot.patches:
                h, w, x = g.get_height(), g.get_width(), g.get_x()
                label=h
                if h==0: h=1
                scale = np.log10(h)
                if np.isnan(label):
                    h = 0
                    label = "Timeout/Error"
                    scale = 1
                
                plot.annotate(
                    text=label,
                    xy=(x+0.5*w, h+2*scale*w),
                    ha = 'center', va = 'center',
                    xytext = (0, 9),
                    textcoords = 'offset points',
                    rotation = 90
                )
            
            #plot.bar_label(plot.containers[0], rotation=90)
            #plot.set_title(f"{metric} across queries")
            #plot.set_yscale("log" if metric in ["exec_time", "nb_http_request"] else "linear")
            plot.set_yscale("log")

            plt.tight_layout()
            plt.savefig(os.path.join(benchdir, f"summary_{metric}_group{group}.png"))
            plt.clf()

if __name__ == "__main__":
    cli()