import glob
from io import BytesIO
import json
import math
import re
import subprocess
import click
import numpy as np
import pandas as pd
import os
from pathlib import Path

import sys

directory = os.path.abspath(__file__)
print(Path(directory).parent.parent.parent.parent)
sys.path.append(os.path.join(Path(directory).parent.parent.parent.parent, "fedshop")) 

from query import exec_query
from utils import load_config, activate_one_container, fedshop_logger

@click.group
def cli():
    pass

CONFIGFILE = os.environ["RSFB__CONFIGFILE"]

WORKDIR = Path(__file__).parent
CONFIG = load_config(CONFIGFILE)["generation"]
SPARQL_COMPOSE_FILE = CONFIG["virtuoso"]["compose_file"]
SPARQL_SERVICE_NAME = CONFIG["virtuoso"]["service_name"]
N_BATCH = CONFIG["n_batch"]
LAST_BATCH = N_BATCH - 1
LOGGER = fedshop_logger(Path(__file__).name)

def query(queryfile, batch_id):
    result = None    
    with open(queryfile, "r") as fp:
        query_text = fp.read()
        print(query_text)
        activate_one_container(batch_id, SPARQL_COMPOSE_FILE, SPARQL_SERVICE_NAME, LOGGER, "/dev/null")
        _, result = exec_query(configfile=CONFIGFILE, query=query_text, error_when_timeout=True, batch_id=batch_id)
        with BytesIO(result) as header_stream, BytesIO(result) as data_stream: 
            header = header_stream.readline().decode().strip().replace('"', '').split(",")
            result = pd.read_csv(data_stream, parse_dates=[h for h in header if "date" in h])
    return result

@cli.command()
def get_endpoint_distribution():
    
    vendor_data = np.arange(CONFIG["schema"]["vendor"]["params"]["vendor_n"])
    _, vendor_edges = np.histogram(vendor_data, N_BATCH)
    vendor_edges = vendor_edges[1:].astype(int)
    
    ratingsite_data = np.arange(CONFIG["schema"]["ratingsite"]["params"]["ratingsite_n"])
    _, ratingsite_edges = np.histogram(ratingsite_data, N_BATCH)
    ratingsite_edges = ratingsite_edges[1:].astype(int)
    
    queryfile = f"{WORKDIR}/endpoint_distribution.sparql"
    endpoints = query(queryfile, LAST_BATCH)
    
    endpoints = endpoints[endpoints['g'].str.contains(r"(ratingsite|vendor)", regex=True)]
    
    
    def get_batch_id(x):
        str_search = re.search(r"(ratingsite|vendor)(\d+)", x)
        dataset = str_search.group(1)
        memberId = int(str_search.group(2))
        return np.argwhere((memberId <= (vendor_edges if dataset == "vendor" else ratingsite_edges))).min().item()
        
    endpoints["batch_id"] = endpoints["g"].apply(get_batch_id)
    result = endpoints.groupby("batch_id").aggregate(list).cumsum().to_dict()

    with open(f"{WORKDIR}/endpoint_distribution.stat", "w") as json_fs:
        json.dump(result, json_fs)
    
@cli.command()
def get_class_distribution():
    queryfile = f"{WORKDIR}/class_distribution.sparql"
    
    results = []
    for batch_id in range(N_BATCH):
        tmp = query(queryfile, batch_id).set_index("class").rename({"nb_entities": f"nb_entities_batch{batch_id}"}, axis=1)
        results.append(tmp)
    
    result = pd.concat(results, axis=1)
    result.to_csv(f"{WORKDIR}/class_distribution.stat", index=True)

def human_readable(bytesize):
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.log(bytesize, 1024))
    p = math.pow(1024, i)
    s = round(bytesize / p, 2)
    return f"{s} {size_name[i]}"

@cli.command()  
def get_vendor_stats():
    
    results = []
    for batch_id in range(N_BATCH):
        tmp = query(f"{WORKDIR}/vendor_stats.sparql", batch_id).rename({"nbQuads": f"batch{batch_id}"}, axis=1)
        results.append(tmp)
    
    result = pd.concat(results, axis=1)
    result.to_csv(f"{WORKDIR}/vendor_stats.stat", index=False)
    

@cli.command()  
def get_ratingsite_stats():
    
    results = []
    for batch_id in range(N_BATCH):
        tmp = query(f"{WORKDIR}/ratingsite_stats.sparql", batch_id).rename({"nbQuads": f"batch{batch_id}"}, axis=1)
        results.append(tmp)
    
    result = pd.concat(results, axis=1)
    result.to_csv(f"{WORKDIR}/ratingsite_stats.stat", index=False)

@cli.command()
def get_overall_stats():
    vendor_stats = pd.read_csv(f"{Path(__file__).parent}/vendor_stats.stat")
    ratingsite_stats = pd.read_csv(f"{Path(__file__).parent}/ratingsite_stats.stat")
    
    result_df = vendor_stats + ratingsite_stats
    
    print(result_df)
    
    result_df.to_csv(f"{Path(__file__).parent}/overall_stats.stat", index=False)
        
if __name__ == "__main__":
    cli()