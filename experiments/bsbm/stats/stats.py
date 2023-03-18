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
sys.path.append(os.path.join(Path(directory).parent.parent.parent.parent, "rsfb")) 

from query import exec_query
from utils import load_config, activate_one_container, rsfb_logger

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
LOGGER = rsfb_logger(Path(__file__).name)

def query(queryfile, batch_id):
    result = None    
    with open(queryfile, "r") as fp:
        query_text = fp.read()
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

    with open(f"{WORKDIR}/endpoint_distribution.json", "w") as json_fs:
        json.dump(result, json_fs)
    
@cli.command()
def get_class_distribution():
    queryfile = f"{WORKDIR}/class_distribution.sparql"
    
    results = []
    for batch_id in range(N_BATCH):
        tmp = query(queryfile, batch_id).set_index("class").rename({"nb_entities": f"nb_entities_batch{batch_id}"}, axis=1)
        results.append(tmp)
    
    result = pd.concat(results, axis=1)
    result.to_csv(f"{WORKDIR}/class_distribution.csv", index=True)

@cli.command()  
def get_vendor_stats():
    
    data = np.arange(CONFIG["schema"]["vendor"]["params"]["vendor_n"])
    _, edges = np.histogram(data, N_BATCH)
    edges = edges[1:].astype(int)
    
    result = {
        "nquads": {},
        "size": {}
    }
    
    for nq_file in glob.glob(f"{Path(__file__).parent.parent}/model/dataset/vendor*.nq"):
        member_id = int(re.findall(r"(\d+)\.nq", nq_file)[0])
        batch_id = np.argwhere((member_id <= edges)).min().item()
        nquads = int(subprocess.check_output(['wc', '-l', nq_file]).split()[0])
        
        if result["nquads"].get(f"batch{batch_id}") is None:    
            result["nquads"][f"batch{batch_id}"] = 0

        result["nquads"][f"batch{batch_id}"] += nquads
        
        fsize = os.stat(nq_file).st_size
        
        if result["size"].get(f"batch{batch_id}") is None:    
            result["size"][f"batch{batch_id}"] = 0

        result["size"][f"batch{batch_id}"] += fsize
        
    result_df = pd.DataFrame.from_dict(result, orient="index").sort_index(axis=1)
    
    def human_readable(bytesize):
        size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
        i = int(math.log(bytesize, 1024))
        p = math.pow(1024, i)
        s = round(bytesize / p, 2)
        return f"{s} {size_name[i]}"
    
    result_df.loc["size_simplified", :] = result_df.loc["size", :].apply(human_readable)
    result_df.to_csv(f"{Path(__file__).parent}/vendor_stats.csv", index=True)
    

@cli.command()  
def get_ratingsite_stats():
    
    data = np.arange(CONFIG["schema"]["ratingsite"]["params"]["ratingsite_n"])
    _, edges = np.histogram(data, N_BATCH)
    edges = edges[1:].astype(int)
    
    result = {
        "nquads": {},
        "size": {}
    }
    
    for nq_file in glob.glob(f"{Path(__file__).parent.parent}/model/dataset/ratingsite*.nq"):
        member_id = int(re.findall(r"(\d+)\.nq", nq_file)[0])
        batch_id = np.argwhere((member_id <= edges)).min().item()
        nquads = int(subprocess.check_output(['wc', '-l', nq_file]).split()[0])
        
        if result["nquads"].get(f"batch{batch_id}") is None:    
            result["nquads"][f"batch{batch_id}"] = 0

        result["nquads"][f"batch{batch_id}"] += nquads
        
        fsize = os.stat(nq_file).st_size
        
        if result["size"].get(f"batch{batch_id}") is None:    
            result["size"][f"batch{batch_id}"] = 0

        result["size"][f"batch{batch_id}"] += fsize
        
    result_df = pd.DataFrame.from_dict(result, orient="index").sort_index(axis=1)
    
    def human_readable(bytesize):
        size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
        i = int(math.log(bytesize, 1024))
        p = math.pow(1024, i)
        s = round(bytesize / p, 2)
        return f"{s} {size_name[i]}"
    
    result_df.loc["size_simplified", :] = result_df.loc["size", :].apply(human_readable)
    result_df.to_csv(f"{Path(__file__).parent}/ratingsite_stats.csv", index=True)
        
        
if __name__ == "__main__":
    cli()