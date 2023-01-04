from io import BytesIO
import os
from pathlib import Path
import click
import pandas as pd
import numpy as np

import sys
directory = os.path.abspath(__file__)
sys.path.append(os.path.join(Path(directory).parent.parent.parent.parent, "rsfb"))

@click.group
def cli():
    pass

from utils import load_config
from query import exec_query

WORKDIR = Path(__file__).parent
CONFIG = load_config("experiments/bsbm/config.yaml")["generation"]

def __query(queryfile, endpoint):
    query_text = open(queryfile, "r").read()
    _, result = exec_query(query_text, endpoint, error_when_timeout=True)
    header = BytesIO(result).readline().decode().strip().replace('"', '').split(",")
    return pd.read_csv(BytesIO(result), parse_dates=[h for h in header if "date" in h])

@cli.command()
@click.option("--endpoint", type=str, default="http://localhost:8890/sparql", help="URL to a SPARQL endpoint")
def test_product_nb_producer_per_product(endpoint):
    """Test whether the average number of producers per product is 1 .

    Args:
        endpoint ([type]): [description]
    """
    result = __query(f"{WORKDIR}/product/test_product_nb_producer_per_product.sparql", endpoint)
    assert (result["nbProducer"] == 1).all()

@cli.command()
@click.option("--endpoint", type=str, default="http://localhost:8890/sparql", help="URL to a SPARQL endpoint")
def test_product_nb_producer(endpoint):
    """Test whether the number of shares the current product .

    Args:
        endpoint ([type]): [description]
    """

    data = np.arange(CONFIG["schema"]["vendor"]["params"]["vendor_n"])
    _, edges = np.histogram(data, CONFIG["n_batch"])
    edges = edges[1:].astype(int)

    result = __query(f"{WORKDIR}/product/test_product_nb_producer.sparql", endpoint)
    result["batchId"] = result["batchId"].apply(lambda x: np.argwhere((x <= edges)).min().item())
    result["groupProducer"] = result["groupProducer"].apply(lambda x: np.unique(x.split("|")))

    groupProducer = result.groupby("batchId")["groupProducer"] \
        .aggregate(np.concatenate) \
        .apply(np.unique) \
        .apply(len)

    std = groupProducer.std()
    print(std)

    # print(average, std)
    # assert (average == 50).all()
    assert (std == 1).all()

if __name__ == "__main__":
    cli()