from copy import deepcopy
import glob
from itertools import chain
import json
from pathlib import Path
from pprint import pprint
import subprocess

from collections import Counter
import os
from pathlib import Path
from tqdm import tqdm

from rdflib.term import Variable
from rdflib.plugins.sparql.parser import parseQuery
from rdflib.plugins.sparql.algebra import _traverseAgg, traverse, translateQuery, pprintAlgebra
from rdflib.plugins.sparql.parserutils import CompValue

from .algebra.rdflib_algebra import add_graph_to_triple_pattern, add_values_with_placeholders, collect_triple_variables, collect_variables, disable_offset, disable_orderby_limit, extract_where, inject_constant_into_placeholders, remove_filter_with_placeholders, replace_select_projection_with_graph, translateAlgebra
from .algebra.pandas_algebra import collect_constants, parse_expr, translate_query

import re
import numpy as np
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, CSV
from io import BytesIO, StringIO
import click

from .utils import load_config, fedshop_logger
logger = fedshop_logger(Path(__file__).name)

import nltk

nltk.download('stopwords', quiet=True)
nltk.download('punkt', quiet=True)

from nltk.corpus import stopwords as nltk_stopwords
from nltk.tokenize import word_tokenize, RegexpTokenizer

tokenizer = RegexpTokenizer(r"\w+")

from ftlangdetect import detect
from iso639 import Lang
import tempfile

PANDAS_RANDOM_STATE = 42
WDQ_BIN_PATH = "fedshop/misc/wdq"

@click.group
def cli():
    pass


stopwords = []
for lang in ["english"]:
    stopwords.extend(nltk_stopwords.words(lang))


def lang_detect(txt):
    lines = str(txt).splitlines()
    result = Counter(map(lambda x: Lang(detect(text=x, low_memory=False)["lang"]).name.lower(), lines)).most_common(1)[
        0]
    return result


def exec_query_on_endpoint(query, endpoint, error_when_timeout, timeout=None, default_graph=None):
    """Send a query to ANY endpoint

    Args:
        query (_type_): _description_
        endpoint (_type_): _description_
        error_when_timeout (_type_): _description_
        timeout (_type_, optional): _description_. Defaults to None.

    Returns:
        _type_: _description_
    """

    sparql_endpoint = SPARQLWrapper(endpoint, defaultGraph=default_graph)
    if error_when_timeout and timeout is not None:
        sparql_endpoint.setTimeout(int(timeout))

    sparql_endpoint.setMethod("GET")
    sparql_endpoint.setReturnFormat(CSV)
    sparql_endpoint.setQuery(query)

    response = sparql_endpoint.query()
    result = response.convert()
    return response, result


def exec_query(query, endpoint, error_when_timeout=False):
    """Send a query to an endpoint of certain batch and return results

    Args:
        query (_type_): _description_
        endpoint (_type_): _description_
        error_when_timeout (bool, optional): _description_. Defaults to False.

    Returns:
        _type_: _description_
    """
    return exec_query_on_endpoint(query, endpoint, error_when_timeout)

@cli.command()
@click.argument("endpoint", type=click.STRING)
@click.option("--queryfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.option("--querydata", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.option("--outfile", type=click.Path(exists=False, file_okay=True, dir_okay=False))
@click.option("--sample", type=click.INT)
@click.option("--seed", type=click.INT, default=PANDAS_RANDOM_STATE)
@click.option("--ignore-errors", is_flag=True, default=False)
@click.option("--dropna", is_flag=True, default=False)
def execute_query(endpoint, queryfile, querydata, outfile, sample, seed, ignore_errors, dropna):
    """Execute query, export to an output file and return number of rows .

    Args:
        queryfile ([type]): the query file name
        outfile ([type]): the output file name
        sample ([type]): the number of rows randomly sampled
        ignore_errors ([type]): if set, ignore when the result is empty
        endpoint ([type]): the SPARQL endpoint

    Raises:
        RuntimeError: the result is empty

    Returns:
        [type]: the number of rows
    """
    
    query_text = querydata
    if queryfile:
        with open(queryfile, mode="r") as qf:
            query_text = qf.read()
    
    if query_text is None:
        raise RuntimeError("No query to execute...")
    
    _, result = exec_query(query=query_text, endpoint=endpoint, error_when_timeout=False)

    with BytesIO(result) as header_stream, BytesIO(result) as data_stream:
        header = header_stream.readline().decode().strip().replace('"', '').split(",")
        csvOut = pd.read_csv(data_stream, parse_dates=[h for h in header if "date" in h])

        if csvOut.empty and not ignore_errors:
            logger.error(query_text)
            raise RuntimeError(f"{queryfile} returns no result...")

        if dropna:
            csvOut.dropna(inplace=True)

        if sample is not None:
            csvOut = csvOut.sample(sample, random_state=seed)

        if outfile: 
            csvOut.to_csv(outfile, index=False)

        return csvOut

def pretty_print_query(queryfile):
    cmd = f"./{WDQ_BIN_PATH} --no-execute --language en --query {queryfile}"
    logger.debug(f"wdq comamnd: {cmd}")
    proc = subprocess.run(cmd, shell=True, capture_output=True)
    return proc.stdout.decode()
    
@cli.command()
@click.argument("queryfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
def pprint_query(queryfile):
    res = pretty_print_query(queryfile)
    click.echo(res)

@cli.command()
@click.argument("queryfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("constfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("outfile", type=click.Path(exists=False, file_okay=True, dir_okay=False))
@click.pass_context
def build_value_selection_query(ctx: click.Context, queryfile, constfile, outfile):
    """
    Builds a value selection query based on the given parameters.
    
    UNION: Split UNION queries into separate queries. The value selection results should be JOINED later.
    OPTIONAL:

    Args:
        ctx (click.Context): The Click context object.
        queryfile (str): The path to the query file.
        outfile (str): The path to the output file.

    Returns:
        None
    """   
    
    def has_constant(node, children, consts):
        if isinstance(node, Variable):
            return str(node) in consts
        elif isinstance(node, CompValue):
            if node.name == "vars":
                return str(node["var"]) in consts
        return any(children)
                
    def split_union_query(node, children):
        if isinstance(node, CompValue):
            if node.name == "SelectQuery":
                where = node["where"]["part"]
                if len(where) == 1 and where[0].name == "GroupOrUnionGraphPattern":
                    graphs = where[0]["graph"]
                    # Only split when it's UNION clause, i.e., there are at least 2 graphs 
                    if len(graphs) > 1:
                        for graph in graphs:
                            children.append([graph])
        
        return list(chain(*children))
    
    def has_optional_first_level(node, children):
        if isinstance(node, CompValue):
            if node.name == "SelectQuery":
                where = node["where"]
                for subpart in where["part"]:
                    if subpart.name == "OptionalGraphPattern":
                        return True
        return any(children)
        
    def split_optional_query(node, children, consts):        
        if isinstance(node, CompValue):
            if node.name == "SelectQuery":
                where = node["where"]
                new_part = []
                for subpart in where["part"]:
                    if subpart.name == "OptionalGraphPattern":
                        if _traverseAgg(subpart, lambda x, c: has_constant(x, c, consts)):
                            new_part.extend(subpart["graph"]["part"])
                    else:
                        new_part.append(subpart)
                
                new_where = CompValue(
                    "GroupGraphPatternSub", 
                    part=new_part
                )
                
                children.append([new_where])
        
        return list(chain(*children))
    
    def build_sub_query(node, new_where=None, new_proj=None):
        if isinstance(node, CompValue):
            if node.name in ["SelectQuery", "ConstructQuery", "DescribeQuery", "AskQuery"]:
                node_args = {
                    "modifier": "DISTINCT"
                }
                node_args["where"] = new_where if new_where else node["where"]
                                
                if new_proj:
                    node_args["projection"] = new_proj
                
                return CompValue("SelectQuery", **node_args) 
                        
    def collect_filter_variables(node, children):
        if isinstance(node, CompValue):
            if node.name == "Filter":
                children.append(list(map(str, _traverseAgg(node, collect_variables))))
        return list(chain(*children))
    
    algebra, options = ctx.invoke(parse_query, queryfile=queryfile)    
    subq_bgp_algebras = []

    consts_info_file = f"{Path(queryfile).parent}/{Path(queryfile).stem}.const.json" 
    consts_info = {}
    with open(consts_info_file, "r") as cifs:
        consts_info = json.load(cifs)
    
    cond_consts = set(consts_info.keys())
    cond_queries = [ q.get("query") for q in consts_info.values() if len(q) > 0 and q.get("query") ]
    if len(cond_queries) > 0:
        cond_query_str = " and ".join(cond_queries)
        logger.debug(f"Cond query: {cond_query_str}")
        cond_algebra = parse_expr(cond_query_str)
        cond_consts.update(_traverseAgg(cond_algebra, collect_constants))
    
    filter_consts = deepcopy(cond_consts)
    optional_consts = set()
    
    # Read and build plan
    for const, info in consts_info.items():
        if len(info) == 0: continue
        # If the constant is exclusive, 
        if info.get("exclusive") == True:
            # The first subquery retrieve the exlusive constant
            subq_bgp_algebras.append(
                (
                    "exclusive",
                    traverse(
                        algebra, 
                        lambda node: build_sub_query(node, new_proj=[
                            CompValue("vars", var=Variable(const))
                        ])
                    )
                )
            )
            
        if info.get("ignoreFilter") == True:
            if const in filter_consts:
                filter_consts.remove(const)
                
        if info.get("optional") == True:
            optional_consts.update(_traverseAgg(parse_expr(info.get("query")), collect_constants))

    filter_consts = filter_consts & set(_traverseAgg(algebra, collect_filter_variables))
    
    # Split UNION queries                    
    subq_bgp_algebras.extend([
        ("join", alg) for alg in
        _traverseAgg(algebra, split_union_query)
    ])
    
    # Split optional queries
    if len(optional_consts) > 0 and _traverseAgg(algebra, has_optional_first_level):
        subq_bgp_algebras.extend([
            ("optional", alg) for alg in
            _traverseAgg(algebra, lambda x, c: split_optional_query(x, c, consts=optional_consts))
        ])
    
    # If there are no subqueries, add a join query with the original query
    if len(subq_bgp_algebras) == 0:
        subq_bgp_algebras = [("join", _traverseAgg(algebra, extract_where)[0])]
        
    subqueries = {}
    
    # Write the subqueries to files
    for subq_id, (kind, subq_bgp_algebra) in enumerate(subq_bgp_algebras):
        subq_vars = set(map(str, _traverseAgg(subq_bgp_algebra, collect_triple_variables))) & cond_consts

        subq_bgp_algebra = traverse(
            subq_bgp_algebra, 
            visitPost=lambda x: remove_filter_with_placeholders(
                x, consts={"query": subq_vars, "filter": filter_consts}
            )
        )
                        
        subq_bgp_algebra = traverse(subq_bgp_algebra, visitPost=disable_orderby_limit)
        subq_bgp_algebra = traverse(subq_bgp_algebra, visitPost=disable_offset)
                
        if kind == "exclusive":
            subqueries[f"sq{subq_id}"] = {
                "kind": kind,
                "query": export_query(subq_bgp_algebra, options)
            }
                            
        elif kind == "join":
            subq_consts = [ CompValue("vars", var=Variable(c)) for c in subq_vars ]    
            subq_algebra = traverse(algebra, visitPost=lambda node: build_sub_query(node, new_where=subq_bgp_algebra, new_proj=subq_consts))
            subqueries[f"sq{subq_id}"] = {
                "kind": kind,
                "query": export_query(subq_algebra, options)
            }
            
        elif kind == "optional":
            subq_vars = subq_vars & optional_consts
            subq_consts = [ CompValue("vars", var=Variable(c)) for c in subq_vars ]    
            subq_algebra = traverse(algebra, visitPost=lambda node: build_sub_query(node, new_where=subq_bgp_algebra, new_proj=subq_consts))
            subqueries[f"sq{subq_id}"] = {
                "kind": kind,
                "query": export_query(subq_algebra, options)
            }

        else:
            raise NotImplementedError(f"Algebra of kind {kind} is not supported!")
        
    with open(outfile, "w") as out_fs:
        json.dump(subqueries, out_fs)

@cli.command()
@click.argument("queryfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("value-selection", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("outfile", type=click.Path(exists=False, file_okay=True, dir_okay=False))
@click.argument("instance-id", type=click.INT)
@click.pass_context
def instanciate_workload(ctx: click.Context, queryfile, value_selection, outfile, instance_id):
    """
    Instantiate a workload by injecting constant values into placeholders in a query.

    Args:
        ctx (click.Context): The Click context object.
        configfile: The path to the configuration file.
        queryfile: The path to the query file.
        value_selection: The path to the value selection file.
        outfile: The path to the output file.
        instance_id: The ID of the instance to use for injecting values.

    Returns:
        None
    """
                    
    value_selection_values = read_csv(value_selection) 
    placeholder_chosen_values = value_selection_values.to_dict(orient="records")[instance_id]
        
    # Open the original queryfile
    algebra, options = ctx.invoke(parse_query, queryfile=queryfile)
    algebra = traverse(algebra, visitPost=lambda node: inject_constant_into_placeholders(node, placeholder_chosen_values))
    algebra = traverse(algebra, visitPost=disable_offset)
    export_query(algebra, options, outfile=outfile)

@cli.command()
@click.argument("provenance", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("opt-comp", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("def-comp", type=click.Path(exists=True, file_okay=True, dir_okay=False))
def unwrap(provenance, opt_comp, def_comp):
    """ Distribute sources for the bgp to all of its triple patterns, then reconsitute the provenance csv
    In:
    |bgp1|bgp2|
    | x  | y  |
    
    Out
    |tp1|tp2|...|tpn|
    | x | x |...| y |

    Args:
        provenance (pd.DataFrame): _description_
        opt_comp (dict): _description_
        def_comp (dict): _description_
    """

    provenance_df = pd.read_csv(provenance)

    with open(opt_comp, 'r') as opt_comp_fs, open(def_comp, 'r') as def_comp_fs:
        opt_comp_dict = json.load(opt_comp_fs)
        def_comp_dict = json.load(def_comp_fs)

        provenance_df.to_csv(f"{provenance}.opt", index=False)

        reversed_def_comp = dict()
        for k, v in def_comp_dict.items():
            tp = " ".join(v)
            if reversed_def_comp.get(tp) is None:
                reversed_def_comp[tp] = []
            reversed_def_comp[tp].append(k)

        result = dict()

        for bgp in provenance_df.columns:
            tps = opt_comp_dict[bgp]
            sources = provenance_df[bgp]
            for tp in tps:
                tpids = reversed_def_comp[tp]
                for tpid in tpids:
                    result[tpid] = sources.to_list()

        result_df = pd.DataFrame.from_dict(result)
        sorted_columns = "tp" + result_df.columns \
            .str.replace("tp", "", regex=False) \
            .astype(int).sort_values() \
            .astype(str)

        result_df = result_df.reindex(sorted_columns, axis=1)
        result_df.to_csv(provenance, index=False)

@cli.command()
@click.argument("queryfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("outfile", type=click.Path(exists=False, file_okay=True, dir_okay=False))
@click.pass_context
def decompose_query(ctx: click.Context, queryfile, outfile):
    """Decompose a query into its triple patterns and bgp then export to a file

    Args:
        ctx (click.Context): click constant to forward to other click commands
        queryfile (_type_): the initial queryfile
        outfile (_type_): the final output query
    """

    def translate(node, children):
        
        if isinstance(node, CompValue):
            if node.name == "pname":
                return f'{node["prefix"]}:{node["localname"]}'
        elif hasattr(node, "n3"):
            return str(node)
    
        return children[0] if isinstance(children, list) and len(children) > 0 else children
    
    def visit_add_triple(node, children):
        if isinstance(node, CompValue):
            if node.name == "TriplesBlock":
                for triple in node["triples"]:
                    s = _traverseAgg(triple[0], translate)
                    p = _traverseAgg(triple[1], translate)
                    o = _traverseAgg(triple[2], translate)
                    children.append([(s, p, o)])
  
        return list(chain(*children))
        
    composition = {}
    algebra, _ = ctx.invoke(parse_query, queryfile=queryfile)
    for triple_id, triple in enumerate(_traverseAgg(algebra, visit_add_triple)):
        composition[f"tp{triple_id}"] = triple
        
    with open(outfile, "w") as out_fs:
        json.dump(composition, out_fs)

def parse_query_proc(queryfile=None, querydata=None):
    if queryfile:
        with open(queryfile, "r") as qf:
            querydata = qf.read()
            
    misc = {
        "explicit_join_order": False
    }
    
    query = deepcopy(querydata)
    
    if re.search(r'DEFINE sql:select-option "order"', query):
        misc["explicit_join_order"] = True
        query = query.replace('DEFINE sql:select-option "order"', '')
    
    algebra = parseQuery(query)
    return algebra, misc
    

@cli.command()
@click.option("--queryfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.option("--querydata", type=click.STRING)
@click.option("--print-algebra", is_flag=True, default=False)
@click.option("--translate", is_flag=True, default=False)
def parse_query(queryfile, querydata, print_algebra, translate):
    """Parse a query string into an algebra object.

    This function takes a query string and parses it into an algebra object. The query string can be provided either through a file or directly as data.

    Args:
        queryfile (str): The path to the query file to parse. If provided, the function will read the query string from the file.
        querydata (str): The query string to parse. If `queryfile` is not provided, the function will use this argument as the query string.
        print_algebra (bool): If True, the parsed algebra object will be printed.
        translate (bool): If True, the parsed algebra object will be translated.

    Returns:
        tuple: A tuple containing the parsed algebra object and any additional miscellaneous data.

    Raises:
        RuntimeError: If both `queryfile` and `querydata` are None, indicating that no query is provided.

    """
    
    if queryfile is None and querydata is None:
        raise RuntimeError("No query to parse...")
    
    algebra, misc = parse_query_proc(queryfile=queryfile, querydata=querydata)
    
    if print_algebra and translate:
        pprintAlgebra(translateQuery(algebra))
    elif print_algebra:
        pprint(algebra)
    return algebra, misc
    
def export_query(algebra, options, outfile=None):
    translated = translateQuery(algebra)
    query = translateAlgebra(translated)
    
    # Remove empty lines
    with StringIO(query) as qfs:
        qlines = [ line for line in qfs.readlines() if line.strip() != "" ]
        query = "".join(qlines)
        
    # Create a temporary file to hold the query
    # with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
    #     temp_file.write(query)   
    # query = pretty_print_query(temp_file.name) 
    # os.unlink(temp_file.name)
    
    if len(query.strip()) == 0:
        raise RuntimeError("Empty query...")
        
    for key, value in options.items():
        if key == "explicit_join_order" and value:
            query = f'DEFINE sql:select-option "order"\n{query}'
    
    if outfile:
        #Path(outfile).parent.mkdir(parents=True, exist_ok=True)
        with open(outfile, "w") as out_fs:
            out_fs.write(query)
    return query
    
def read_csv(csv_file):
    with open(csv_file, "r") as header_fs:
        header = header_fs.readline().strip().replace('"', '').split(",")
        tmp = pd.read_csv(csv_file, parse_dates=[h for h in header if "date" in h], low_memory=False)
        return tmp
        
@cli.command()
@click.argument("queryfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("outfile", type=click.Path(dir_okay=False, file_okay=True))
@click.pass_context
def build_provenance_query(ctx: click.Context, queryfile, outfile):
    """
    Builds a provenance query by wrapping each triple pattern with graph clause
    
    1. Wrap each triple pattern with GRAPH clause
    2. Change projection to include all graph variables + SELECT DISTINCT
    3. Disable filters

    Args:
        queryfile (str): The path to the input query file.
        outfile (str): The path to the output file where the modified query will be written.

    Returns:
        None
    """
    
    algebra, options = ctx.invoke(parse_query, queryfile=queryfile)
    algebra = traverse(algebra, visitPost=add_graph_to_triple_pattern)
    algebra = traverse(algebra, visitPost=replace_select_projection_with_graph)
    algebra = traverse(algebra, visitPost=disable_orderby_limit)
    algebra = traverse(algebra, visitPost=disable_offset)
    export_query(algebra, options, outfile=outfile)
        
@cli.command()
@click.argument("configfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("constfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("subqueryfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("workload-value-selection", type=click.Path(exists=False, file_okay=True, dir_okay=False))
@click.argument("n-instances", type=click.INT)
@click.pass_context
def create_workload_value_selection(ctx: click.Context, configfile, constfile, subqueryfile, workload_value_selection, n_instances):
    """Create a value selection file from a query file

    Args:
        queryfile (str): Path to the query file.
        value_selection (str): Path to the value selection file.
        n_instances (int): Number of instances to create in the value selection file.
        constfile (str): Path to the constfile.
        seed (int): Random seed for reproducibility.
        workload_value_selection (str): Path to the output workload value selection file.
    """
    
    # Read config
    config = load_config(configfile)
    batch0_endpoint = config["generation"]["virtuoso"]["default_endpoint"]

    # Get subqueries
    subqueries = {}
    with open(subqueryfile, "r") as sqfs:
        subqueries = json.load(sqfs)
        
    b_require_exclusive = False
    exclusive_sq = None

    for subq_id, subq_info in subqueries.items():
        subq_kind = subq_info["kind"]
        subq_text = subq_info["query"]
        
        subq_value_selection_file = f"{Path(subqueryfile).parent}/{Path(subqueryfile).stem}.{subq_id}.csv"
        
        if not os.path.exists(subq_value_selection_file):
            logger.debug(f"Executing subquery:\n {subq_text}")
            ctx.invoke(
                execute_query, 
                querydata = subq_text,
                outfile=subq_value_selection_file, 
                endpoint=batch0_endpoint
            )
        subqueries[subq_id]["subq_value_selection_file"] = subq_value_selection_file
            
        if subq_kind == "exclusive":
            b_require_exclusive = True
            exclusive_sq = subq_text
                
    # Update subqueries file
    with open(subqueryfile, "w") as sqfs:
        json.dump(subqueries, sqfs)
    
    # Create workload value selection
    if b_require_exclusive:
        ctx.invoke(
            create_workload_value_selection_with_exclusive,
            configfile=configfile,
            querydata=exclusive_sq,
            excl_value_selection=subq_value_selection_file,
            subqueries_file=subqueryfile,
            n_instances=n_instances,
            workload_value_selection=workload_value_selection,
            constfile=constfile
        )
    else:
        ctx.invoke(
            create_workload_value_selection_with_constraints, 
            subquery_file=subqueryfile, 
            n_instances=n_instances, 
            workload_value_selection=workload_value_selection,
            constfile=constfile
        )

@click.command()
@click.argument("configfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("excl-value-selection", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("subqueries-file", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("n-instances", type=click.INT)
@click.option("--queryfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.option("--querydata", type=click.STRING)
@click.option("--workload-value-selection", type=click.Path(exists=False, file_okay=True, dir_okay=False))
@click.option("--constfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.option("--seed", type=click.INT, default=PANDAS_RANDOM_STATE)
@click.pass_context
def create_workload_value_selection_with_exclusive(ctx: click.Context, configfile, excl_value_selection, subqueries_file, n_instances, queryfile, querydata, workload_value_selection, constfile, seed):
            
    # Read config
    config = load_config(configfile)
    batch0_endpoint = config["generation"]["virtuoso"]["default_endpoint"]
    
    # Composition
    comp = {}
    with open(constfile, "r") as cfs:
        comp = json.load(cfs)
    
    # Obtain the rest of the placeholders using VALUES
    workload_subq_value_selection = (
        read_csv(excl_value_selection)
        .sample(n_instances, random_state=seed)
        .reset_index(drop=True)
    )
        
    subq_algebra, _ = ctx.invoke(parse_query, queryfile=queryfile, querydata=querydata)
    subq_variables = set(map(str, _traverseAgg(subq_algebra, collect_triple_variables)))
    
    cond_queries = [ q.get("query") for q in comp.values() if len(q) > 0 and q.get("query") ]
    consts = set(comp.keys())
    if len(cond_queries) > 0:
        cond_query_str = " and ".join(cond_queries)
        query_algebra = parse_expr(cond_query_str)
        consts.update(_traverseAgg(query_algebra, collect_constants))
    consts = consts & subq_variables
    
    filter_consts = deepcopy(consts)
    for const, info in comp.items():
        if info.get("ignoreFilter") == True:
            if const in filter_consts:
                filter_consts.remove(const)
    
    tmp_dfs = []
    for instance_id in tqdm(range(n_instances)):
        tmp_query_algebra, options = ctx.invoke(parse_query, queryfile=queryfile, querydata=querydata)  
        inline_data = workload_subq_value_selection.iloc[instance_id]
        if isinstance(inline_data, pd.Series):
            inline_data = inline_data.to_frame().T
        inline_data = inline_data.to_dict(orient="list")
        query_consts = set(map(str, _traverseAgg(tmp_query_algebra, collect_triple_variables)))
        tmp_query_algebra = traverse(tmp_query_algebra, visitPost=lambda node: add_values_with_placeholders(node, inline_data))
        tmp_query_algebra = traverse(tmp_query_algebra, visitPost=lambda node: remove_filter_with_placeholders(node, consts={"query": query_consts,"select": consts, "filter": filter_consts}))
        tmp_query_algebra = traverse(tmp_query_algebra, disable_orderby_limit)
        tmp_query_algebra = traverse(tmp_query_algebra, disable_offset)
        tmp_query_str = export_query(tmp_query_algebra, options)
        
        # if not os.path.exists(tmp_query_result_file):              
        tmp_query_result = ctx.invoke(
            execute_query, 
            querydata=tmp_query_str, 
            endpoint=batch0_endpoint, 
        )        
        
        tmp_df: pd.DataFrame = ctx.invoke(
            create_workload_value_selection_with_constraints, 
            value_selection_data=tmp_query_result, 
            n_instances=1, 
            seed=PANDAS_RANDOM_STATE+instance_id,
            constfile=constfile
        )
        
        tmp_dfs.append(tmp_df)
        
    workload_subq_value_selection = pd.concat(tmp_dfs, ignore_index=True)
    workload_subq_value_selection.to_csv(workload_value_selection, index=False)
    return workload_subq_value_selection
                            
@cli.command()
@click.option("--value-selection", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.option("--value-selection-data", type=click.STRING)
@click.option("--n-instances", type=click.INT)
@click.option("--subquery-file", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.option("--workload-value-selection", type=click.Path(exists=False, file_okay=True, dir_okay=False))
@click.option("--constfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.option("--seed", type=click.INT, default=PANDAS_RANDOM_STATE)
@click.pass_context
def create_workload_value_selection_with_constraints(ctx: click.Context, value_selection, value_selection_data, n_instances, subquery_file, workload_value_selection, constfile, seed):
    """Sample {n_instances} rows amongst the value selection. 
    The sampling is guaranteed to return results for provenance queries, using statistical criteria:
        1. Percentiles for numerical attribute: if value falls between 25-75 percentile
        2. URL: ignored
        3. String value: contains top 10 most common words

    Args:
        value_selection (_type_): _description_
        workload_value_selection (_type_): _description_
        n_instances (_type_): _description_
    """
    
    with open(constfile, "r") as cfs:
        comp = json.load(cfs)
        
    subqueries = {}
    
    if subquery_file:
        with open(subquery_file, "r") as sqfs:
            subqueries = json.load(sqfs)
    
    subquery_results = []
    if value_selection_data is not None:
        subquery_results.append(value_selection_data)
    
    if value_selection is None: 
        subquery_result_files = [ sq_info["subq_value_selection_file"] for sq_info in subqueries.values() if "subq_value_selection_file" in sq_info ]

        join_cols = set()        
        for subquery_result_file in subquery_result_files:
            tmp = read_csv(subquery_result_file)
            subquery_results.append(tmp)
            
            if len(join_cols) == 0:
                join_cols = set(tmp.columns)
            else:
                join_cols = join_cols & set(tmp.columns)
        
        logger.debug(f"Join columns: {join_cols}")
    else:
        subquery_results.append(read_csv(value_selection))

    # Join all subquery results  
    df = subquery_results.pop(0)
    while len(subquery_results) > 0:
        tmp = subquery_results.pop(0)
        logger.debug(f"Left join with {df.columns}")
        logger.debug(f"Right join with {tmp.columns}")
        df = pd.merge(df, tmp, how="inner", on=list(join_cols))
    
    # Filter out numerical values that are not in the 10-90 percentile    
    numerical_cols = []
    for col in df.columns:
        values = df[col].dropna()
        if not values.empty:
            dtype = values.dtype
            if np.issubdtype(dtype, np.number) or np.issubdtype(dtype, np.datetime64):
                numerical_cols.append(col)
    
    numerical = df[numerical_cols]
    if not numerical.empty:
        query = " or ".join([
            f"( `{col}` >= {repr(numerical[col].quantile(0.10))} and `{col}` <= {repr(numerical[col].quantile(0.90))} )"
            # f"( `{col}` > {repr(numerical[col].min())} and `{col}` < {repr(numerical[col].max())} )"
            for col in numerical.columns
        ])

        df = df.query(query)   
    
    def has_only_placeholder(node, children):
        if isinstance(node, CompValue):
            if node.name == "Placeholder":
                return True
            return False
        return all(children)
    
    def remove_placeholder_nodes(node, non_placeholder_queries, non_placeholder_names, df):
        if isinstance(node, CompValue):
            if node.name == "ComparisonCondition":
                left, op, right = node["left"]["column_name"], node["op"]["op"], node["right"]["column_name"]
                logger.debug(f"Inspecting: left={repr(left)}, op={repr(op)}, right={repr(right)}")
                if left not in df.columns:
                    non_placeholder_queries.append(node)
                    non_placeholder_names.add(right)
                    return CompValue("Placeholder")
                if right not in df.columns:
                    non_placeholder_queries.append(node)
                    non_placeholder_names.add(right)
                    return CompValue("Placeholder")
            if node.name == "BinaryExpr":
                left, op, right = node["left"], node["op"], node["right"]
                if left.name == "Placeholder" and right.name == "Placeholder":
                    return CompValue("Placeholder")
                elif left.name == "Placeholder":
                    return right
                elif right.name == "Placeholder":
                    return left
            elif node.name == "UnaryExpr":
                op, right = node["op"], node["right"]
                if right.name == "Placeholder":
                    return CompValue("Placeholder")
                
    def create_placeholder_value(row, placeholder_query):
        """Create a placeholder value for a given row based on the placeholder query.
    
        Args:
            row (dict): The row containing the values.
            placeholder_query (dict): The placeholder query specifying the left and right column names and the operator.

        Raises:
            ValueError: If the left column name is not found in the dataframe.
            ValueError: If the right column name is not found in the dataframe.

        Returns:
            dict: The updated row with the placeholder value filled.
        """
        
        left, op, right = placeholder_query["left"]["column_name"], placeholder_query["op"]["op"], placeholder_query["right"]["column_name"]
        
        def estimate_replacement_value_based_on_op(op, value):
            epsilon = None
            if str(value).isnumeric():
                epsilon = 1
            elif isinstance(value, pd.Timestamp):
                epsilon = pd.Timedelta(days=1)
            else:
                raise ValueError(f"Unsupported value type {type(value)} for value {value}!")
            
            if op in ["=", "!=", "in"]:
                return value
            elif op in [">", ">="]:
                return value - epsilon
            elif op in ["<", "<="]:
                return value + epsilon
            else:
                raise ValueError(f"Unsupported operator: {op}")
        
        # Left is the placeholder, select random value in df[right]
        # x < p1
        if left not in df.columns:
            logger.debug(f"Row: {row}")
            subq = f"{right} {op} {repr(estimate_replacement_value_based_on_op(op, row[right]))}" 
            logger.debug(f"subq: {subq}")
            candidates = df.query(subq)[right]
            if candidates.empty:
                raise ValueError(f"Query {subq} returns no result!")
            row[left] = candidates.sample(1, random_state=seed).item()
        
        # Right is the placeholder, select random value in df[left]
        elif right not in df.columns:
            subq = f"{left} {op} {repr(estimate_replacement_value_based_on_op(op, row[left]))}" 
            candidates = df.query(subq)[left]
            if candidates.empty:
                raise ValueError(f"Query {subq} returns no result!")
            row[right] = candidates.sample(1, random_state=seed).item()
                
        return row
                    
    non_placeholder_queries = []
    non_placeholder_names = set()
    
    cond_queries = [ q.get("query") for q in comp.values() if len(q) > 0 and q.get("query") ]
    if len(cond_queries) > 0:
        query = " and ".join(cond_queries)
        logger.debug(f"Query to transform: {query}")
        algebra = parse_expr(query)
        algebra = traverse(
            algebra, 
            visitPost=lambda node: remove_placeholder_nodes(
                node, non_placeholder_queries, non_placeholder_names, df
            )
        )
        
        if not _traverseAgg(algebra, has_only_placeholder):
            logger.debug("Filtering on placeholder columns...")
            query = translate_query(algebra)    
            df = df.query(query)
    
    if df.empty:
        raise ValueError("No results after filtering...")
    
    # Sample n_instances
    result = df.sample(n_instances)
    
    # Get a value for the placeholder
    for placeholder_query in non_placeholder_queries:
        logger.debug(f"Placeholder query: {placeholder_query}")
        result = result.apply(lambda row: create_placeholder_value(row, placeholder_query), axis=1) 
    
    # Remove placeholder columns
    result.drop(columns=non_placeholder_names, axis=1, inplace=True)
    if workload_value_selection:
        result.to_csv(workload_value_selection, index=False)
    return result


if __name__ == "__main__":
    cli()
