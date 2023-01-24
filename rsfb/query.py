import json
from pathlib import Path
from time import time

from collections import Counter
import os
from pathlib import Path
from tqdm import tqdm

import re
import numpy as np
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, CSV, DESCRIBE
from io import BytesIO, StringIO
from rdflib import Literal, URIRef, XSD
import click

from utils import load_config

import nltk
nltk.download('stopwords', quiet=True)
nltk.download('punkt', quiet=True)

from nltk.corpus import stopwords as nltk_stopwords
from nltk.tokenize import word_tokenize, RegexpTokenizer
tokenizer = RegexpTokenizer(r"\w+")

from ftlangdetect import detect
from iso639 import Lang

@click.group
def cli():
    pass

stopwords = []
for lang in ["english"]:
    stopwords.extend(nltk_stopwords.words(lang))

def lang_detect(txt):
    lines = str(txt).splitlines()
    result = Counter(map(lambda x: Lang(detect(text=x, low_memory=False)["lang"]).name.lower(), lines)).most_common(1)[0]
    return result

def exec_query(configfile, query, batch_id=None, error_when_timeout=False):
    
    config = load_config(configfile)["generation"]
    endpoint = config["sparql"]["endpoint"]
    sparql_endpoint = SPARQLWrapper(endpoint)
    if error_when_timeout: sparql_endpoint.addParameter("timeout", "300000") # in ms
        
    if batch_id is not None:
        from_clause = []
        from_keyword = "FROM" if re.search(r"GRAPH\s+(\?\w+\s*)+\{", query) is None else "FROM NAMED"
                
        for schema_name, schema_props in config["schema"].items():
            if schema_props["is_source"]:
                n_items = schema_props["params"][f"{schema_name}_n"]
                
                _, edges = np.histogram(np.arange(n_items), config["n_batch"])
                edges = edges[1:].astype(int) + 1
                for id in range(edges[batch_id]):
                    provenance =  re.sub(re.escape(f"{{%{schema_name}_id}}"), f"{schema_name}{id}", schema_props["provenance"])
                    from_clause.append(f"{from_keyword} <{provenance}>")
    
        query = re.sub(r"(SELECT|CONSTRUCT|DESCRIBE)(\s+DISTINCT)?\s+(.*)\s+WHERE", rf"\1\2 \3 \n{' '.join(from_clause)}\nWHERE", query)
    
    sparql_endpoint.setMethod("POST")
    sparql_endpoint.setReturnFormat(CSV)        
    sparql_endpoint.setQuery(query)
    response = sparql_endpoint.query()
    result = response.convert()
    return response, result

def __parse_query(queryfile):
    """Parse input queryfile into get placeholders

    Args:
        queryfile (_type_): _description_

    Returns:
        _type_: subDict containing constant, their depending contants and a prefix dictionary
    """
    parse_result = dict()
    prefix_full_to_alias = dict()
    prefix_full_to_alias["http://www.w3.org/2001/XMLSchema#"] = "xsd"
    prefix_full_to_alias["http://www.w3.org/2002/07/owl#"] = "owl"

    for line in open(queryfile, mode="r").readlines():
        toks = np.array(re.split(r"\s+", line))
        if "#" in toks:
            if "const" in toks:
                if (comp := re.search(r"const\s+(\?\w+)\s+(\W|\w+)\s+(\?\w+)", line)) is not None:
                    op = comp.group(2)
                    const = comp.group(1)
                    constSrc = comp.group(3)
                    parse_result[const] = {"type": "comparison", "src": constSrc, "op": op}
                elif(assertion := re.search(r"const\s+(not)\s+(\?\w+)", line)) is not None:
                    op = assertion.group(1)
                    const = assertion.group(2)
                    parse_result[const] = {"type": "assertion", "op": op}
                else:
                    const_search = re.search(r"const\s+(\?\w+)", line)
                    if const_search is not None:
                        const = const_search.group(1)
                        parse_result[const] = None
                continue

        elif "PREFIX" in toks:
            regex = r"PREFIX\s+([\w\-]+):\s*<(.*)>\s*"
            prefixName = re.search(regex, line).group(1)
            prefixSub = re.search(regex, line).group(2)
            prefix_full_to_alias[prefixSub] = prefixName
    
    return prefix_full_to_alias, parse_result

def __get_uninjected_placeholders(queryfile):
    _, parse_result = __parse_query(queryfile)
    return [
        const if (resource is None or resource.get("src") is None ) else resource["src"]
        for const, resource in parse_result.items()
    ]
    

@cli.command()
@click.argument("configfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("queryfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("outfile", type=click.Path(exists=False, file_okay=True, dir_okay=False))
@click.option("--sample", type=click.INT)
@click.option("--ignore-errors", is_flag=True, default=False)
@click.option("--batch-id", type=click.INT)
def execute_query(configfile, queryfile, outfile, sample, ignore_errors, batch_id):
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
    query_text = open(queryfile, mode="r").read()
    _, result = exec_query(configfile=configfile, query=query_text, batch_id=batch_id, error_when_timeout=True)  

    header = BytesIO(result).readline().decode().strip().replace('"', '').split(",")
    csvOut = pd.read_csv(BytesIO(result), parse_dates=[h for h in header if "date" in h])

    if csvOut.empty and not ignore_errors:
        print(query_text)
        raise RuntimeError(f"{queryfile} returns no result...")

    if sample is None:
        csvOut.to_csv(outfile, index=False)
    else:
        csvOut.sample(sample).to_csv(outfile, index=False)

    return csvOut.shape[0]

@cli.command()
@click.argument("queryfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("outfile", type=click.Path(exists=False, file_okay=False, dir_okay=True))
def build_value_selection_query(queryfile, outfile):
    """From an input query, generate a selection pool of values for placeholders
    TODO:
        [] avoid running big query using ORDER BY RAND() LIMIT n
    Args:
        queryfile (_type_): input query
        n_variations (_type_): number of variations

    Returns:
        _type_: a csv file at outfile containing values for placeholders
    """
    consts = __get_uninjected_placeholders(queryfile)

    query = open(queryfile).read()
    query = re.sub(r"(SELECT|CONSTRUCT|DESCRIBE)(\s+DISTINCT)?\s+(.*)\s+WHERE", f"SELECT DISTINCT {' '.join(consts)} WHERE", query)
    #query = re.sub(r"((\?\w+|\w+:\w+|<\S+>)\s+(\?\w+|a|\w+:\w+|<\S+>)\s+(<\S+>))", r"##\1", query)
    query = re.sub(r"(#)*(LIMIT|FILTER\s+|OFFSET|ORDER)", r"##\2", query)

    with open(outfile, "w+") as out:
        out.write(query)
        out.close()

    return query

@cli.command()
@click.argument("queryfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("injection_cache", type=click.Path(exists=True, file_okay=True, dir_okay=False))
def inject_from_cache(queryfile, cache_file):
    
    query = open(queryfile, "r").read()
    injection_cache = json.load(open(cache_file, "r"))
    for variable, value in injection_cache.items():
        query = re.sub(rf"{re.escape(variable)}(\W)", rf"{value}\1", query)

@cli.command()
@click.argument("queryfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("value-selection", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.option("--ignore-errors", is_flag=True, default=False)
@click.option("--accept-random", is_flag=True, default=False)
@click.option("--instance-id", type=click.INT)
def inject_constant(queryfile, value_selection, ignore_errors, accept_random, instance_id):
    """Inject constant in placeholders for a query template.

    Args:
        queryfile ([type]): [description]
        value_selection ([type]): [description]
        instance_id ([type]): [description]
        ignore_errors ([type]): [description]

    Raises:
        RuntimeError: [description]

    Returns:
        [type]: [description]
    """

    qroot = Path(queryfile).parent
    query = open(queryfile, "r").read()

    cache_filename = f"{qroot}/injection_cache.json"
    injection_cache: dict = json.load(open(cache_filename, "r")) if os.path.exists(cache_filename) else dict()

    header = open(value_selection, "r").readline().strip().replace('"', '').split(",")
    value_selection_values = pd.read_csv(value_selection, parse_dates=[h for h in header if "date" in h])
    placeholder_chosen_values = value_selection_values
    
    prefix_full_to_alias, parse_result = __parse_query(queryfile)

    for const, resource in parse_result.items():                
        # Replace all tokens in subDict
        column = const[1:] if (resource is None or resource.get("src") is None) else resource["src"][1:]

        # When not using workload value_selection_query, i.e, filling partially injected queries
        if "workload_" not in value_selection:
            # dedup_query = " and ".join([ 
            #     f"`{subSrc}` != {repr(value)}" \
            #         for const, value in injection_cache.items() \
            #         if const != subSrc #and value_selection_values[const].dtype == value_selection_values[subSrc].dtype 
            # ] + [ f"`{subSrc}`.notna()" ])
            # value_selection_values = value_selection_values.query(dedup_query).sample(1)
            placeholder_chosen_values = value_selection_values.sample(1)

        repl_val = placeholder_chosen_values[column].item() if instance_id is None else placeholder_chosen_values.loc[instance_id, column]
        if pd.isnull(repl_val): 
            if accept_random:
                repl_val = value_selection_values[column].dropna().sample(1).item()
            elif ignore_errors: continue
            else: raise RuntimeError(f"There is no value for {column}")

        # Replace for FILTER clauses
        if resource is not None:

            dtype = value_selection_values[column].dtype
            epsilon = None
            if np.issubdtype(dtype, np.number):
                epsilon = 0
            elif np.issubdtype(dtype, np.datetime64):
                epsilon = pd.Timedelta(1, unit="day")

            if ">" in resource["op"]:
                repl_val = value_selection_values[column].dropna().max() + epsilon
            elif "<" in resource["op"]:
                repl_val = value_selection_values[column].dropna().min() - epsilon
            
            # Special treatment for REGEX
            #   Extract randomly 1 from 10 most common words in the result list
            elif resource["op"] == "in":
                langs = value_selection_values[column].dropna().apply(lambda x: lang_detect(x))
                for lang in set(langs):
                    try: stopwords.extend(nltk_stopwords.words(lang))
                    except: continue
                
                words = [
                    token for token in
                    tokenizer.tokenize(str(value_selection_values[column].str.cat(sep=" ")).lower())
                    if token not in stopwords
                ]
                    
                # Option 1: Choose randomly amongst 10 most common words
                # bow = Counter(words)
                # bow = Counter({ k: v for k, v in bow.items() if k not in stopwords})
                # print(bow.most_common(10))
                # repl_val = np.random.choice(list(map(lambda x: x[0], bow.most_common(10))))

                # Option 2: Choose randomly amongst words
                repl_val = np.random.choice(words)
            
            # Special treatment for exclusion
            # Query with every other placeholder set
            elif resource["op"] == "not":
                # From q03: user asks for products having several features but not having a specific other feature. 
                exclusion_query = " and ".join([f"`{column}` != {repr(repl_val)}"] + [ f"`{k}` != {repr(v)}" for k, v in injection_cache.items() ])
                repl_val = value_selection_values.query(exclusion_query)[column].sample(1)
            
        # Convert Pandas numpy object to Python object
        try: repl_val = repl_val.item()
        except: pass

        if str(repl_val).startswith("http") or str(repl_val).startswith("nodeID"): 
            repl_val = URIRef(repl_val).n3()
        else:
            repl_val = Literal(repl_val).n3()

        # Shorten string representations with detected and common prefixes
        for prefixSub, prefixName in prefix_full_to_alias.items():
            if prefixSub in repl_val:
                repl_val = re.sub(rf"<{prefixSub}(\w+)>", rf"{prefixName}:\1", repl_val)
                missing_prefix = f"PREFIX {prefixName}: <{prefixSub}>"
                if re.search(re.escape(missing_prefix), query) is None:
                    query = f"PREFIX {prefixName}: <{prefixSub}>" + "\n" + query
        
        injection_cache[column] = repl_val
        query = re.sub(rf"{re.escape(const)}(\W)", rf"{repl_val}\1", query)
    
    json.dump(injection_cache, open(cache_filename, "w"))
    return query

@cli.command()
@click.argument("configfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("queryfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("value-selection", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("outfile", type=click.Path(exists=False, file_okay=True, dir_okay=False))
@click.argument("instance-id", type=click.INT)
@click.argument("batch-id", type=click.INT)
@click.pass_context
def instanciate_workload(ctx: click.Context, configfile, queryfile, value_selection, outfile, instance_id, batch_id):
    """From a query template, instanciate the {instance_id}-th instance.

    1. - Until all placeholders are replaced:
    2. -    On the first iteration, inject values from the initial value_selection csv 
            (common to all instances of the workload), produce the partially injected query.
    3. -    On other iteration, execute the partially injected query to find the missing constants

    Args:
        ctx (click.Context): click constant to forward to other click commands
        queryfile (_type_): the initial queryfile
        outfile (_type_): the final output query
        instance_id (_type_): the query instance id with respect to workload
        endpoint (_type_): the sparql endpoint
    """

    query = open(queryfile, "r").read()
    initial_queryfile = queryfile
    consts = __get_uninjected_placeholders(initial_queryfile)
    select = re.search(r"(SELECT|CONSTRUCT|DESCRIBE)(\s+DISTINCT)?\s+(.*)\s+WHERE", query).group(3)     

    qname = Path(outfile).stem
    qroot = Path(outfile).parent

    solution_option = 1

    itr = 0
    while len(consts) > 0:
        print(f"Iteration {itr}...")
        next_queryfile = f"{qroot}/{qname}.sparql"
        ctx.invoke(build_value_selection_query, queryfile=initial_queryfile, outfile=next_queryfile)

        # First iteration, inject value using initial value_selection of the workload
        if itr == 0:
            next_value_selection = value_selection
            query = ctx.invoke(inject_constant, queryfile=next_queryfile, value_selection=next_value_selection, ignore_errors=True, instance_id=instance_id)
        
        # Execute the partially injected query to find the rest of constants
        else:
            query = re.sub(r"(SELECT|CONSTRUCT|DESCRIBE)(\s+DISTINCT)?\s+(.*)\s+WHERE", rf"\1\2 {' '.join(consts)}\nWHERE", query)
            
            # Option 1: Exclude the partialy injected query to refill
            if solution_option == 1:
                try:
                    print("Option 1: Exclude the partialy injected query to refill")
                    next_value_selection = f"{qroot}/{qname}.csv"
                    ctx.invoke(execute_query, configfile=configfile, queryfile=next_queryfile, outfile=next_value_selection, batch_id=batch_id)
                    query = ctx.invoke(inject_constant, queryfile=next_queryfile, value_selection=next_value_selection, ignore_errors=False)
                except RuntimeError:
                    solution_option = 2  
                    continue
            
            # Option 2: extract the needed value for placeholders from value_selection.csv
            elif solution_option == 2:
                try:
                    print("Option 2: extract the needed value for placeholders from value_selection.csv")
                    next_value_selection = f"{Path(value_selection).parent}/value_selection.csv"
                    query = ctx.invoke(inject_constant, queryfile=next_queryfile, value_selection=next_value_selection, ignore_errors=False)
                except RuntimeError:
                    solution_option = 3
                    continue

            # Option 3: Relaxing the query knowing we are in an optional clause
            elif solution_option == 3:
                try:
                    print("Option 3: Relax the query knowing we are in an optional clause")
                    print("Relaxing query...")
                    relaxed_query = re.sub(r"(#)*((\?\w+|\w+:\w+|<\S+>)\s+(\?\w+|a|\w+:\w+|<\S+>)\s+(\w+:\w+|<\S+>)) ", r"##\2", query)
                    next_queryfile = f"{qroot}/{qname}_relaxed.sparql"
                    with open(next_queryfile, "w+") as f:
                        f.write(relaxed_query)
                        f.close()

                    next_value_selection = f"{qroot}/{qname}.csv"
                    ctx.invoke(execute_query, configfile=configfile, queryfile=next_queryfile, outfile=next_value_selection, batch_id=batch_id)
                    relaxed_query = ctx.invoke(inject_constant, queryfile=next_queryfile, value_selection=next_value_selection, ignore_errors=False)
                    print("Restoring query...")
                    query = re.sub(r"(#)*((\?\w+|\w+:\w+|<\S+>)\s+(\?\w+|a|\w+:\w+|<\S+>)\s+(\w+:\w+|<\S+>)) ", r"\2", relaxed_query)
                except:
                    raise RuntimeError("Something went wrong: (1) The data are not properly ingested. (2) The generated files are corrupted due to interuption.")

        with open(next_queryfile, "w+") as f:
            f.write(query)
            f.close()

        initial_queryfile = next_queryfile
        consts = __get_uninjected_placeholders(initial_queryfile)
        itr+=1
    
    query = re.sub(r"(SELECT|CONSTRUCT|DESCRIBE)(\s+DISTINCT)?\s+(.*)\s+WHERE", rf"\1\2 {select} WHERE", query)
    query = re.sub(r"(regex|REGEX)\s*\(\s*(\?\w+)\s*,", r"\1(lcase(\2),", query)
    #query = re.sub(r"(#){2}(LIMIT|FILTER\s+|OFFSET|ORDER)", r"\2", query)
    query = re.sub(r"(#){2}(FILTER\s+)", r"\2", query)

    with open(next_queryfile, "w+") as f:
        f.write(query)
        f.close()

@cli.command()
@click.argument("queryfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("outfile", type=click.Path(dir_okay=False, file_okay=True))
def build_provenance_query(queryfile, outfile):
    """Given an input query, this function select the {instance_id}-th row in {value_selection}, 
    replace each marked variable with a value in that row.
     

    Args:
        queryfile (path): input query
        instance_id (integer): instance_id number
        value_selection (path): the value seletion csv file
        outfile (_type_): file that holds the result
        endpoint (_type_): the virtuoso endpoint

    Returns:
        _type_: _description_
    """

    query = open(queryfile).read()
    prefix_full_to_alias, _ = __parse_query(queryfile)
    ss_cache = dict()

    # Wrap each tp with GRAPH clause
    ntp = 0
    wherePos = np.inf
    composition = dict()
    prefix_alias_to_full = {v: k for k, v in prefix_full_to_alias.items()}

    queryHeader = ""
    queryBody = ""
    for cur, line in enumerate(StringIO(query).readlines()):
        if "WHERE" not in line and cur < wherePos: 
            if "SELECT" not in line:
                queryHeader += line
            continue
        
        subline_search = re.search(r"^(\s|(\w+\s*\{)|\{)*((\?\w+|\w+:\w+|<\S+>)\s+(\?\w+|a|\w+:\w+|<\S+>)\s+(\?\w+|\w+:\w+|<\S+>))(\s|\}|\.)*\s*$", line)
        if subline_search is not None:
            subline = subline_search.group(3)
            subject = subline_search.group(4)
            predicate = subline_search.group(5)
            object = subline_search.group(6)
            if subline not in ss_cache.keys():
                ntp += 1
                predicate_search = re.search(r"(\?\w+|a|(\w+):(\w+)|<\S+>)", predicate)
                predicate_full = predicate_search.group(1)
                if predicate_search.group(2) is not None and predicate_search.group(3) is not None:
                    predicate_full = f"{prefix_alias_to_full[predicate_search.group(2)]}{predicate_search.group(3)}"
                composition[f"tp{ntp}"] = [subject, predicate_full, object]
                queryBody += re.sub(re.escape(subline), f"GRAPH ?tp{ntp} {{ {subline} }}", line)
        else:
            queryBody += line
            if "WHERE" in line:
                wherePos = cur
    
    graph_proj = ' '.join([ f"?tp{i}" for i in np.arange(1, ntp+1) ])
    # Deferring the task of eliminate duplicate downstream if needed.
    with open(outfile, mode="w+") as out:
        query = queryHeader + queryBody
        query = re.sub(r"(SELECT|CONSTRUCT|DESCRIBE)(\s+DISTINCT)?\s+(.*)\s+WHERE", f"SELECT DISTINCT {graph_proj} WHERE", query)
        out.write(query)
        out.close()
    
    json.dump(composition, open(f"{outfile}.comp", mode="w"))

@cli.command()
@click.argument("value-selection", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("workload-value-selection", type=click.Path(exists=False, file_okay=True, dir_okay=False))
@click.argument("n-instances", type=click.INT)
def create_workload_value_selection(value_selection, workload_value_selection, n_instances):
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

    header = open(value_selection, "r").readline().strip().replace('"', '').split(",")
    value_selection_values = pd.read_csv(value_selection, parse_dates=[h for h in header if "date" in h], low_memory=False)

    numerical_cols = []
    for col in value_selection_values.columns:
        values = value_selection_values[col].dropna()
        if not values.empty:
            dtype = values.dtype
            if np.issubdtype(dtype, np.number) or np.issubdtype(dtype, np.datetime64):
                numerical_cols.append(col)

    numerical = value_selection_values[numerical_cols]

    workload = value_selection_values
    if not numerical.empty:
        query = " or ".join([
            f"( `{col}` >= {repr(numerical[col].quantile(0.25))} and `{col}` <= {repr(numerical[col].quantile(0.75))} )" 
            for col in numerical.columns
        ])

        print(query)
        workload = value_selection_values.query(query)
   
    workload.sample(n_instances).to_csv(workload_value_selection, index=False)
    

if __name__ == "__main__":
    cli()