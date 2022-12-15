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

def exec_query(query, endpoint, error_when_timeout=False):
    sparql_endpoint = SPARQLWrapper(endpoint)
    if error_when_timeout: sparql_endpoint.addParameter("timeout", "300000") # in ms
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
                comp = re.search(r"const\s+(\?\w+)\s+(\W|\w+)\s+(\?\w+)", line)
                if comp is not None:
                    op = comp.group(2)
                    const = comp.group(1)
                    constSrc = comp.group(3)
                    parse_result[const] = {"src": constSrc, "op": op}
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

@cli.command()
@click.argument("queryfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("outfile", type=click.Path(exists=False, file_okay=True, dir_okay=False))
@click.option("--sample", type=click.INT)
@click.option("--endpoint", type=str, default="http://localhost:8890/sparql", help="URL to a SPARQL endpoint")
def execute_query(queryfile, outfile, sample, endpoint):
    query_text = open(queryfile, mode="r").read()
    _, result = exec_query(query_text, endpoint, error_when_timeout=True)  

    header = BytesIO(result).readline().decode().strip().replace('"', '').split(",")
    csvOut = pd.read_csv(BytesIO(result), parse_dates=[h for h in header if "date" in h])

    if csvOut.empty:
        print(query_text)
        raise RuntimeError(f"{queryfile} returns no result...")

    if sample is None:
        csvOut.to_csv(outfile, index=False)
    else:
        csvOut.sample(sample).to_csv(outfile, index=False)

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

    _, parse_result = __parse_query(queryfile)
    consts = [ const if constSrc is None else constSrc["src"] for const, constSrc in parse_result.items() ]

    query = open(queryfile).read()
    query = re.sub(r"(SELECT|CONSTRUCT|DESCRIBE)(\s+DISTINCT)?\s+(.*)\s+WHERE", f"SELECT DISTINCT {' '.join(consts)} WHERE", query)
    #query = re.sub(r"((\?\w+|\w+:\w+|<\S+>)\s+(\?\w+|a|\w+:\w+|<\S+>)\s+(<\S+>))", r"##\1", query)
    query = re.sub(r"(#)*(LIMIT|FILTER|OFFSET|ORDER)", r"##\2", query)

    with open(outfile, "w+") as out:
        out.write(query)
        out.close()

    return query

@cli.command()
@click.argument("queryfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("value-selection", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.option("--instance-id", type=click.INT)
def inject_constant(queryfile, value_selection, instance_id):
    """ This fuction extract the {instance_id}-th row in {value_selection},
        replace the placeholders with their corresponding columns.

    TODO:
        [x] Force injecting distinct variable

    Args:
        queryfile (_type_): _description_
        value_selection (_type_): _description_
        instance_id (int, optional): _description_. Defaults to 0.

    Returns:
        _type_: _description_
    """

    qroot = Path(queryfile).parent
    query = open(queryfile, "r").read()

    cache_filename = f"{qroot}/injection_cache.json"
    injection_cache: dict = json.load(open(cache_filename, "r")) if os.path.exists(cache_filename) else dict()

    header = open(value_selection, "r").readline().strip().replace('"', '').split(",")
    value_selection_values = pd.read_csv(value_selection, parse_dates=[h for h in header if "date" in h])
    
    prefix_full_to_alias, parse_result = __parse_query(queryfile)

    for const, constSrc in parse_result.items():                
        # Replace all tokens in subDict
        subSrc = const[1:] if constSrc is None else constSrc["src"][1:]

        # When not using workload value_selection_query, i.e, filling partially injected queries
        if "workload_" not in value_selection:
            # dedup_query = " and ".join([ 
            #     f"`{subSrc}` != {repr(value)}" \
            #         for const, value in injection_cache.items() \
            #         if const != subSrc #and value_selection_values[const].dtype == value_selection_values[subSrc].dtype 
            # ] + [ f"`{subSrc}`.notna()" ])
            # value_selection_values = value_selection_values.query(dedup_query).sample(1)
            value_selection_values = value_selection_values.sample(1)

        repl_val = value_selection_values[subSrc].item() if instance_id is None else value_selection_values.loc[instance_id, subSrc]
        if pd.isnull(repl_val): continue

        # Replace for FILTER clauses
        if constSrc is not None:

            if ">" in constSrc["op"]:
                repl_val = value_selection_values[subSrc].dropna().max()
            elif "<" in constSrc["op"]:
                repl_val = value_selection_values[subSrc].dropna().min()
            
            # Special treatment for REGEX
            #   Extract randomly 1 from 10 most common words in the result list
            elif constSrc["op"] == "in":
                langs = value_selection_values[subSrc].dropna().apply(lambda x: lang_detect(x))
                for lang in set(langs):
                    try: stopwords.extend(nltk_stopwords.words(lang))
                    except: continue
                    
                bow = Counter(tokenizer.tokenize(str(value_selection_values[subSrc].str.cat(sep=" ")).lower()))
                bow = Counter({ k: v for k, v in bow.items() if k not in stopwords})
                print(bow.most_common(10))
                repl_val = np.random.choice(list(map(lambda x: x[0], bow.most_common(10))))
            
        # Convert Pandas numpy object to Python object
        try: repl_val = repl_val.item()
        except: pass

        # Stringify time object and cache
        injection_cache[subSrc] = repl_val.strftime("%Y-%m-%d") if isinstance(repl_val, pd._libs.tslibs.timestamps.Timestamp) else repl_val 

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
        
        query = re.sub(rf"{re.escape(const)}(\W)", rf"{repl_val}\1", query)
    
    json.dump(injection_cache, open(cache_filename, "w"))
    return query

@cli.command()
@click.argument("queryfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("value-selection", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("outfile", type=click.Path(exists=False, file_okay=True, dir_okay=False))
@click.argument("instance-id", type=click.INT)
@click.option("--endpoint", type=str, default="http://localhost:8890/sparql", help="URL to a SPARQL endpoint")
@click.pass_context
def instanciate_workload(ctx: click.Context, queryfile, value_selection, outfile, instance_id, endpoint):
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

    def get_uninjected_placeholder(queryfile):
        _, parse_result = __parse_query(queryfile)
        consts = [ const if constSrc is None else constSrc["src"] for const, constSrc in parse_result.items() ]
        return consts

    query = open(queryfile, "r").read()
    initial_queryfile = queryfile
    consts = get_uninjected_placeholder(initial_queryfile)
    select = re.search(r"(SELECT|CONSTRUCT|DESCRIBE)(\s+DISTINCT)?\s+(.*)\s+WHERE", query).group(3)     

    qname = Path(outfile).stem
    qroot = Path(outfile).parent

    itr = 0
    while len(consts) > 0:
        next_queryfile = f"{qroot}/{qname}.sparql"
        ctx.invoke(build_value_selection_query, queryfile=initial_queryfile, outfile=next_queryfile)

        # First iteration, inject value using initial value_selection of the workload
        if itr == 0:
            next_value_selection = value_selection
            query = ctx.invoke(inject_constant, queryfile=next_queryfile, value_selection=next_value_selection, instance_id=instance_id)
        
        # Execute the partially injected query to find the rest of constants
        else:
            # next_value_selection = f"{qroot}/{qname}.csv"
            # try: ctx.invoke(execute_query, queryfile=next_queryfile, outfile=next_value_selection, endpoint=endpoint)
            # except RuntimeError:
            #     print("Relaxing query...")
            #     query = re.sub(r"(#)*((\?\w+|\w+:\w+|<\S+>)\s+(\?\w+|a|\w+:\w+|<\S+>)\s+(<\S+>)) ", r"##\2", query)
            #     print(query)
            #     with open(next_queryfile, "w+") as f:
            #         f.write(query)
            #         f.close()
            #     ctx.invoke(execute_query, queryfile=next_queryfile, outfile=next_value_selection, endpoint=endpoint)
            #     print(f"Relaxed query yield results. See {next_value_selection}")

            next_value_selection = f"{Path(value_selection).parent}/value_selection.csv"
            query = ctx.invoke(inject_constant, queryfile=next_queryfile, value_selection=next_value_selection)

        with open(next_queryfile, "w+") as f:
            f.write(query)
            f.close()

        initial_queryfile = next_queryfile
        consts = get_uninjected_placeholder(initial_queryfile)
        itr+=1
    
    query = re.sub(r"(SELECT|CONSTRUCT|DESCRIBE)(\s+DISTINCT)?\s+(.*)\s+WHERE", rf"\1\2 {select} WHERE", query)
    query = re.sub(r"(regex|REGEX)\s*\(\s*(\?\w+)\s*,", r"\1(lcase(\2),", query)
    #query = re.sub(r"(#){2}(LIMIT|FILTER|OFFSET|ORDER|(((\?\w+|\w+:\w+|<\S+>)\s+(\?\w+|a|\w+:\w+|<\S+>)\s+(<\S+>))))", r"\2", query)
    query = re.sub(r"(#){2}(LIMIT|FILTER|OFFSET|ORDER)", r"\2", query)

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
        #query = re.sub(r"((\?\w+|\w+:\w+|<\S+>)\s+(\?\w+|a|\w+:\w+|<\S+>)\s+(<\S+>))", r"##\1", query)
        #query = re.sub(r"(#)*(LIMIT|FILTER|OFFSET|ORDER)", r"##\2", query)
        out.write(query)
        out.close()
    
    json.dump(composition, open(f"{outfile}.comp", mode="w"))


if __name__ == "__main__":
    cli()