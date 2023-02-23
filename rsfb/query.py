import json
from pathlib import Path
from time import time

from collections import Counter
import os
from pathlib import Path
from typing import Dict, Tuple
from tqdm import tqdm

import re
import numpy as np
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, CSV, DESCRIBE
from io import BytesIO, StringIO
from rdflib import Literal, URIRef, XSD
import click

import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

from utils import load_config, str2n3

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

def exec_query(configfile, query, batch_id, error_when_timeout=False):
    
    config = load_config(configfile)["generation"]
    endpoint = config["virtuoso"]["endpoints"][batch_id]
    sparql_endpoint = SPARQLWrapper(endpoint)
    if error_when_timeout: sparql_endpoint.addParameter("timeout", "300000") # in ms
        
    # if batch_id is not None:
    #     from_clause = []
    #     from_keyword = "FROM" if re.search(r"GRAPH\s+(\?\w+\s*)+\{", query) is None else "FROM NAMED"
                
    #     for schema_name, schema_props in config["schema"].items():
    #         if schema_props["is_source"]:
    #             n_items = schema_props["params"][f"{schema_name}_n"]
                
    #             _, edges = np.histogram(np.arange(n_items), config["n_batch"])
    #             edges = edges[1:].astype(int) + 1
    #             for id in range(edges[batch_id]):
    #                 provenance =  re.sub(re.escape(f"{{%{schema_name}_id}}"), f"{schema_name}{id}", schema_props["provenance"])
    #                 from_clause.append(f"{from_keyword} <{provenance}>")
    
    #     query = re.sub(r"(SELECT|CONSTRUCT|DESCRIBE)(\s+DISTINCT)?\s+(.*)\s+WHERE", rf"\1\2 \3 \n{' '.join(from_clause)}\nWHERE", query)
        
    sparql_endpoint.setMethod("POST")
    sparql_endpoint.setReturnFormat(CSV)        
    sparql_endpoint.setQuery(query)
    response = sparql_endpoint.query()
    result = response.convert()
    return response, result

def __parse_query(queryfile) -> Tuple[Dict[str, str], Dict[str, Dict]]:
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

    with open(queryfile, mode="r") as qfs:
        for line in qfs.readlines():
            toks = np.array(re.split(r"\s+", line))
            if "#" in toks:
                exclusive = False
                priority_level = 0

                if (comp := re.search(r"const([\*\!]*)\s+(\?\w+)\s+(\W+|\w+)\s+((\?\w+|\w+:\w+)(,\s*(\?\w+|\w+:\w+))*)", line)) is not None:
                    op = comp.group(3)
                    left = comp.group(2)

                    priority = comp.group(1)
                    if priority is not None:
                        priority_level = priority.count("*")
                        exclusive = ("!" in priority)

                    deps = re.split(r",\s*", comp.group(4))

                    right = deps[0]
                    if op == "$!":
                        right = deps.pop(0)
                    elif op == "!=":
                        right = left

                    parse_result[left] = {
                        "priority": priority_level,
                        "exclusive": exclusive,
                        "type": "comparison",
                        "src": right,
                        "op": op,
                        "op_kind": "binary",
                        "extras": deps
                    }

                elif (assertion := re.search(r"const([\*\!]*)\s+(not)\s+(\?\w+)", line)) is not None:
                    op = assertion.group(2)
                    left = assertion.group(3)

                    priority = assertion.group(1)
                    if priority is not None:
                        priority_level = priority.count("*")
                        exclusive = ("!" in priority)

                    parse_result[left] = {
                        "priority": priority_level,
                        "exclusive": exclusive,
                        "type": "assertion",
                        "op": op,
                        "op_kind": "unary"
                    }
                else:
                    const_search = re.search(r"const([\*\!]*)\s+(\?\w+)", line)
                    if const_search is not None:
                        left = const_search.group(2)

                        priority = const_search.group(1)
                        if priority is not None:
                            priority_level = priority.count("*")
                            exclusive = ("!" in priority)

                        parse_result[left] = {
                            "priority": priority_level,
                            "exclusive": exclusive,
                            "type": None
                        }
                continue

            elif "PREFIX" in toks:
                regex = r"PREFIX\s+([\w\-]+):\s*<(.*)>\s*"
                alias = re.search(regex, line).group(1)
                full_name = re.search(regex, line).group(2)
                prefix_full_to_alias[full_name] = alias

        parse_result = dict(sorted(parse_result.items(),
                            key=lambda item: item[1]["priority"], reverse=True))
        return prefix_full_to_alias, parse_result

def __get_uninjected_placeholders(queryfile, exclusive=False):
    _, parse_result = __parse_query(queryfile)       
    
    consts = set()
    for const, resource in parse_result.items():
        c = const if (resource["type"] is None or resource.get("src") is None ) else resource["src"]
        if c.startswith("?"):
            if exclusive and resource.get("exclusive"):
                consts.add(c)
            elif not exclusive:
                consts.add(c)
    return consts

@cli.command()
@click.argument("configfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("queryfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("outfile", type=click.Path(exists=False, file_okay=True, dir_okay=False))
@click.argument("batch-id", type=click.INT)
@click.option("--sample", type=click.INT)
@click.option("--ignore-errors", is_flag=True, default=False)
@click.option("--dropna", is_flag=True, default=False)
def execute_query(configfile, queryfile, outfile, batch_id, sample, ignore_errors, dropna):
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
    with open(queryfile, mode="r") as qf:
        query_text = qf.read()
                
        _, result = exec_query(configfile=configfile, query=query_text, batch_id=batch_id, error_when_timeout=True)  

        with BytesIO(result) as header_stream, BytesIO(result) as data_stream: 
            header = header_stream.readline().decode().strip().replace('"', '').split(",")
            csvOut = pd.read_csv(data_stream, parse_dates=[h for h in header if "date" in h])
            
            if csvOut.empty and not ignore_errors:
                logger.error(query_text)
                raise RuntimeError(f"{queryfile} returns no result...")
            
            if dropna:
                csvOut.dropna(inplace=True)

            if sample is not None:
                csvOut = csvOut.sample(sample)
            
            if ("DISTINCT" in query_text or "distinct" in query_text) and csvOut.duplicated().any():
                logger.error(query_text)
                logger.error(csvOut[csvOut.duplicated()])
                raise RuntimeError(f"{queryfile} has duplicates!")
            #     csvOut.drop_duplicates(inplace=True)
                
            csvOut.to_csv(outfile, index=False)

            return csvOut.shape[0]

@cli.command()
@click.argument("queryfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("outfile", type=click.Path(exists=False, file_okay=True, dir_okay=False))
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
    consts = __get_uninjected_placeholders(queryfile, exclusive=True)
    if len(consts) == 0:
        consts = __get_uninjected_placeholders(queryfile, exclusive=False)
    logger.debug(consts)

    with open(queryfile, "r") as qf:
        query = qf.read()
        query = re.sub(r"(SELECT|CONSTRUCT|DESCRIBE)(\s+DISTINCT)?\s+(.*)\s+WHERE", f"SELECT DISTINCT {' '.join(consts)} WHERE", query)
        #query = re.sub(r"((\?\w+|\w+:\w+|<\S+>)\s+(\?\w+|a|\w+:\w+|<\S+>)\s+(<\S+>))", r"##\1", query)
        query = re.sub(r"(#)*(LIMIT|FILTER\s+|OFFSET|ORDER)", r"##\2", query)

        with open(outfile, "w") as out:
            out.write(query)

        return query

@cli.command()
@click.argument("queryfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("cache-file", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("outputfile", type=click.Path(exists=False, file_okay=True, dir_okay=False))
def inject_from_cache(queryfile, cache_file, outputfile):
    
    with open(queryfile, "r") as qf:
        query = qf.read()
        injection_cache = json.load(open(cache_file, "r"))
        for variable, value in injection_cache.items():
            # Convert to n3 representation
            value = str2n3(value)
            prefix_full_to_alias = json.load(open(f"{Path(cache_file).parent}/prefix_cache.json", 'r'))
            for prefixSub, prefixName in prefix_full_to_alias.items():
                if prefixSub in value:
                    value = re.sub(rf"<{prefixSub}(\w+)>", rf"{prefixName}:\1", value)
            
            query = re.sub(rf"{re.escape('?'+variable)}(\W)", rf"{value}\1", query)
            
        with open(outputfile, "w") as f:
            f.write(query)
            f.close()

@cli.command()
@click.argument("queryfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("value-selection", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.option("--ignore-errors", is_flag=True, default=False)
@click.option("--instance-id", type=click.INT)
def inject_constant(queryfile, value_selection, ignore_errors, instance_id):
    """Inject constant in placeholders for a query template.
    
    - For every uninjected constant, ordered by priotity: 
        - If this is the first injection, or the operator is unary, inject with the `instance_id `-th row of `value_selection`
        - Else, each operator has its own rule to inject missing constants:
            - Comparison op, e.g, ?a > ?b: first try to select random value constrained by the operator
            - Dependant-difference ($!) or independant-different (!=): choose randomly a value that is different to injected constant 
            - Containment ("in"): choose randomly one out of 10 least common words

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
    with open(queryfile, "r") as qf:
        query = qf.read()

        prefix_cache_filename = f"{qroot}/prefix_cache.json"
        injection_cache_filename = f"{qroot}/injection_cache.json"
        injection_cache: dict = json.load(open(injection_cache_filename, "r")) if os.path.exists(injection_cache_filename) else dict()

        flr = open(value_selection, "r")
        header = flr.readline().strip().replace('"', '').split(",")
        flr.close()
        
        value_selection_values = pd.read_csv(value_selection, parse_dates=[h for h in header if "date" in h])
        placeholder_chosen_values = value_selection_values
        placeholder_chosen_values_idx = instance_id
        
        prefix_full_to_alias, parse_result = __parse_query(queryfile)
        prefix_alias_to_full = {v: k for k, v in prefix_full_to_alias.items()}
        
        for const, resource in parse_result.items():                
            # Replace all tokens in subDict
            isOpUnary = (resource["type"] is None or resource.get("src") is None)
            left = right = const[1:] if isOpUnary else resource.get("src")[1:]        
            if not isOpUnary:
                left = const[1:]
                right = resource.get("src")[1:]
            
            if right not in value_selection_values.columns:
                msg = f"Column {right} is not in {value_selection}..."
                if ignore_errors: 
                    logger.debug(msg)
                    continue
                else: raise ValueError(msg)
            
            # When not using workload value_selection_query, i.e, filling partially injected queries
            placeholder_chosen_values = value_selection_values.query(f"`{right}` == `{right}`")
            if len(placeholder_chosen_values) == 0:
                if ignore_errors: continue
                else: raise RuntimeError(f"There is no value for `{right}` in {value_selection}")

            if instance_id is None and (
                placeholder_chosen_values_idx is None or 
                placeholder_chosen_values_idx not in placeholder_chosen_values.index.values
            ):
                placeholder_chosen_values_idx = placeholder_chosen_values.sample(1).index.item()
            # Replace for FILTER clauses
            repl_val = placeholder_chosen_values.loc[placeholder_chosen_values_idx, right]
            if resource["type"] is not None:
                dtype = placeholder_chosen_values[right].dtype
                epsilon = 0
                # Use sigmoid function to change value slighly
                if np.issubdtype(dtype, np.number):
                    if np.issubdtype(dtype, np.number):
                        epsilon = 1
                    else:
                        # epsilon = # Do something here
                        raise ValueError(f"{right} is of type {dtype}, which is not yet supported")
                elif np.issubdtype(dtype, np.datetime64):
                    epsilon = pd.Timedelta(1, unit="day")

                # Implement more operators here
                op = resource["op"]
                if (op_search := re.match(r"(\<|\>)=?", op)) is not None:
                    # Option 1: Chose randomly another value filtered by chosen value
                    try: repl_val = placeholder_chosen_values.query(f"`{right}` {op} {repr(repl_val)}").sample(1)[right]
                    except: pass
                    
                    # Option 2: substract/add a small amount to a chosen value so the query yield result
                    sub_op = op_search.group(1)
                    if sub_op == ">": repl_val += epsilon
                    elif sub_op == "<": repl_val -= epsilon

                # elif op == "$":
                #     repl_val = placeholder_chosen_values.loc[placeholder_chosen_values_idx, right]
                    
                elif op == "!=" and instance_id is None:
                    constraints = []
                    for extra in resource.get("extras"):
                        if str(extra).startswith("?"):
                            r_extra = extra[1:]
                            constraint = f"`{left}` != {repr(injection_cache[r_extra])}"
                            constraints.append(constraint)
                        else:
                            if (prefix_search := re.search(r"([\w\-]+):(\w+)", extra)) is not None:
                                prefix_alias = prefix_search.group(1)
                                suffix = prefix_search.group(2)
                                prefix_full_name = prefix_alias_to_full[prefix_alias]
                                    
                                formatted_extra =  f"{prefix_full_name}{suffix}"
                                constraint = f"`{left}` != {repr(formatted_extra)}"
                                constraints.append(constraint)

                    if len(constraints) > 0:
                        constraint_query = " and ".join(constraints)
                        logger.debug(constraint_query)
                        repl_val = placeholder_chosen_values.query(constraint_query)[right].sample(1)
                        
                elif op == "$!" and instance_id is None:
                    targetCol = left 
                    if bool(injection_cache) and left not in injection_cache and right in injection_cache:
                        targetCol = right
                        
                    if bool(injection_cache): 
                        constraints = []
                        if targetCol in injection_cache:
                            constraints.append(f"`{targetCol}` != {repr(injection_cache[targetCol])}")
                            
                        for extra in resource.get("extras"):
                            if str(extra).startswith("?"):
                                r_extra = extra[1:]
                                constraint = f"`{left}` != {repr(injection_cache[r_extra])}"
                                constraints.append(constraint)
                            else:
                                if (prefix_search := re.search(r"([\w\-]+):(\w+)", extra)) is not None:
                                    prefix_alias = prefix_search.group(1)
                                    suffix = prefix_search.group(2)
                                    prefix_full_name = prefix_alias_to_full[prefix_alias]
                                        
                                    formatted_extra =  f"{prefix_full_name}{suffix}"
                                    constraint = f"`{left}` != {repr(formatted_extra)}"
                                    constraints.append(constraint)
                        if len(constraints) > 0:
                            constraint_query = " and ".join(constraints)
                            logger.debug(constraint_query)
                            repl_val = placeholder_chosen_values.query(constraint_query)[right].sample(1)
                                                            
                # Special treatment for REGEX
                #   Extract randomly 1 from 10 most common words in the result list
                elif op == "in":
                    langs = placeholder_chosen_values[right].apply(lambda x: lang_detect(x))
                    for lang in set(langs):
                        try: stopwords.extend(nltk_stopwords.words(lang))
                        except: continue
                        
                    words = [
                        token for token in
                        tokenizer.tokenize(str(placeholder_chosen_values[right].str.cat(sep=" ")).lower())
                        if token not in stopwords
                    ]
                            
                    # Option 1: Choose randomly amongst 10 most common words
                    bow = Counter(words)
                    bow = Counter({ k: v for k, v in bow.items() if k not in stopwords})
                    repl_val = pd.Series(bow).nsmallest(10).sample(1).index.item()

                    # Option 2: Choose randomly amongst words
                    # repl_val = np.random.choice(words)
                    
                # # Special treatment for exclusion
                # # Query with every other placeholder set
                # elif op == "not":
                #     # From q03: user asks for products having several features but not having a specific other feature. 
                #     exclusion_query = " and ".join([f"`{right}` != {repr(repl_val)}"] + [ f"`{k}` != {repr(v)}" for k, v in injection_cache.items() ])
                #     repl_val = placeholder_chosen_values.query(exclusion_query)[right].sample(1)
                    
                logger.debug(f"op: {op}; isOpUnary: {isOpUnary}; left: {left}; right: {right}; val: {repl_val}")
                
            try: repl_val = repl_val.item()
            except: pass
            
            # Should never happens, but...
            if repl_val is None:
                raise ValueError(f"There is no value for left = `{left}`, right = `{right}`")

            # Save injection
            if np.issubdtype(placeholder_chosen_values[right].dtype, np.datetime64):
                injection_cache[left] = str(repl_val)
            else:
                injection_cache[left] = repl_val

            # Convert to n3 representation
            repl_val = str2n3(repl_val)
                
            # Shorten string representations with detected and common prefixes
            for prefix_full_name, prefix_alias in prefix_full_to_alias.items():
                if prefix_full_name in repl_val:
                    repl_val = re.sub(rf"<{prefix_full_name}(\w+)>", rf"{prefix_alias}:\1", repl_val)
                    missing_prefix = f"PREFIX {prefix_alias}: <{prefix_full_name}>"
                    if re.search(re.escape(missing_prefix), query) is None:
                        query = f"PREFIX {prefix_alias}: <{prefix_full_name}>" + "\n" + query
            
            query = re.sub(rf"{re.escape(const)}(\W)", rf"{repl_val}\1", query)
        
        with open(injection_cache_filename, "w") as inject_cache_fs, open(prefix_cache_filename, "w") as prefix_cache_fs:
            json.dump(injection_cache, inject_cache_fs)
            json.dump(prefix_full_to_alias, prefix_cache_fs)
        return query, injection_cache

@cli.command()
@click.argument("configfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("queryfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("value-selection", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("outfile", type=click.Path(exists=False, file_okay=True, dir_okay=False))
@click.argument("instance-id", type=click.INT)
@click.pass_context
def instanciate_workload(ctx: click.Context, configfile, queryfile, value_selection, outfile, instance_id):
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

    with open(queryfile, "r") as query_fs:
        query = query_fs.read()
        initial_queryfile = queryfile
        consts = __get_uninjected_placeholders(initial_queryfile)
        select = re.search(r"(SELECT|CONSTRUCT|DESCRIBE)(\s+DISTINCT)?\s+(.*)\s+WHERE", query).group(3)     

        qname = Path(outfile).stem
        qroot = Path(outfile).parent
        next_queryfile = f"{qroot}/{qname}.sparql"

        solution_option = 1

        itr = 0
        while len(consts) > 0:
            logger.debug(f"Iteration {itr}...")
            
            # First iteration, inject value using initial value_selection of the workload
            if itr == 0:
                ctx.invoke(build_value_selection_query, queryfile=initial_queryfile, outfile=next_queryfile)
                #os.system(f"cp {initial_queryfile} {next_queryfile}")
                next_value_selection = value_selection
                query, injection_cache = ctx.invoke(inject_constant, queryfile=next_queryfile, value_selection=next_value_selection, ignore_errors=True, instance_id=instance_id)
            
            # Execute the partially injected query to find the rest of constants
            else:                
                # Option 1: Exclude the partialy injected query to refill
                if solution_option == 1:
                    try:
                        logger.debug("Option 1: Execute the partialy injected query to refill")
                        next_value_selection = f"{qroot}/{qname}.csv"
                        ctx.invoke(execute_query, configfile=configfile, queryfile=next_queryfile, outfile=next_value_selection, batch_id=0)
                        query, injection_cache = ctx.invoke(inject_constant, queryfile=next_queryfile, value_selection=next_value_selection, ignore_errors=False)
                    except RuntimeError:
                        solution_option = 2  
                        continue
                
                # Option 2: extract the needed value for placeholders from value_selection.csv
                elif solution_option == 2:
                    try:
                        logger.debug("Option 2: extract the needed value for placeholders from value_selection.csv")
                        next_value_selection = f"{Path(value_selection).parent}/value_selection.csv"
                        query, injection_cache = ctx.invoke(inject_constant, queryfile=next_queryfile, value_selection=next_value_selection, ignore_errors=False)
                    except RuntimeError:
                        solution_option = 3
                        continue

                # Option 3: Relax the query knowing there is NO solution mapping for given combination of placeholders
                elif solution_option == 3:
                    #try:
                        logger.debug("Option 3: Relax the query knowing there is NO solution mapping for given combination of placeholders")
                        logger.debug("Relaxing query...")
                        relaxed_query = re.sub(r"(#)*((\?\w+|\w+:\w+|<\S+>)\s+(\?\w+|a|\w+:\w+|<\S+>)\s+(\w+:\w+|<\S+>)\s*\.)", r"##\2", query)
                        with open(next_queryfile, "w") as f:
                            f.write(relaxed_query)

                        next_value_selection = f"{qroot}/{qname}.csv"
                        ctx.invoke(execute_query, configfile=configfile, queryfile=next_queryfile, outfile=next_value_selection, batch_id=0, dropna=True)
                        relaxed_query, injection_cache = ctx.invoke(inject_constant, queryfile=next_queryfile, value_selection=next_value_selection, ignore_errors=False)
                        logger.debug("Restoring query...")
                        query = re.sub(r"(##)((\?\w+|\w+:\w+|<\S+>)\s+(\?\w+|a|\w+:\w+|<\S+>)\s+(\w+:\w+|<\S+>)\s*\.)", r"\2", relaxed_query)
                    #except:
                    #   raise RuntimeError("Cannot instancitate this workload. Either (1) Delete workload_value_selection.csv and retry. (2) Rewrite bounded tps into hard FILTER")
            
            # Remove from set injected consts
            with open(next_queryfile, "w") as f:
                query = re.sub(r"(SELECT|CONSTRUCT|DESCRIBE)(\s+DISTINCT)?\s+(.*)\s+WHERE", rf"\1\2 {' '.join([c for c in consts if c[1:] not in injection_cache.keys()])}\nWHERE", query)
                query = re.sub(r"(regex|REGEX)\s*\(\s*(\?\w+)\s*,", r"\1(lcase(\2),", query)
                query = re.sub(r"(#)*(FILTER\s*\(\!bound)", r"\2", query)
                logger.debug(next_queryfile)
                logger.debug(query)
                f.write(query)
                #raise ValueError("LOL")
            
            initial_queryfile = next_queryfile
            consts = __get_uninjected_placeholders(initial_queryfile)
            itr+=1
        
        query = re.sub(r"(SELECT|CONSTRUCT|DESCRIBE)(\s+DISTINCT)?\s+(.*)\s+WHERE", rf"\1\2 {select} WHERE", query)
        query = re.sub(r"(#){2}(LIMIT|FILTER\s+)", r"\2", query)
        #query = re.sub(r"(#){2}(FILTER\s+)", r"\2", query)
        with open(next_queryfile, "w") as f:
            f.write(query)

@cli.command
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
        
        reversed_def_comp = {" ".join(v): k for k, v in def_comp_dict.items()}
        result = dict()
        
        for bgp in provenance_df.columns:
            tps = opt_comp_dict[bgp] 
            sources = provenance_df[bgp]
            for tp in tps:
                tpid = reversed_def_comp[tp]
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

    with open(queryfile, "r") as qfs:
        query = qfs.read()
        prefix_full_to_alias, _ = __parse_query(queryfile)
        ss_cache = []

        # Wrap each tp with GRAPH clause
        ntp = 0
        wherePos = np.inf
        composition = dict()
        prefix_alias_to_full = {v: k for k, v in prefix_full_to_alias.items()}

        queryHeader = ""
        queryBody = ""
        with StringIO(query) as query_ss:
            for cur, line in enumerate(query_ss.readlines()):
                if "WHERE" not in line and cur < wherePos: 
                    if "SELECT" not in line:
                        queryHeader += line
                    continue
                
                #logger.debug("read:", line.strip())
                subline_search = re.search(r"^(\s|(\w+\s*\{)|\{)*((\?\w+|\w+:\w+|<\S+>)\s+(\?\w+|a|\w+:\w+|<\S+>)\s+(\?\w+|\w+:\w+|<\S+>))(\s|\}|\.)*\s*$", line)
                if subline_search is not None:
                    subline = subline_search.group(3)
                    subject = subline_search.group(4)
                    predicate = subline_search.group(5)
                    object = subline_search.group(6)
                    # logger.debug("parsed: ", subject, predicate, object)
                        #ss_cache.append(subline)
                    ntp += 1
                    # predicate_search = re.search(r"(\?\w+|a|(\w+):(\w+)|<\S+>)", predicate)
                    # predicate_full = predicate_search.group(1)
                    # prefix, suffix = predicate_search.group(2),  predicate_search.group(3)
                    # if prefix is not None and suffix is not None:
                    #     predicate_full = f"{prefix_alias_to_full[prefix]}{suffix}"
                    composition[f"tp{ntp}"] = [subject, predicate, object]
                    queryBody += re.sub(re.escape(subline), f"GRAPH ?tp{ntp} {{ {subline} }}", line)
                else:
                    queryBody += line
                    if "WHERE" in line:
                        wherePos = cur
        
        # Wrap tpi with STR(...) as ... because there is a bug where Virtuoso v7.5.2 yield duplicata with empty OPTIONAL columns
        # Reproduce bug:
        #   - Take two query instances of the same template: A: (q02 instance 8) and B: (q02 instance 5)
        #   - Execute A, wait for completion, then execute B, watt for completion
        #   - There should be duplicated results for query B 
        graph_proj = ' '.join([ f"(STR(?tp{i}) AS ?tp{i})" for i in np.arange(1, ntp+1) ])
        
        with open(outfile, mode="w") as out:
            query = queryHeader + queryBody
            query = re.sub(r"(SELECT|CONSTRUCT|DESCRIBE)(\s+DISTINCT)?\s+(.*)\s+WHERE", f"SELECT DISTINCT {graph_proj} WHERE", query)
            # Disable filters
            query = re.sub(r"(#)*(LIMIT|OFFSET|ORDER)", r"##\2", query)
            out.write(query)
        
        with open(f"{outfile}.comp", mode="w") as composition_fs:
            json.dump(composition, composition_fs)

@cli.command()
@click.argument("queryfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("value-selection", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("workload-value-selection", type=click.Path(exists=False, file_okay=True, dir_okay=False))
@click.argument("n-instances", type=click.INT)
def create_workload_value_selection(queryfile, value_selection, workload_value_selection, n_instances):
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

    header_fs = open(value_selection, "r")
    header = header_fs.readline().strip().replace('"', '').split(",")
    header_fs.close()
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
            f"( `{col}` >= {repr(numerical[col].quantile(0.10))} and `{col}` <= {repr(numerical[col].quantile(0.90))} )"
            #f"( `{col}` > {repr(numerical[col].min())} and `{col}` < {repr(numerical[col].max())} )" 
            for col in numerical.columns
        ])
        
        workload = value_selection_values.query(query)
        
    _, parse_result = __parse_query(queryfile)
    filter_query_clauses = []

    for const, resource in parse_result.items():                
        # Replace all tokens in subDict
        isOpUnary = (resource["type"] is None or resource.get("src") is None)
        left = right = const[1:] if isOpUnary else resource.get("src")[1:]        
        if not isOpUnary:
            left = const[1:]
            right = resource.get("src")[1:]
            
        # Replace for FILTER clauses
        if resource["type"] is not None:
            
            if left not in workload.columns:
                continue
            
            # Implement more operators here
            op = resource["op"]
            if ">" in op:
                filter_query_clauses.append(f"`{left}` {op} `{right}`")
            elif "<" in op:
                filter_query_clauses.append(f"`{left}` {op} `{right}`")
            elif op in ["!=", "$!"]:
                for extra in resource.get("extras"):
                    r_extra = extra[1:]
                    filter_query_clauses.append(f"`{left}` != `{r_extra}`")               
    
    filter_query = " and ".join(filter_query_clauses)
    logger.debug(filter_query)
    result = workload.query(filter_query) if len(filter_query) > 0 else workload
    result.sample(n_instances).to_csv(workload_value_selection, index=False)
    

if __name__ == "__main__":
    cli()