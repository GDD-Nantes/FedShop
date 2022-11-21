from pathlib import Path
from time import time

from collections import Counter
import os
from pathlib import Path
from rdflib import ConjunctiveGraph
from tqdm import tqdm

import re
import numpy as np
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, CSV, DESCRIBE
from io import BytesIO, StringIO
from rdflib.term import Literal, URIRef, BNode
import click

import nltk
nltk.download('stopwords', quiet=True)
nltk.download('punkt', quiet=True)

from nltk.corpus import stopwords as nltk_stopwords
from nltk.tokenize import word_tokenize, RegexpTokenizer
tokenizer = RegexpTokenizer(r"\w+")

from ftlangdetect import detect
from iso639 import Lang

stopwords = []
for lang in ["english"]:
    stopwords.extend(nltk_stopwords.words(lang))

def preprocess(query, pool):
    result = query
    result = re.sub(r"SELECT(\s+DISTINCT)?\s+((\?\w+)\s+|\*)*", r"SELECT\1 * ", result)
    result = re.sub(r"DESCRIBE\s+((\?\w+)\s)*", r"SELECT * ", result)
    result = re.sub("ORDER", "#ORDER", result)
    result = re.sub(r"LIMIT (\d+)", "LIMIT 100", result)
    result = re.sub(r"FILTER", "#FILTER", result)
    #result = re.sub(r"(OPTIONAL|UNION|FILTER) (\{|\()", r"#\1 \2", result)
    #result = re.sub(r"(\t+)\}(\s+)", r"\1#}\2", result)

    if re.search(r"LIMIT", result) is None:
        result += f"\nLIMIT {pool}"
    else:
        result = re.sub(r"LIMIT(\s+\d+)", f"LIMIT {pool}", result)
    #print(result)
    return result

def lang_detect(txt):
    lines = str(txt).splitlines()
    result = Counter(map(lambda x: Lang(detect(text=x, low_memory=False)["lang"]).name.lower(), lines)).most_common(1)[0]
    #print(result)
    return result

def exec_query(query, endpoint):
    sparql_endpoint = SPARQLWrapper(endpoint)
    sparql_endpoint.setReturnFormat(CSV)        
    sparql_endpoint.setQuery(query)
    result = sparql_endpoint.queryAndConvert()
    return result

@click.group
def cli():
    pass

@cli.command()
@click.argument("queryfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("distribfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.option("--output", type=click.Path(dir_okay=True, file_okay=False), default=None, help="The folder in which the query result will be stored.")
@click.option("--endpoint", type=str, default="http://localhost:8890/sparql/", help="URL to a SPARQL endpoint")
@click.option("--pool", type=int, default=1000, help="Seed for random function")
@click.option("--variation", type=int, default=1, help="Number of variation for each query")
def transform_query(queryfile, distribfile, output, endpoint, pool, variation):

    # Read distribution information
    distrib = pd.read_csv(distribfile)
    distrib.filter(regex="candidate*")
    infos = dict()
    infos["const"] = [ col for col in distrib.columns if "nb" not in col and "candidate" not in col ]
    infos["candidate"] = []
    infos["nb"] = []
    for col in distrib.columns:
        if "candidate" in col:
            distrib[col] = distrib[col].apply(lambda x: x.split("|"))
            infos["candidate"].append(col)

        elif "nb" in col:
            infos["nb"].append(col)
        
    # If there are more than one source to choose from, choose randomly one
    nbInfo = np.random.choice(infos["nb"])
    #nbCandidate = np.random.choice(infos["candidate"])

    # Cut into groups in term of availability across source
    distrib["variation"] = pd.cut(distrib[nbInfo], bins=3, labels=range(variation))
    print(distrib["variation"].value_counts())

    subDict = dict()
    distribDict = dict()
    prefixDict = dict()
    prefixDict["http://www.w3.org/2001/XMLSchema#"] = "xsd"

    query_name = Path(queryfile).resolve().stem

    for var in range(variation):
        repl_cache = dict()

        #===============================
        # Parse input file
        #===============================
        print(f"Parsing {queryfile} ...")
        query = open(queryfile, mode="r").read()
        for line in open(queryfile, mode="r").readlines():
            toks = np.array(re.split(r"\s+", line))
            if "#" in toks:
                if "const" in toks:
                    comp = re.search(r"(\?\w+)\s+(\W|\w+)\s+(\?\w+)", line)
                    if comp is not None:
                        op = comp.group(2)
                        const = comp.group(1)
                        constSrc = comp.group(3)
                        subDict[const] = {"src": constSrc, "op": op}
                    else:
                        const = toks[np.argwhere(toks == "const") + 1].item()
                        subDict[const] = None
                    continue
                elif "distrib" in toks:
                    comp = re.search(r"(\?\w+)\s+(\W|\w+)\s+(\?\w+)", line)
                    if comp is not None:
                        op = comp.group(2)
                        const = comp.group(1)
                        constSrc = comp.group(3)
                        distribDict[const] = {"src": constSrc, "op": op}
                    continue

            elif "PREFIX" in toks:
                regex = r"PREFIX\s+([\w\-]+):\s*<(.*)>\s*"
                prefixName = re.search(regex, line).group(1)
                prefixSub = re.search(regex, line).group(2)
                prefixDict[prefixSub] = prefixName
        
        # Step 1: Inject constant using distribution infos
        print(f"Inject constant using distribution infos...")
        distrib_sample = distrib[distrib["variation"] == var].sample(1)
        distrib_cache = dict()
        for const in subDict.keys():
            constHaveRef = const in distribDict.keys()
            subSrc = distribDict[const]["src"][1:] if constHaveRef else const[1:]
            if subSrc in infos["const"]:

                if constHaveRef: # Resample if already have ref
                    search_query = ["variation == @var"] + [ f"{k} == '{v}'" for k, v in distrib_cache.items() ]
                    search_query = " and ".join(search_query)
                    print(search_query)
                    distrib_sample = distrib.query(search_query).sample(1)
                repl_val = distrib_sample[subSrc].item()
                distrib_cache[subSrc] = repl_val
                repl_val = URIRef(repl_val).n3()
                repl_cache[const] = repl_val
                query = re.sub(rf"{re.escape(const)}(\W)", rf"{repl_val}\1", query)

        # Step 2: Inject const using random sampling for the rest
        print(f"Inject const using random sampling for the rest")
        query_intermediate = preprocess(query, pool)

        query_results = []
        
        # Step 3: Replace const in subqueries and obtain result:
        subQueries = [ os.path.join(Path(queryfile).parent, fn) for fn in os.listdir(Path(queryfile).parent) if fn.startswith(f"{query_name}_") ]
        print(subQueries)
        if len(subQueries) == 0:
            query_results.append(pd.read_csv(BytesIO(exec_query(query_intermediate, endpoint))))

        for subQuery in tqdm(subQueries):
            content = open(subQuery, "r").read()
            for const, repl_val in repl_cache.items():
                content = re.sub(rf"{re.escape(const)}(\W)", rf"{repl_val}\1", content)
            print(content)
            query_results.append(pd.read_csv(BytesIO(exec_query(content, endpoint))))

        # Step 4: Replace const in the rest of the queries
        for const, constSrc in subDict.items():
            subSrc = const[1:]

            if const in repl_cache.keys(): continue
            
            def find(subSrc, query_results):
                
                # Find amongst the subqueries if the required column is presetn
                query_result = None
                for r in query_results:
                    print(f"Finding {subSrc} amongst {r.columns}")
                    if subSrc in r.columns:
                        query_result = r
                        break
                return query_result
                
            # Replace all tokens in subDict
            query_result = find(subSrc, query_results)
            repl_val = None
            if constSrc is not None:
                query_result = find(subSrc, query_results)
                subSrc = constSrc["src"][1:]
                if ">" in constSrc["op"]:
                    repl_val = query_result[subSrc].dropna().max()
                elif "<" in constSrc["op"]:
                    repl_val = query_result[subSrc].dropna().min()
                elif constSrc["op"] == "in":
                    query_result["lang"] = query_result[subSrc].apply(lambda x: lang_detect(x))
                    for lang in query_result["lang"].unique():
                        try: stopwords.extend(nltk_stopwords.words(lang))
                        except: continue
                    
                    bow = Counter(tokenizer.tokenize(str(query_result[subSrc].str.cat(sep=" ")).lower()))
                    bow = Counter({ k: v for k, v in bow.items() if k not in stopwords})
                    print(bow.most_common(10))
                    repl_val = np.random.choice(list(map(lambda x: x[0], bow.most_common(10)))).item()

                else:
                    query_result = find(subSrc, query_results)
                    repl_val = query_result[subSrc].dropna().value_counts().idxmax()
            else: 
                repl_val = query_result[subSrc].dropna().value_counts().idxmax()
            
            if str(repl_val).startswith("http") or str(repl_val).startswith("nodeID"): 
                repl_val = URIRef(repl_val).n3()
            else: 
                repl_val = Literal(repl_val).n3()

            # Shorten string representations with detected and common prefixes
            for prefixSub, prefixName in prefixDict.items():
                if prefixSub in repl_val:
                    repl_val = re.sub(rf"<{prefixSub}(\w+)>", rf"{prefixName}:\1", repl_val)
                    missing_prefix = f"PREFIX {prefixName}: <{prefixSub}>"
                    if re.search(re.escape(missing_prefix), query) is None:
                        query = f"PREFIX {prefixName}: <{prefixSub}>" + "\n" + query
            query = re.sub(rf"{re.escape(const)}(\W)", rf"{repl_val}\1", query)

        #===============================
        # Write vanilla version
        #===============================
        out_no_ss_file = os.path.join(output, f"{query_name}_v{var}_no_ss.sparql")
        with open(out_no_ss_file, mode="w") as out:
            out.write(query)
            out.close()

        #===============================
        # Write source selection version
        #===============================

        # Wrap each tp with GRAPH clause
        ntp = 0
        wherePos = np.inf
        for cur, line in enumerate(StringIO(query).readlines()):
            if "WHERE" not in line and cur < wherePos: continue
            else: wherePos = cur
            try:
                ntp += 1
                subline = re.search(r"^(\s|(\w+\s*\{)|\{)*((\?\w+|\w+:\w+|<\S+>)\s+(\?\w+|a|\w+:\w+|<\S+>)\s+(\?\w+|\w+:\w+|<\S+>))(\s|\}|\.)*\s*$", line).group(3)
                query = re.sub(re.escape(subline), f"GRAPH ?tp{ntp} {{ {subline} }}", query)
            except:
                ntp -= 1
                continue
        
        graph_proj = ' '.join([ f"?tp{i}" for i in np.arange(1, ntp+1) ])
        query = re.sub(r"SELECT(\s+DISTINCT)?\s+((\?\w+)\s+|\*)*", rf"SELECT\1 {graph_proj} ", query)


        Path(output).mkdir(parents=True, exist_ok=True)
        out_ss_file = os.path.join(output, f"{query_name}_v{var}_ss.sparql")
        with open(out_ss_file, mode="w") as out:
            out.write(query)
            out.close()

@cli.command()
@click.argument("query", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.option("--output", type=click.Path(exists=False, file_okay=True, dir_okay=False), default=None,
    help="The file in which the query result will be stored.")
@click.option("--output-format", type=click.Choice(["stdout", "csv"]), default="stdout")
@click.option("--records", type=click.Path(exists=False, file_okay=True, dir_okay=False), default=None,
    help="The file in which the stats will be stored.")
@click.option("--records-format", type=click.Choice(["stdout", "csv"]), default="stdout")
@click.option("--endpoint", type=str, default="http://localhost:8890/sparql/",
    help="URL to a SPARQL endpoint")
def execute_query(query, output, output_format, records, records_format, endpoint):

    query_name = Path(query).resolve().stem
    query_text = open(query, mode="r").read()

    startTime = time()    
    result = exec_query(query_text, endpoint)  
    endTime = time()
    
    csvOut = pd.read_csv(BytesIO(result))
    if output is None:
        if output_format == "stdout": 
            print(result.decode())
    else:
        if output_format == "csv":
            csvOut.to_csv(output, index=False, quotechar='"')
        elif output_format == "stdout": 
            print(result.decode())

    recordsOut = {
        "query": query_name,
        "exec_time": (endTime-startTime)*1e-3
    }

    if records is None :
        if records_format == "stdout": 
            print(recordsOut)
    else:
        if records_format == "csv": 
            pd.DataFrame(recordsOut, index=[0]).to_csv(records, index=False)
        elif records_format == "stdout": 
            print(recordsOut)

if __name__ == "__main__":
    cli()