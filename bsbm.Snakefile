import pandas as pd
import os
from pathlib import Path

VIRTUOSO_HOME = ""
ENDPOINT = "http://localhost:8890/sparql"

WORK_DIR = "bsbm"
QUERY_DIR = f"{WORK_DIR}/queries"
MODEL_DIR = f"{WORK_DIR}/model"
BENCH_DIR = f"{WORK_DIR}/benchmark"
VARIATION = 3
VERBOSE = False

N_VENDORS=100
N_REVIEWERS=50
SCALE_FACTOR=1
SS_PROB_LIMIT=1
VERSIONS=["ss"] # noss

# rule all:
#     input: 
#         expand(
#             "{benchDir}/{query}_v{var}_{ver}.dft.out",
#             benchDir=BENCH_DIR,
#             query=[Path(os.path.join(QUERY_DIR, f)).resolve().stem for f in os.listdir(QUERY_DIR) if "_" not in f],
#             var=range(VARIATION),
#             ver=VERSIONS
#         )

# rule run__compile_and_run_federapp_default:
#    input:
#        query="{benchDir}/{query}_v{var}_{ver}.sparql",
#        config="{benchDir}/fedx.ttl"
#    params:
#        run=VARIATION
#    threads: 1
#    output:
#        result="{benchDir}/{query}_v{var}_{ver}.dft.out",
#        stat="{benchDir}/{query}_v{var}_{ver}.dft.stat",
#        log="{benchDir}/{query}_v{var}_{ver}.dft.log",
#        sourceselection="{benchDir}/{query}_v{var}_{ver}.dft.ss",
#        httpreq="{benchDir}/{query}_v{var}_{ver}.dft.nhttp"
#    shell:
#        "sh utils/compile_and_run_federapp.sh {input.config} {input.query} {output.result} {output.stat} {output.sourceselection} {output.httpreq} > {output.log}"

# rule run__compile_and_run_federapp_forcess:
#    input:
#        query="{benchDir}/{query}_v{var}_{ver}.sparql",
#        config=expand("{workDir}/fedx.ttl", workDir=WORK_DIR),
#        ssopt="{benchDir}/{query}_v{var}_{ver}.rec.csv"
#    params:
#        run=VARIATION
#    threads: 1
#    output:
#        result="{benchDir}/{query}_v{var}_{ver}.fss.out",
#        stat="{benchDir}/{query}_v{var}_{ver}.fss.stat",
#        log="{benchDir}/{query}_v{var}_{ver}.fss.log",
#        sourceselection="{benchDir}/{query}_v{var}_{ver}.fss.ss",
#        httpreq="{benchDir}/{query}_v{var}_{ver}.fss.nhttp"
#    shell:
#        "sh utils/compile_and_run_federapp.sh {input.config} {input.query} {output.result} {output.stat} {output.sourceselection} {output.httpreq} {input.ssopt} > {output.log}"

# rule run__generate_fedx_config:
#     input: 
#         exported=expand("{modelDir}/exported", modelDir=MODEL_DIR),
#         dump=expand(
#             "{benchDir}/{query}_v{var}_{ver}.dump.csv",
#             benchDir=BENCH_DIR,
#             query=[Path(os.path.join(QUERY_DIR, f)).resolve().stem for f in os.listdir(QUERY_DIR) if "_" not in f],
#             var=range(VARIATION),
#             ver=VERSIONS
#         )
#     output: "{benchDir}/fedx.ttl",
#     params: endpoint=ENDPOINT,
#     shell: 'python utils/generate-fedx-config-file.py {input.exported} {output} --endpoint {params.endpoint}'

rule all:
    input: 
        expand(
            "{benchDir}/{query}_v{var}_{ver}.rec.csv",
            benchDir=BENCH_DIR,
            query=[Path(os.path.join(QUERY_DIR, f)).resolve().stem for f in os.listdir(QUERY_DIR) if "_" not in f],
            var=range(VARIATION),
            ver=VERSIONS
        )

rule run__exec_sourceselection_query:
    input: "{benchDir}/{query}_v{var}_{ver}.sparql"
    output: 
        records="{benchDir}/{query}_v{var}_{ver}.rec.csv",
        dump="{benchDir}/{query}_v{var}_{ver}.dump.csv"
    params:
        endpoint=ENDPOINT,
        variation=VARIATION
    shell: 
        'python utils/query.py execute-query {input} --endpoint {params.endpoint} --output {output.dump} --output-format "csv" --records {output.records} --records-format "csv"'

# rule all:
#     input: 
#         expand(
#             "{benchDir}/{query}_v{var}_{ver}.sparql",
#             benchDir=BENCH_DIR,
#             query=[Path(os.path.join(QUERY_DIR, f)).resolve().stem for f in os.listdir(QUERY_DIR) if "_" not in f],
#             var=range(VARIATION),
#             ver=VERSIONS
#         )

rule run__build_sourceselection_query: # compute source selection query
    input:
        query=expand("{queryDir}/{{query}}.sparql", queryDir=QUERY_DIR),
        distrib=expand("{modelDir}/distrib/{{query}}.csv", modelDir=MODEL_DIR)
    output: 
        noss="{benchDir}/{query}_v{var}_noss.sparql",
        ss="{benchDir}/{query}_v{var}_ss.sparql"
    params:
        variation=VARIATION,
        pool=SS_PROB_LIMIT
    shell:
        "python utils/query.py transform-query {input.query} {input.distrib} {params.variation} {wildcards.var} {output.noss} {output.ss} --endpoint {ENDPOINT} --pool {params.pool}"

# rule all:
#     input: 
#         expand(
#             "{modelDir}/distrib/{query}.csv", 
#             modelDir=MODEL_DIR,
#             query=[Path(filename).resolve().stem for filename in os.listdir(QUERY_DIR) if "_" not in filename]
#         )

rule run__plot_distribution:
    input: 
        virtuoso=expand("{workDir}/virtuoso-ok.txt", workDir=WORK_DIR),
        queryfile=expand("{workDir}/plotter/{{query}}.sparql", workDir=WORK_DIR)
    output: 
        csv="{modelDir}/distrib/{query}.csv",
        #fig="{modelDir}/distrib/{query}.png",
        #fitout="{modelDir}/distrib/{feature}_fit.csv",
        #fitfig="{modelDir}/distrib/{feature}_fit.png"
    shell: "python utils/plot.py plot-entitytype-distribution {input.queryfile} --csvout {output.csv}"
        # --figout {output.fig} 
        #--fitout {output.fitout} --fitfig {output.fitfig}"

rule ingest_virtuoso:
    input: 
        shop=expand("{modelDir}/exported/shop{vendor_id}.nq", vendor_id=range(N_VENDORS), modelDir=MODEL_DIR),
        person=expand("{modelDir}/exported/person{person_id}.nq", person_id=range(N_REVIEWERS), modelDir=MODEL_DIR)
    output: "{WORK_DIR}/virtuoso-ok.txt"
    run:  os.system(f'sh utils/ingest.sh bsbm && echo "" > {WORK_DIR}/virtuoso-ok.txt')

rule run__agg_product_person:
    input:
        person="{modelDir}/tmp/person{person_id}.nt.tmp",
        product="{modelDir}/tmp/product/"
    output: "{modelDir}/exported/person{person_id}.nq"
    shell: 'python utils/aggregator.py {input.person} {input.product} {output} http://www.person{wildcards.person_id}.fr'

rule run__agg_product_vendor:
    input: 
        vendor="{modelDir}/tmp/vendor{vendor_id}.nt.tmp",
        product="{modelDir}/tmp/product/"
    output: "{modelDir}/exported/shop{vendor_id}.nq",   
    shell: 'python utils/aggregator.py {input.vendor} {input.product} {output} http://www.shop{wildcards.vendor_id}.fr'

rule run__split_products:
    input: "{modelDir}/tmp/product0.nt.tmp"
    output: directory("{modelDir}/tmp/product/")
    shell: 'python utils/splitter.py {input} {output}'

rule run__generate_reviewers:
    output: "{modelDir}/tmp/person{person_id}.nt.tmp"
    params:
        verbose=VERBOSE
    shell: 'python utils/generate.py generate {WORK_DIR}/config.yaml person {wildcards.modelDir}/bsbm-person.template {output} {wildcards.person_id} --verbose {params.verbose}'

rule run__generate_products:
    output: "{modelDir}/tmp/product0.nt.tmp", 
    params:
        verbose=VERBOSE
    shell: 'python utils/generate.py generate {WORK_DIR}/config.yaml product {wildcards.modelDir}/bsbm-product.template {output} 0 --verbose {params.verbose}'

rule run__generate_vendors:
    output: "{modelDir}/tmp/vendor{vendor_id}.nt.tmp"
    params:
        verbose=VERBOSE
    shell: 'python utils/generate.py generate {WORK_DIR}/config.yaml vendor {wildcards.modelDir}/bsbm-vendor.template {output} {wildcards.vendor_id} --verbose {params.verbose}'
