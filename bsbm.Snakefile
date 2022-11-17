import pandas as pd
import os

VIRTUOSO_HOME = ""
ENDPOINT = "http://localhost:8890/sparql"

WORKDIR = "bsbm"
QUERY_DIR = f"{WORKDIR}/queries"
MODEL_DIR = f"{WORKDIR}/model"
BENCH_DIR = f"{WORKDIR}/benchmark"
VARIATION = 3
VERBOSE = True

N_VENDORS=10
SCALE_FACTOR=1

rule all:
    input: expand("{benchDir}/results.csv", benchDir=BENCH_DIR)

rule summary_exectime:
    input: 
        expand(
            "{benchDir}/{query}_v{var}_{ver}.rec.csv", 
            benchDir=BENCH_DIR,
            query=[Path(os.path.join(QUERY_DIR, f)).resolve().stem for f in os.listdir(QUERY_DIR)],
            var=range(VARIATION),
            ver=["no_ss", "ss"]
        )
    output: "{workdir}/results.csv"
    run:
        print(f"Merging {input} ...")
        print(output)
        pd.concat(map(pd.read_csv, input)).sort_values("query").to_csv(str(output), index=False)

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
      

rule run__build_sourceselection_query: # compute source selection query
    input:
        status=expand("{workdir}/virtuoso-ok.txt", workdir=WORKDIR),
        query=expand("{queryDir}/{{query}}.sparql", queryDir=QUERY_DIR)

    output: "{benchDir}/{query}_v{var}_{ver}.sparql"
    params:
        variation=VARIATION
    shell:
        'python utils/query.py transform-query {input.query} --endpoint {ENDPOINT} --output {BENCH_DIR} --variation {params.variation}'

rule ingest_virtuoso:
    input: expand("{model_dir}/vendors/shop{vendor_id}.nq", model_dir=MODEL_DIR, vendor_id=range(N_VENDORS))
    output: "{WORKDIR}/virtuoso-ok.txt"
    shell: 'sh utils/ingest.sh bsbm && echo "" > {WORKDIR}/virtuoso-ok.txt'

rule run__agg_shops:
    input: 
        vendor="{model_dir}/Vendor{vendor_id}.nt.tmp",
        indir="{model_dir}/products/"
    output: "{model_dir}/vendors/shop{vendor_id}.nq"
    shell: 'python utils/aggregator.py {input.vendor} {input.indir} {output}'

rule run__split_products:
    input: "{model_dir}/products.nt.tmp"
    output: directory("{model_dir}/products/")
    shell: 'python utils/splitter.py {input} {output}'

rule run__generate_products:
    output: "{model_dir}/products.nt.tmp", 
    params:
        scale_factor=SCALE_FACTOR,
        verbose=VERBOSE
    shell: "python {WORKDIR}/generate_product.py"

rule run__generate_vendors:
    output: "{model_dir}/Vendor{vendor_id}.nt.tmp"
    params:
        scale_factor=SCALE_FACTOR,
        verbose=VERBOSE
    shell: "python {WORKDIR}/generate_vendor.py {wildcards.vendor_id}"