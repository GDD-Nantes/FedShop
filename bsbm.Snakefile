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
N_OFFERS=10
SCALE_FACTOR=1

# rule all:
#     input: expand("{benchDir}/results.csv", benchDir=BENCH_DIR)

# rule summary_exectime:
#     input: 
#         expand(
#             "{benchDir}/{query}_v{var}_{ver}.rec.csv", 
#             benchDir=BENCH_DIR,
#             query=[Path(os.path.join(QUERY_DIR, f)).resolve().stem for f in os.listdir(QUERY_DIR)],
#             var=range(VARIATION),
#             ver=["no_ss", "ss"]
#         )
#     output: "{workdir}/results.csv"
#     run:
#         print(f"Merging {input} ...")
#         print(output)
#         pd.concat(map(pd.read_csv, input)).sort_values("query").to_csv(str(output), index=False)

# rule run__exec_sourceselection_query:
#     input: "{benchDir}/{query}_v{var}_{ver}.sparql"
#     output: 
#         records="{benchDir}/{query}_v{var}_{ver}.rec.csv",
#         dump="{benchDir}/{query}_v{var}_{ver}.dump.csv"
#     params:
#         endpoint=ENDPOINT,
#         variation=VARIATION
#     shell: 
#         'python utils/query.py execute-query {input} --endpoint {params.endpoint} --output {output.dump} --output-format "csv" --records {output.records} --records-format "csv"'
      

# rule run__build_sourceselection_query: # compute source selection query
#     input:
#         status=expand("{workdir}/virtuoso-ok.txt", workdir=WORK_DIR),
#         query=expand("{queryDir}/{{query}}.sparql", queryDir=QUERY_DIR)

#     output: "{benchDir}/{query}_v{var}_{ver}.sparql"
#     params:
#         variation=VARIATION
#     shell:
#         'python utils/query.py transform-query {input.query} --endpoint {ENDPOINT} --output {BENCH_DIR} --variation {params.variation}'

rule all:
    input: 
        expand(
            "{model_dir}/distrib/{feature}.csv", 
            model_dir=MODEL_DIR, 
            feature=[Path(filename).resolve().stem for filename in os.listdir(os.path.join(WORK_DIR, "plotter"))]
        )

rule run__plot_distribution:
    input: expand("{workdir}/virtuoso-ok.txt", workdir=WORK_DIR)
    output: 
        csv="{model_dir}/distrib/{feature}.csv",
        fig="{model_dir}/distrib/{feature}.png",
        fitout="{model_dir}/distrib/{feature}_fit.csv",
        fitfig="{model_dir}/distrib/{feature}_fit.png"
    shell: "python utils/plot.py plot-entitytype-distribution {WORK_DIR}/plotter/{wildcards.feature}.sparql \
        --csvout {output.csv} --figout {output.fig} \
        --fitout {output.fitout} --fitfig {output.fitfig}"

rule ingest_virtuoso:
    input: expand("{model_dir}/vendor/shop{vendor_id}.nq", vendor_id=range(N_VENDORS), model_dir=MODEL_DIR)
    output: "{WORK_DIR}/virtuoso-ok.txt"
    run: 
        os.system(f'sh utils/ingest.sh bsbm && echo "" > {WORK_DIR}/virtuoso-ok.txt')
        if not VERBOSE:
            os.system(f"rm {MODEL_DIR}/*.tmp")

rule run__agg_product_vendor:
    input: 
        vendor="{model_dir}/vendor{vendor_id}.nt.tmp",
        product="{model_dir}/product/"
    output: "{model_dir}/vendor/shop{vendor_id}.nq"
    shell: 'python utils/aggregator.py {input.vendor} {input.product} {output} http://www.shop{wildcards.vendor_id}.fr'

rule run__split_products:
    input: "{model_dir}/product0.nt.tmp"
    output: directory("{model_dir}/product/")
    shell: 'python utils/splitter.py {input} {output}'

# rule run__split_offers:
#     input: "{model_dir}/offer{offer_id}.nt.tmp"
#     output: directory("{model_dir}/offer/")
#     shell: 'python utils/splitter.py {input} {output}'

# rule run__generate_offers:
#     output: "{model_dir}/offer{offer_id}.nt.tmp"
#     params:
#         verbose=VERBOSE
#     shell: 'python utils/generate.py generate {WORK_DIR}/config.yaml vendor {wildcards.model_dir}/bsbm-vendor.template {output} {wildcards.offer_id} --verbose {params.verbose}'

rule run__generate_products:
    output: "{model_dir}/product0.nt.tmp", 
    params:
        verbose=VERBOSE
    shell: 'python utils/generate.py generate {WORK_DIR}/config.yaml product {wildcards.model_dir}/bsbm-product.template {output} 0 --verbose {params.verbose}'

rule run__generate_vendors:
    output: "{model_dir}/vendor{vendor_id}.nt.tmp"
    params:
        verbose=VERBOSE
    shell: 'python utils/generate.py generate {WORK_DIR}/config.yaml vendor {wildcards.model_dir}/bsbm-vendor.template {output} {wildcards.vendor_id} --verbose {params.verbose}'

