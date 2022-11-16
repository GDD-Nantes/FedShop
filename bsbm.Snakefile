import pandas as pd
import os

VIRTUOSO_HOME=""
ENDPOINT="http://localhost:8890/sparql"
QUERY_DIR="bsbm/queries"
MODEL_DIR="bsbm/model"
VARIATION=3
VERBOSE=True

N_VENDORS=10
SCALE_FACTOR=1

rule all:
    input: "results.csv"

rule summary_exectime:
    input: 
        expand(
            "{model_dir}/benchmark/{query}_v{var}_{ver}.rec.csv", 
            model_dir=MODEL_DIR,
            query=[Path(os.path.join(QUERY_DIR, f)).resolve().stem for f in os.listdir(QUERY_DIR)],
            var=range(VARIATION),
            ver=["no_ss", "ss"]
        )
    output: "results.csv"
    run:
        print(f"Merging {input} ...")
        print(output)
        pd.concat(map(pd.read_csv, input)).sort_values("query").to_csv(str(output), index=False)

rule run__exec_sourceselection_query:
    input: "{model_dir}/benchmark/{query}_v{var}_{ver}.sparql"
    output: 
        records="{model_dir}/benchmark/{query}_v{var}_{ver}.rec.csv",
        dump="{model_dir}/benchmark/{query}_v{var}_{ver}.dump.csv"
    params:
        endpoint=ENDPOINT,
        variation=VARIATION
    shell: 
        'python utils/query.py {input} --entrypoint {params.endpoint} --output {output.dump} --records {output.records}'
      

rule run__build_sourceselection_query: # compute source selection query
    input:
        status="virtuoso-status.txt",
        query=expand(
            "{query_dir}/{{query}}.sparql",
            query_dir=QUERY_DIR
        )
    output: "{model_dir}/benchmark/{query}_v{var}_{ver}.sparql"
    params:
        variation=VARIATION
    shell:
        'python utils/transform_query.py {input.query} --entrypoint {ENDPOINT} --output {model_dir}/benchmark/ --variation {params.variation}'

rule ingest_virtuoso:
    input: expand("{{model_dir}}/vendors/shop{vendor_id}.nq", vendor_id=range(N_VENDORS))
    output: "virtuoso-status.txt"
    shell: 'sh ingest.sh bsbm && echo "Success" > virtuoso-status.txt'

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
    shell: "python bsbm/generate_product.py"

rule run__generate_vendors:
    output: "{model_dir}/Vendor{vendor_id}.nt.tmp"
    params:
        scale_factor=SCALE_FACTOR,
        verbose=VERBOSE
    shell: "python bsbm/generate_vendor.py {wildcards.vendor_id}"