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
    input: expand("{model_dir}/vendors/shop{vendor_id}.nq", model_dir=MODEL_DIR, vendor_id=range(N_VENDORS))

# rule ingest_virtuoso:
#     input: expand("{model_dir}/vendors/shop{vendor_id}.nq", model_dir=MODEL_DIR, vendor_id=range(N_VENDORS))
#     run:
#         os.python(f`./isql "EXEC=ld_dir('../vad/dbpedia-201610', '*.ttl.bz2', 'http://example.com/datasets/dbpedia-201610');`)

rule run__agg_shops:
    input: 
        vendor="{model_dir}/Vendor{vendor_id}.nt.tmp",
        indir=directory("{model_dir}/products/")
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

# rule summary_exectime:
#     input: 
#         expand(
#             "benchmark/{query}_v{var}_{ver}.rec.csv", 
#             query=[Path(os.path.join(QUERY_DIR, f)).resolve().stem for f in os.listdir(QUERY_DIR)],
#             var=range(VARIATION),
#             ver=["no_ss", "ss"]
#         )
#     output: "results.csv"
#     run:
#         print(f"Merging {input} ...")
#         print(output)
#         pd.concat(map(pd.read_csv, input)).sort_values("query").to_csv(str(output), index=False)

# rule run__exec_sourceselection_query:
#     input: "benchmark/{query}_v{var}_{ver}.sparql"
#     output: 
#         records="benchmark/{query}_v{var}_{ver}.rec.csv",
#         dump="benchmark/{query}_v{var}_{ver}.dump.csv"
#     params:
#         endpoint=ENDPOINT,
#         variation=VARIATION
#     shell: 
#         'python utils/query.py {input} --entrypoint {params.endpoint} --output {output.dump} --records {output.records}'
      

# rule run__build_sourceselection_query: # compute source selection query
#     input: 
#         query=expand(
#             "{query_dir}/{{query}}.sparql",
#             query_dir=QUERY_DIR
#         ),
#         endpoint=expand(
#             "{model_dir}/bsbm.nq",
#             model_dir=MODEL_DIR
#         )
#     output: "benchmark/{query}_v{var}_{ver}.sparql"
#     params:
#         variation=VARIATION
#     shell:
#         'python utils/transform_query.py {input.query} --entrypoint {input.endpoint} --output benchmark/ --variation {params.variation}'