import glob
import os
from pathlib import Path
import re
import shutil
import pandas as pd
from tqdm import tqdm

# from rsfb.query import write_query

print(os.getcwd())
count = 0

# summary_df = pd.concat([pd.read_csv(ss) for ss in glob.glob("experiments/bsbm/benchmark/generation/metrics_batch*.csv", recursive=True)])
# print(summary_df.groupby(["query", "batch"]).mean().query("`query` == 'q05'"))

for engine_result_file in tqdm(glob.glob("experiments/bsbm/benchmark/evaluation/arq/**/results.csv", recursive=True)):
    name_search = re.search(r".*/(\w+)/(q\w+)/instance_(\d+)/batch_(\d+)/(attempt_(\d+)/)?results.csv", engine_result_file)
    engine = name_search.group(1)
    query = name_search.group(2)
    instance = int(name_search.group(3))
    batch = int(name_search.group(4))
    attempt = name_search.group(6)
    
    if os.stat(engine_result_file).st_size == 0: 
        # print(f"Skipping empty file {engine_result_file}")
        continue
    
    engine_results = pd.read_csv(engine_result_file).dropna(how="all", axis=1)
    engine_results = engine_results.reindex(sorted(engine_results.columns), axis=1)
    engine_results = engine_results \
        .sort_values(engine_results.columns.to_list()) \
        .drop_duplicates() \
        .reset_index(drop=True) 
    
    expected_result_file = f"experiments/bsbm/benchmark/generation/{query}/instance_{instance}/batch_{batch}/results.csv"
    expected_results = pd.read_csv(expected_result_file).dropna(how="all", axis=1)
    expected_results = expected_results.reindex(sorted(expected_results.columns), axis=1)
    expected_results = expected_results \
        .sort_values(expected_results.columns.to_list()) \
        .reset_index(drop=True)
    
    if not engine_results.equals(expected_results):
        # print(expected_results)
        # print("not equals to")
        # print(engine_results)
        
        # print(f"{engine_result_file} does not produce the same results as {expected_result_file}")
        
        #shutil.rmtree(Path(engine_result_file).parent.parent)
    
        count += 1

print(count)