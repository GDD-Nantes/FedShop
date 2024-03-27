import glob
import numpy as np
import pandas as pd
import re

fedup_id = sorted(glob.glob("experiments/bsbm/benchmark/evaluation/fedup_id/**/provenance.csv", recursive=True))
rsa = sorted(glob.glob("experiments/bsbm/benchmark/evaluation/rsa/**/provenance.csv", recursive=True))

for fedup_id_ss_file, rsa_ss_file in zip(fedup_id, rsa):
    
    try:
        fedup_id_df = pd.read_csv(fedup_id_ss_file)
        rsa_ss_df = pd.read_csv(rsa_ss_file)
        
        # Iterate through columns in rsa_ss_df
        for column in rsa_ss_df.columns:
            if column not in fedup_id_df.columns:
                # Add an empty column to fedup_id_df with the same column name
                fedup_id_df[column] = np.nan
                
        fedup_id_df = fedup_id_df.reindex(sorted(fedup_id_df.columns, key=lambda x: int(re.sub(r"tp(\d+)", r"\1", x))), axis=1)
        fedup_id_df = fedup_id_df \
                .sort_values(fedup_id_df.columns.to_list()) \
                .reset_index(drop=True)
                
        rsa_ss_df = rsa_ss_df.reindex(sorted(rsa_ss_df.columns, key=lambda x: int(re.sub(r"tp(\d+)", r"\1", x))), axis=1)
        rsa_ss_df = rsa_ss_df \
                .sort_values(rsa_ss_df.columns.to_list()) \
                .reset_index(drop=True)

        if not fedup_id_df.equals(rsa_ss_df):
            print(fedup_id_df)
            print(rsa_ss_df)
            raise RuntimeError(f"{fedup_id_ss_file} is not equal to {rsa_ss_file}")
        
    except pd.errors.EmptyDataError:
        raise RuntimeError(f"Either {fedup_id_ss_file} or {rsa_ss_file} is empty!")