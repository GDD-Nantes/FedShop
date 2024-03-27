import glob
from pathlib import Path
import re
import pandas as pd

stats = []
for query_fn in sorted(glob.glob("experiments/bsbm/benchmark/generation/**/results.csv", recursive=True)):
    name_search = re.search(r".*/(q\w+)/instance_(\d+)/batch_(\d+)/results.csv", query_fn)
    query = name_search.group(1)
    instance = int(name_search.group(2))
    batch = int(name_search.group(3))

    results_df = pd.read_csv(query_fn)
    stats.append({"query": query, "instance": instance, "batch": batch, "nbResults": len(results_df)})

df = pd.DataFrame.from_records(stats)
pivoted_df = df.pivot(index=['query', 'instance'], columns='batch', values='nbResults').reset_index()
pivoted_df = pivoted_df.rename(columns=lambda x: f'batch{x}' if isinstance(x, int) else x)

print(pivoted_df)
pivoted_df.to_csv("stat_expected_nb_results.csv", index=False)
