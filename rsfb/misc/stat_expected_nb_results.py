import glob
from pathlib import Path
import pandas as pd

stats = {}
for query_fn in glob.glob("experiments/bsbm/benchmark/generation/**/results.csv", recursive=True):
    results_df = pd.read_csv(query_fn)
    row_id = str(Path(query_fn).parent)
    stats[row_id] = [len(results_df)]

df = pd.DataFrame.from_dict(stats, orient="index", columns=["nbResults"])
df.to_csv("stat_expected_nb_results.csv")
