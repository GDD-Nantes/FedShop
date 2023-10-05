import glob

for query_fn in sorted(glob.glob("experiments/bsbm/benchmark/generation/**/injected.sparql", recursive=True)):
    with open(query_fn, "r") as query_fs:
        print(f"==> {query_fn} <==")
        print(query_fs.read())
