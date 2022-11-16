# RSFB
Code for Realistic Synthetic Federated Benchmark

# Instruction

1. Install dependencies

```bash
pip install -r requirements.txt
```

2. Run experiments

- Tweaks parameters in `bsbm/config.yaml`
- Tweaks parameters at the beginning of `bsbm.Snakefile` for variate `N_VENDORS` and `SCALE_FACTOR`

```bash
sh clean.sh && snakemake --snakefile bsbm.Snakefile --cores 1 --latency-wait 1
```
