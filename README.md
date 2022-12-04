# RSFB
Code for Realistic Synthetic Federated Benchmark

Refer to the [Wiki](https://github.com/mhoangvslev/RSFB/wiki) for details.

# Instruction

1. Install dependencies

- Install Python dependencies

```bash
pip install -r requirements.txt
```
- Install [Docker](https://docs.docker.com/get-docker/)

- Install [WatDiv](https://github.com/mhoangvslev/watdiv) (optional)


2. Run experiments

- Tweaks parameters in `bsbm/config.yaml`
- Tweaks parameters at the beginning of `bsbm/benchmark.sh`

```bash
# Run
sh bsbm/benchmark.sh

# Clean and rerun
sh bsbm/clean.sh deep && sh bsbm/benchmark.sh

# In case one of the batch did not complete
sh bsbm/clean.sh && sh bsbm/benchmark.sh
```
