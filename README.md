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

- Compile Federapp (modified FedX)

```bash
cd Federapp/
mvn clean && mvn install dependency:copy-dependencies package
```

2. Run experiments

- Tweaks parameters in `experiments/bsbm/config.yaml`
- Tweaks parameters at the beginning of `experiments/bsbm/benchmark.sh`

```bash
# Run
sh experiments/bsbm/benchmark.sh <task>

# Clean and run
sh experiments/bsbm/benchmark.sh <task> clean

# In case one of the batch did not complete
sh experiments/bsbm/clean.sh && sh experiments/bsbm/benchmark.sh <task>

<task>: generate|evaluate
```
