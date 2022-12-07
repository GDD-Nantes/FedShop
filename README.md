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

- Tweaks parameters in `bsbm/config.yaml`
- Tweaks parameters at the beginning of `bsbm/benchmark.sh`

```bash
# Run
sh bsbm/benchmark.sh <task>

# Clean and run
sh bsbm/benchmark.sh <task> clean

# In case one of the batch did not complete
sh bsbm/clean.sh && sh bsbm/benchmark.sh <task>

<task>: generate|evaluate
```
