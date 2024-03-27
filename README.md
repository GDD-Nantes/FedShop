# FedShop: The Federated Shop Benchmark

FedShop is a synthetic RDF Federated Benchmark designed for scalability. It evaluates the performance of SPARQL federated-query engines such as [FedX](https://rdf4j.org/documentation/programming/federation/), [CostFed](https://github.com/dice-group/CostFed), [Semagrow](https://semagrow.github.io/), Splendid, [HeFQUIN](https://github.com/LiUSemWeb/HeFQUIN), etc, when the number of federation members grows. FedShop is built around an e-commerce scenario with online hops and rating Web sites as in [BSBM](http://wbsg.informatik.uni-mannheim.de/bizer/berlinsparqlbenchmark/). Compared to  BSBM, each shop and rating site of FedShop has its own SPARQL endpoint and shares a standard catalog of products. Following the BSBM idea, the FedShop queries simulate a user navigating the federation of shops as if it was a single virtual shop. The scale factor corresponds to the number of shops and rating sites within the federation. Hence, with the FedShop benchmark, we can observe the performance of federated queries when the number of federation members increases.

FedShop consists of three components: 
- **the FedShop data generator** to generate data,
- **the FedShop query generator** to instantiate template queries, along with a Reference Source Assignment (RSA) queries ie. fedshop queries expressed as SPARQL 1.1 queries with service clauses,
- **the FedShop runner** to evaluate federated-query engines with FedShop queries on FedShop data.

## QuickStart and Documentation

- The quickstart guide is available in the [Quickstart tutorial](https://github.com/GDD-Nantes/FedShop/wiki/1.-Quick-start)
- How to configure fedshop and how to extend fedshop is available in the [wiki](https://github.com/GDD-Nantes/FedShop/wiki)




## FedShop200 Datasets and Queries

**FedShop200** is a basic set of datasets and queries generated with FedShop. It contains 120 SPARQL queries and datasets to populate a federation up to 200 endpoints. It is available at [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.7919872.svg)](https://doi.org/10.5281/zenodo.7919872) and [![Google Drive](https://img.shields.io/badge/Google%20Drive-4285F4?style=for-the-badge&logo=googledrive&logoColor=white)](https://drive.google.com/drive/folders/1vi7iElN25Pmtciy5y7iccx5T1P9bNMXJ). 

The following command allows downloading the archive (1.6GB) from Google Drive:

```bash
# !pip install --quiet gdown==4.5.4 --no-cache-dir
gdown 1vi7iElN25Pmtciy5y7iccx5T1P9bNMXJ -O fedshop.zip # large dataset
```
Instead of downloading the complete archive, you can also download only individual parts of FedShop:
* `fedshop-dataset.zip`: All the quads for the 200 federation members.
* `fedshop-virtuoso.db`: The final Virtuoso database with all 200 federation members.
* `fedshop-workload.txt`: The FedShop Workload
* `fedshop-service.txt`: RSA Fedshop Workload, i.e. the Reference Source Assignement for FedShop queries as SPARQL 1.1 queries with Service clauses.

## FedShop200 Results

A first evaluation of existing SPARQL Federation engines on **FedShop200** computed by the FedShop runner is available through a Jupyter Notebook:
-   [Jupyter Evaluation](FedShop_Evaluation.ipynb)

## FedShop Data Generator

The FedShop Data Generator is defined as three  [WatDiv](https://dsg.uwaterloo.ca/watdiv/) template models in [experiments/bsbm/model](experiments/bsbm/model/). These models follow the [BSBM](http://wbsg.informatik.uni-mannheim.de/bizer/berlinsparqlbenchmark/) specification as closely as possible. Using WatDiv models allows to changing the schema easily through the configuration file [`experiments/bsbm/config.yaml`](experiments/bsbm/config.yaml).

Most of the parameters of FedShop are set in [`experiments/bsbm/config.yaml`](experiments/bsbm/config.yaml). It includes the number of products to generate, the number of vendors and rating sites. 

Basic statistics about the default configuration of FedShop are available in the [jupyter notebook](Realistic_Synthetic_Federated.ipynb)

## Generate Datasets and Queries

Once `config.yaml` properly set, you can launch the generation of the FedShop benchmark with the following command:

```bash
python fedshop/benchmark.py generate data|queries experiments/bsbm/config.yaml  [OPTIONS]

OPTIONS:
--clean [benchmark|metrics|instances][+db]: clean the benchmark|metrics|instances then (optional) destroy all database containers
--touch : mark a phase as "terminated" so snakemake would not rerun it.
```

Such a process is very long and complex. All the artifacts produced during generation is created under experiment/bsbm. Datasets are created under experiments/bsbm/model/dataset, and queries under experiments/bsbm/benchmark/generation.

The overall workflow for FedShop generation is as follows:
* Create the catalog of products (200000 by default)
* Batch(0)= Create 10 autonomous vendors and 10 autonomous rating-sites sharing products from the catalog (products are replicated with local URL per vendors and rating sites). The distribution law can be controled with parameters declared in experiments/bsbm/config.yaml 
* Workload= Instantiate the 12 template queries with 10 different random place-holders, such that each query return results.
* Compute the optimal source assignment of each of the 120 queries of the Workload on Batch(0)
* For i from 1 to 9
  * Batch(i)=Batch(i-1)+10 new vendors and 10 rating-sites
  * Compute the Reference Source Assignment (RSA) for each query of the Workload over Batch(i)

We finished this process with a federation of 200 different federation members. This overall workflow can be changed thanks to parameters declared in experiments/bsbm/config.yaml 

Please note:
* The workflow is managed with the [Snakemake](https://snakemake.readthedocs.io/en/stable/) workflow management system. It allows the creation of reproducible and scalable data analyses. The snakemake files are located in experiments/bsbm/*.smk.
* The generation of queries and the computation of optimal source assignments requires [Virtuoso](https://github.com/openlink/virtuoso-opensource)
* The dataset generation is realized with many calls to [Watdiv](https://dsg.uwaterloo.ca/watdiv/). WatDiv is marginally updated and is available [here](https://github.com/mhoangvslev/watdiv/tree/e50cc38a28c79b73706ab3ee6f4d0340eedeee3f). It has been integrated into this github repository as a submodule.

## Evaluate federated-query engines using FedShop Runner

As the number of federation members can be high, having a SPARQL endpoint per federation member becomes hard. We ingested all shops and rating-sites over a single Virtuoso server as Virtual Endpoints,i.e.,  each shop and rating-site has its own Virtual SPARQL endpoint. The different configurations relative to Batch(i) are available to configure a given federated-query engine. It is possible at this stage to run all FedShop Benchmark with [Kobe](https://github.com/semagrow/kobe). However, we also provide a benchmark runner based on Snakemake that is convenient for managing failures during the execution of the benchmark.

Federated-query engines must implement a [template](fedshop/engines/TemplateEngine.py) to be integrated in the evaluation workflow. Many templates are already written in [`fedshop/engines/`](fedshop/engines/). Once integrated, 
the engine to be tested must be declared in `experiments/bsbm/config.yaml` to run.

The following command allows to launch the evaluation:
```bash
python fedshop/benchmark.py evaluate experiments/bsbm/config.yaml --rerun-incomplete [OPTIONS]

OPTIONS:
--clean [benchmark|metrics|instances][+db]: clean the benchmark|metrics|instances then (optional) destroy all database containers
--touch : mark a phase as "terminated" so Snakemake would not rerun it.
```
This launches the evaluation the FedShop workload over the different federations Batch(i) with the federated-query engines declared in experiments/bsbm/config.yaml. As for the generation, this process is long and complex and is managed by Snakemake. The evaluation rules are declared in experiments/bsbm/evaluate.smk. All the results are produced under experiments/bsbm/benchmark/evaluation.

Our [jupyter notebook](FedShop_Evaluation.ipynb) is already written to read results and computes the diverse metrics.

## Benchmark your engine:

- [Load](https://github.com/mhoangvslev/RSFB/wiki/Quick-tutorial#saveload-model) our [basic model]() and mark both the generation and evaluation phases as "completed":
```bash
python fedshop/benchmark.py generate data|queries experiments/bsbm/config.yaml --touch
python fedshop/benchmark.py evaluate experiments/bsbm/config.yaml --touch
```

- Register your engine's repo as a submodule:
```bash
cd engines
git submodule add <link_to_your_repo>
```

- Update `config.yaml` and provide key/value pair if needed:

```yaml
evaluation:
  n_attempts: 4
  timeout: 600
  engines:
    fedx:
      dir: "engines/FedX/target"
    ...
    <your_engine>:
      <keyN>: <valueN>
```

- Make `<your_engine>.py`:
```bash
cd fedshop/engines/
cp TemplateEngine.py <your_engine>.py
```

- Implement every function within `<your_engine>.py`.

- Use [evaluate command](https://github.com/mhoangvslev/RSFB/wiki/Quick-tutorial#generationevaluation) to benchmark your engine:
```bash
python fedshop/benchmark.py evaluate experiments/bsbm/config.yaml --clean metrics

```

- Compare to other engines using our Jupyter Notebook.


## Most used commands:

```bash
# Remove Snakemake log directory
rm -rf .snakemake

# Continue the workflow if interrupted 
python fedshop/benchmark.py generate|evaluate experiments/bsbm/config.yaml --rerun-incomplete

# Delete everything and restart
python fedshop/benchmark.py generate|evaluate experiments/bsbm/config.yaml --rerun-incomplete --clean all

# Keep the data but remove the intermediary artefacts and db containers.
python fedshop/benchmark.py generate data|queries experiments/bsbm/config.yaml --rerun-incomplete --clean benchmark+db

# Only remove the metrics files, applicable when you need to rerun some of the steps
python fedshop/benchmark.py generate data|queries experiments/bsbm/config.yaml --rerun-incomplete --clean metrics

```



## FedShop Contributors

* Minh-Hoang DANG ([Nantes University](https://english.univ-nantes.fr/))
* [Pascal Molli](https://sites.google.com/view/pascal-molli) ([Nantes University](https://english.univ-nantes.fr/))
* [Hala Skaf](http://pagesperso.ls2n.fr/~skaf-h/pmwiki/pmwiki.php) ([Nantes University](https://english.univ-nantes.fr/))
* [Olaf Hartig](https://olafhartig.de/) ([Linköping University](https://liu.se/)) 
* Julien Aimonier-Davat ([Nantes University](https://english.univ-nantes.fr/))
* Yotlan LE CROM ([Nantes University](https://english.univ-nantes.fr/))
* Matthieu Gicquel ([Nantes University](https://english.univ-nantes.fr/))

