import numpy as np
import pandas as pd
import os
from pathlib import Path
import glob
import time
import requests
import subprocess
import json

import sys
smk_directory = os.path.abspath(workflow.basedir)
sys.path.append(os.path.join(Path(smk_directory).parent.parent.parent, "fedshop"))

from utils import load_config

#===============================
# GENERATION PHASE:
# - Generate data
# - Ingest the data in virtuoso
#===============================

CONFIGFILE = config["configfile"]

WORK_DIR = "experiments/bsbm"
CONFIG = load_config(CONFIGFILE)["generation"]

N_QUERY_INSTANCES = CONFIG["n_query_instances"]
VERBOSE = CONFIG["verbose"]
N_BATCH = CONFIG["n_batch"]

# Config per batch
N_VENDOR=CONFIG["schema"]["vendor"]["params"]["vendor_n"]
N_RATINGSITE=CONFIG["schema"]["ratingsite"]["params"]["ratingsite_n"]

QUERY_DIR = f"{WORK_DIR}/queries"
MODEL_DIR = f"{WORK_DIR}/model"
BENCH_DIR = f"{WORK_DIR}/benchmark/generation"
TEMPLATE_DIR = f"{MODEL_DIR}/watdiv"

#=================
# USEFUL FUNCTIONS
#=================

def start_generator(status_file):
    exec_cmd = CONFIG["generator"]["exec"]
    if os.system(f"command -v {exec_cmd}") == 0:
        with open(status_file, "w") as f:
            f.write(exec_cmd + "\n")
    else: raise RuntimeError(f"{exec_cmd} doesn't exist...")

    return status_file

#=================
# PIPELINE
#=================

rule all:
    input:
        vendor=expand("{modelDir}/dataset/vendor{vendor_id}.nq", vendor_id=range(N_VENDOR), modelDir=MODEL_DIR),
        ratingsite=expand("{modelDir}/dataset/ratingsite{ratingsite_id}.nq", ratingsite_id=range(N_RATINGSITE), modelDir=MODEL_DIR)
    
rule generate_ratingsites:
    priority: 12
    threads: 5
    input: 
        status=expand("{workDir}/generator-ok.txt", workDir=WORK_DIR),
        product=ancient(CONFIG["schema"]["product"]["export_output_dir"])
    output: "{modelDir}/dataset/ratingsite{ratingsite_id}.nq"
    shell: "python fedshop/generate.py generate {CONFIGFILE} ratingsite {output} --id {wildcards.ratingsite_id}"

rule generate_vendors:
    priority: 13
    threads: 5
    input: 
        status=expand("{workDir}/generator-ok.txt", workDir=WORK_DIR),
        product=ancient(CONFIG["schema"]["product"]["export_output_dir"])
    output: "{modelDir}/dataset/vendor{vendor_id}.nq"
    shell: "python fedshop/generate.py generate {CONFIGFILE} vendor {output} --id {wildcards.vendor_id}"

rule generate_products:
    priority: 14
    threads: 1
    input: expand("{workDir}/generator-ok.txt", workDir=WORK_DIR)
    output: directory(CONFIG["schema"]["product"]["export_output_dir"]), 
    shell: 'python fedshop/generate.py generate {CONFIGFILE} product {output}'

rule start_generator_container:
    output: "{workDir}/generator-ok.txt"
    run: start_generator(f"{output}")