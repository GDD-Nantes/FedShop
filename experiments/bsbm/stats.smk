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
sys.path.append(os.path.join(Path(smk_directory).parent.parent, "rsfb"))

from utils import load_config, get_docker_endpoint_by_container_name, check_container_status

#===============================
# GENERATION PHASE:
# - Generate data
# - Ingest the data in virtuoso
#===============================

CONFIGFILE = config["configfile"]

WORK_DIR = "experiments/bsbm/stats"

#=================
# PIPELINE
#=================

rule all:
    input: 
        expand(
            "{workDir}/{kind}.stat",
            workDir=WORK_DIR,
            kind=["class_distribution", "vendor_stats", "ratingsite_stats", "endpoint_distribution"]
        )

rule generate_stats:
    output: "{workDir}/{kind}.stat"
    run:
        shell(f"RSFB__CONFIGFILE={CONFIGFILE} python experiments/bsbm/stats/stats.py get-{str(wildcards.kind).replace('_', '-')}")

