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
sys.path.append(os.path.join(Path(smk_directory).parent.parent, "fedshop"))

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
            kind=["class_distribution", "vendor_stats", "ratingsite_stats", "endpoint_distribution", "overall_stats"]
        )

rule class_distribution:
    output: "{workDir}/class_distribution.stat"
    run:
        shell(f"RSFB__CONFIGFILE={CONFIGFILE} python experiments/bsbm/stats/stats.py get-class-distribution")

rule vendor_stats:
    output: "{workDir}/vendor_stats.stat"
    run:
        shell(f"RSFB__CONFIGFILE={CONFIGFILE} python experiments/bsbm/stats/stats.py get-vendor-stats")

rule ratingsite_stats:
    output: "{workDir}/ratingsite_stats.stat"
    run:
        shell(f"RSFB__CONFIGFILE={CONFIGFILE} python experiments/bsbm/stats/stats.py get-ratingsite-stats")

rule endpoint_distribution:
    output: "{workDir}/endpoint_distribution.stat"
    run:
        shell(f"RSFB__CONFIGFILE={CONFIGFILE} python experiments/bsbm/stats/stats.py get-endpoint-distribution")

rule overall_stats:
    input:
        vendor_stats="{workDir}/vendor_stats.stat",
        ratingsite_stats="{workDir}/ratingsite_stats.stat"
    output: "{workDir}/overall_stats.stat"
    run:
        shell(f"RSFB__CONFIGFILE={CONFIGFILE} python experiments/bsbm/stats/stats.py get-overall-stats")