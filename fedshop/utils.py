import ast
import importlib
from io import BytesIO
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import colorlog
import numpy as np
import requests
from scipy.stats import norm, truncnorm
from omegaconf import OmegaConf
import psutil
import pandas as pd
from rdflib import Literal, URIRef

import logging

def fedshop_logger(logname):
    logger = logging.getLogger(logname)
    logger.setLevel(logging.DEBUG)

    # create console handler with a higher log level
    handler = colorlog.StreamHandler()
    formatter = colorlog.ColoredFormatter(
        "%(log_color)s%(levelname)s:%(name)s:%(message)s",
        reset=True,
        log_colors={
            'DEBUG':    'cyan',
            'INFO':     'purple',
            'WARNING':  'yellow',
            'ERROR':    'red',
            'CRITICAL': 'red',
        }
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

LOGGER = fedshop_logger(Path(__file__).name)

def docker_check_container_running(container_name):
    try:
        status = subprocess.check_output(f"docker inspect -f '{{{{.State.Running}}}}' {container_name}", shell=True)
        status = status.decode("utf-8").strip()
        return status == "true"
    except subprocess.CalledProcessError:
        return False

def check_container_status(compose_file, service_name, container_name):
    if docker_check_container_running(container_name):
        return "running"
    return "stopped"

def get_docker_endpoint_by_container_name(container_name):
    try:
        # Try to get the host port mapped to 8890
        cmd = f"docker inspect --format='{{{{(index (index .NetworkSettings.Ports \"8890/tcp\") 0).HostPort}}}}' {container_name}"
        port = subprocess.check_output(cmd, shell=True).decode("utf-8").strip()
        return f"http://localhost:{port}/sparql"
    except Exception:
        # Fallback for host network or if port is not mapped
        return "http://localhost:8890/sparql"

def get_virtuoso_containers(compose_file, service_name):
    try:
        cmd = f"docker compose -f {compose_file} ps -a --format '{{{{.Name}}}}'"
        output = subprocess.check_output(cmd, shell=True).decode("utf-8").strip()
        if not output: return []
        return output.split("\n")
    except Exception:
        return []

def get_docker_endpoints(compose_file, service_name):
    containers = get_virtuoso_containers(compose_file, service_name)
    return [get_docker_endpoint_by_container_name(c) for c in containers]

# Register resolvers
try:
    OmegaConf.register_new_resolver("get_docker_endpoints", get_docker_endpoints)
    OmegaConf.register_new_resolver("get_virtuoso_containers", get_virtuoso_containers)
    OmegaConf.register_new_resolver("get_docker_endpoint", get_docker_endpoint_by_container_name)
except Exception:
    pass

def str2n3(value):
    if str(value).startswith("http") or str(value).startswith("nodeID"): 
        return URIRef(value).n3()
    elif re.match(r"\d{4}-\d{2}-\d{2}", str(value)):
        return Literal(pd.to_datetime(value)).n3()
    elif re.match(r"(-?\d+)((\.\d+)|(e-?\d+))?", str(value)):
        return Literal(ast.literal_eval(str(value))).n3()
    else:
        return Literal(value).n3()

def load_config(filename, saveAs=None):
    """Load configuration from a file. By default, attributes are interpolated at access time.

    Args:
        filename ([type]): an input template config file
        saveAs ([type], optional): When specified, interpolate all attributes and persist to a file. Defaults to None, meaning that attributes will be interpolated at access time.

    Returns:
        [type]: [description]
    """
    
    custom_loader_file = f"{Path(filename).parent}/omega_conf.py"
    if os.path.exists(custom_loader_file):
        for path in Path(filename).parents:
            sys.path.append(str(path))
        config_module_name = custom_loader_file.replace(".py", "").replace("/", ".")

        config_module = importlib.import_module(config_module_name)
        config_loader = getattr(config_module, "load_config")
        config = config_loader(filename, saveAs=saveAs)
    else:
        config = OmegaConf.load(filename)
        if saveAs is not None:
            cache_config = None
            try: cache_config = OmegaConf.to_object(config)
            except: cache_config = { k: v for k, v in config.items() if k not in ["virtuoso"]}
            
            with open(saveAs, "w") as tmpfile:
                OmegaConf.save(cache_config, tmpfile)
    return config

def write_empty_stats(outfile, reason):
    with open(outfile, "w") as fout:
        fout.write("query,engine,instance,batch,attempt,exec_time,ask,source_selection_time,planning_time\n")
        if outfile != "/dev/null":
            basicInfos = re.match(r".*/(\w+)/(q\w+)/instance_(\d+)/batch_(\d+)/(attempt_(\d+)|debug)/stats.csv", outfile)
            engine = basicInfos.group(1)
            queryName = basicInfos.group(2)
            instance = basicInfos.group(3)
            batch = basicInfos.group(4)
            attempt = basicInfos.group(6)
            fout.write(",".join([queryName, engine, instance, batch, attempt, reason, reason, reason, reason])+"\n")
            
def create_stats(statsfile, failed_reason=None):
    """Create stats.csv from metrics.txt files
    """
    
    baseDir = Path(statsfile).parent
    
    print(statsfile)
    basicInfos = re.match(r".*/(\w+)/(q\w+)/instance_(\d+)/batch_(\d+)/(attempt_(\d+)|debug)/stats.csv", statsfile)
    result = {
        "engine": basicInfos.group(1),
        "query": basicInfos.group(2),
        "instance": basicInfos.group(3),
        "batch": basicInfos.group(4),
        "attempt": basicInfos.group(6)
    }
    
    metrics = {
        "source_selection_time": failed_reason,
        "planning_time": failed_reason,
        "ask": failed_reason,
        "exec_time": failed_reason,
        "http_req": failed_reason,
        "data_transfer": failed_reason
    }
    
    for metric in metrics.keys():
        metric_file = f"{baseDir}/{metric}.txt"
        if os.path.exists(metric_file):
            with open(metric_file, "r") as fs:
                metrics[metric] = float(fs.read())
                
    result.update(metrics)
    
    stats_df = pd.DataFrame([result])
    try:
        stats_df.to_csv(statsfile, index=False)
    except MemoryError:
        with open(statsfile, "w") as fs:
            # Write the header
            fs.write(f"{','.join(result.keys())}\n")
            fs.write(f"{','.join(result.values())}\n")    
    
def kill_process(proc_pid):
    try:
        process = psutil.Process(proc_pid)
        LOGGER.debug(f"Killing {process.pid} {process.name}")
        for child in process.children(recursive=True):
            LOGGER.debug(f"Killing child process {child.pid} {child.name}")
            child.kill()
        process.kill()
    except psutil.NoSuchProcess:
        LOGGER.warning(f"Process {proc_pid} already terminated...")
        pass
    
def ping(endpoint):
    proxies = {
        "http": "",
        "https": "",
    }
    try:
        response = requests.get(endpoint, proxies=proxies)
        # print(response.status_code, response.text)
        return response.status_code
    except: return -1


