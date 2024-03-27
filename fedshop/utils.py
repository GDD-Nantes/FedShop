import ast
from io import BytesIO
import json
import os
from pathlib import Path
import re
import subprocess
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

def str2n3(value):
    if str(value).startswith("http") or str(value).startswith("nodeID"): 
        return URIRef(value).n3()
    elif re.match(r"\d{4}-\d{2}-\d{2}", str(value)):
        return Literal(pd.to_datetime(value)).n3()
    elif re.match(r"(-?\d+)((\.\d+)|(e-?\d+))?", str(value)):
        return Literal(ast.literal_eval(str(value))).n3()
    else:
        return Literal(value).n3()

class RandomBucket:	
    def __init__(self, size):
        self._cumulativePercentage = [None] * size
        self._objects = [None] * size
        self._index=0
        self._totalPercentage = 0.0
	
    def add(self, percentage, obj):
        if self._index == len(self._objects):
            return
        else:
            self._objects[self._index] = obj
            self._cumulativePercentage[self._index] = percentage
            self._totalPercentage += percentage
		
        self._index += 1
		
        if self._index == len(self._objects):
            cumul = 0.0
            for i in range(len(self._objects)):
                cumul += self._cumulativePercentage[i] / self._totalPercentage
                self._cumulativePercentage[i] = cumul

	
    def getRandom(self):
        randIndex = np.random.uniform()
		
        for i in range(len(self._objects)):
            if randIndex <= self._cumulativePercentage[i]:
                return self._objects[i]
		
        # Should never happens, but...
        return self._objects[len(self._objects)-1]

class NormalDistGenerator:
    def __init__(self, mu, sigma, avg) -> None:
        self._avg = avg
        self._mu = mu
        self._sigma = sigma
    
    def getValue(self) -> int:
        randVal: float = norm.ppf(np.random.rand(), loc=self._mu, scale=self._sigma)

        while randVal < 0:
            randVal = norm.ppf(np.random.rand(), loc=self._mu, scale=self._sigma)
        
        return int(((randVal / self._mu) * self._avg) + 1)

class NormalDistRangeGenerator:
    def __init__(self, mu, sigma, maxValue, normalLimit) -> None:
        self._mu = mu
        self._sigma = sigma
        self._maxValue = maxValue
        self._normalLimit = normalLimit
    
    def getValue(self) -> int:
        randVal: float = norm.ppf(np.random.rand(), loc=self._mu, scale=self._sigma)

        while randVal > self._normalLimit or randVal < 0:
            randVal = norm.ppf(np.random.rand(), loc=self._mu, scale=self._sigma)
        
        return int(((randVal / self._normalLimit) * self._maxValue) + 1)

def divide(*args):
    if len(args) != 2:
        raise RuntimeError(f"The number of arguments must not exceed 2. Args: {args}")
    
    return int(args[0] / args[1])

def get_branching_factors(nbProducts):
    """Compute the branching factor given the number of products. Ref: bsbmtool

    Args:
        nbProducts (_type_): _description_

    Returns:
        _type_: _description_
    """
    
    logSF = np.log10(nbProducts)

    # depth = log10(scale factor)/2 + 1
    depth = round(logSF / 2) + 1
		
    branchingFactors = [None] * depth
    branchingFactors[0] = 2 * round(logSF)

    temp = [2, 4, 8]
    for i in range(depth):
        if (i+1) < depth:
            branchingFactors[i] = 8
        else:
            value = temp[round(logSF*3/2+1) % 3]
            branchingFactors[i] = value

    return branchingFactors

def create_product_type_hierarchy(nbProducts):
    branchFt = get_branching_factors(nbProducts)
    oldDepth = -1
    depth = 0
    nr = 1
    
    maxProductTypeNrPerLevel = []
    productTypeLeaves = []
    productTypeNodes = []

    typeQueue = [depth]
    while len(typeQueue) > 0:
        parent_type = typeQueue.pop(0)
        depth = parent_type

        if oldDepth != depth:
            oldDepth = depth
            maxProductTypeNrPerLevel.append(nr)

        for _ in range(branchFt[parent_type]):
            nr += 1
            child_type = parent_type + 1

            if parent_type == len(branchFt)-1:
                productTypeLeaves.append(child_type)
            else:
                productTypeNodes.append(child_type)
                typeQueue.append(child_type)

    
    if nr != maxProductTypeNrPerLevel[len(maxProductTypeNrPerLevel)-1]:
        maxProductTypeNrPerLevel.append(nr)
    
    return productTypeLeaves, productTypeNodes

def get_product_features(nbProducts):
    """Compute the number of features given the number of products, and the random number of required feature for 1 product. Ref: bsbmtool

    Args:
        nbProducts (_type_): _description_

    Returns:
        _type_: _description_
    """
    productTypeLeaves, productTypeNodes = create_product_type_hierarchy(nbProducts)
    leaves_features, nodes_features = [None] * len(productTypeLeaves), [None] * len(productTypeNodes)
    depth = productTypeLeaves[0]
    featureFrom = [None] * depth
    featureTo = [None] * depth

    featureFrom[0] = featureTo[0] = 5
    depthSum = depth * (depth+1) / 2 - 1

    for i in range(2, depth+1):
        featureFrom[i-1] = int(35 * i / depthSum)
        featureTo[i-1] = int(75 * i / depthSum)

    productFeatureNr = 1

    for i, node in enumerate(productTypeNodes):
        if i == 0: continue
        _from = featureFrom[node]
        _to = featureTo[node] + 1

        _count = np.random.randint(_from, _to)
        productFeatureNr += _count
        nodes_features[i] = _count

    for i, node in enumerate(productTypeLeaves):
        _from = featureFrom[node-1]
        _to = featureTo[node-1] + 1

        _count = np.random.randint(_from, _to)
        productFeatureNr += _count
        leaves_features[i] = _count

    return productFeatureNr, np.random.choice(leaves_features).item()

def generate_producer_distribution(productCount):
    productCountGen = NormalDistGenerator(3, 1, 50)
    productNr = 1
    producerOfProduct = [0]
		
    while productNr <= productCount :
        # Now generate Products for this Producer
        hasNrProducts = productCountGen.getValue()
        if productNr+hasNrProducts-1 > productCount:
            hasNrProducts = productCount - productNr + 1
        productNr += hasNrProducts
        producerOfProduct.append(productNr-1)
    return producerOfProduct

def get_product_producers(nbProducts):
    """Compute the number of producers given the number of products and the number of types per product. Ref: bsbmtool

    Args:
        nbProducts (_type_): _description_

    Returns:
        _type_: _description_
    """
    productNr = 1
    producerNr = 1

    producerOfProduct = generate_producer_distribution(nbProducts)
    nbTypes = []
		
    while producerNr < len(producerOfProduct):	
        # Generate Publisher data		
        hasNrProducts = producerOfProduct[producerNr] - producerOfProduct[producerNr-1]
        nbTypes.append(hasNrProducts)
        # createProductsOfProducer(producerNr, productNr, hasNrProducts, productSeedGen)
			
        productNr += hasNrProducts
        producerNr += 1
    
    return producerNr, np.random.choice(nbTypes).item()

def __get_publisher_info(x):
    return -1 if x is None else pd.DataFrame.from_records(x) \
        .query('`URL` == "0.0.0.0" and `TargetPort` == 8890')["PublishedPort"] \
        .astype(int).item()

def get_docker_endpoints(manual_endpoint, compose_file, service_name):
    if manual_endpoint != -1:
        return [f"http://localhost:{manual_endpoint}/sparql"]
    
    cmd = f"docker-compose -f {compose_file} ps --all --format json {service_name}"
    proc = subprocess.run(cmd, capture_output=True, shell=True)

    if proc.returncode != 0:
        raise RuntimeError(f"{cmd} return code {proc.returncode}!")

    json_bytes = proc.stdout
    with BytesIO(json_bytes) as json_bs:
        records = []
        for js in json_bs.readlines():
            records.append(json.loads(js))

        infos = pd.DataFrame.from_records(records)
        infos["containerId"] = infos["Name"].str.replace(r".*\-(\d+)$", r"\1", regex=True).astype(int)
        infos.sort_values("containerId", inplace=True)
                
        result = infos["Publishers"] \
            .apply(__get_publisher_info) \
            .apply(lambda x: f"http://localhost:{x}/sparql") \
            .to_list()
        return result

def get_docker_endpoint_by_container_name(compose_file, service_name, container_name):        
    json_bytes = subprocess.run(f"docker-compose -f {compose_file} ps --all --format json {service_name}", capture_output=True, shell=True).stdout
    with BytesIO(json_bytes) as json_bs:
        records = []
        for js in json_bs.readlines():
            records.append(json.loads(js))
        infos = pd.DataFrame.from_records(records)
        infos["containerId"] = infos["Name"].str.replace(r".*\-(\d+)$", r"\1", regex=True).astype(int)
        infos.sort_values("containerId", inplace=True)
        result = infos.query(f"`Name` == {repr(container_name)}")["Publishers"] \
            .apply(__get_publisher_info) \
            .apply(lambda x: f"http://localhost:{x}/sparql") \
            .item()
        return result

def check_container_status(compose_file, service_name, container_name):
    compose_proc = subprocess.run(f"docker-compose -f {compose_file} ps --all --format json {service_name}", capture_output=True, shell=True)
    json_bytes = compose_proc.stdout
    
    with BytesIO(json_bytes) as json_bs:
        records = []
        for js in json_bs.readlines():
            records.append(json.loads(js))        
        infos = pd.DataFrame.from_records(records)
    
        result = None
        if not infos.empty:
            result = infos.query(f"`Name` == {repr(container_name)}")["State"].item()
            
        return result
    
def get_docker_containers(compose_file, service_name):    
    cmd = f"docker-compose -f {compose_file} ps --all --format json {service_name}"
    json_bytes = subprocess.run(cmd, capture_output=True, shell=True).stdout
    with BytesIO(json_bytes) as json_bs:
        records = []
        for js in json_bs.readlines():
            records.append(json.loads(js))
        result = pd.DataFrame.from_records(records)
        result["containerId"] = result["Name"].str.replace(r".*\-(\d+)$", r"\1", regex=True).astype(int)
        result.sort_values("containerId", inplace=True)
        return result["Name"].to_list()

def normal_truncated(mu, sigma, lower, upper):
    return int(truncnorm.rvs((lower - mu) / sigma, (upper - mu) / sigma, loc=mu, scale=sigma))
    
OmegaConf.register_new_resolver("multiply", lambda *args: np.prod(args).item())
OmegaConf.register_new_resolver("sum", lambda *args: np.sum(args).item())
OmegaConf.register_new_resolver("divide", divide)
OmegaConf.register_new_resolver("get_docker_endpoints", get_docker_endpoints)
OmegaConf.register_new_resolver("get_virtuoso_containers", get_docker_containers)
OmegaConf.register_new_resolver("get_product_type_n", lambda nbProd: len(create_product_type_hierarchy(nbProd)[0]))
OmegaConf.register_new_resolver("get_product_type_c", lambda nbProd: get_product_producers(nbProd)[1])
OmegaConf.register_new_resolver("get_product_feature_n", lambda nbProd: get_product_features(nbProd)[0])
OmegaConf.register_new_resolver("get_product_feature_c", lambda nbProd: get_product_features(nbProd)[1])
OmegaConf.register_new_resolver("get_product_producer_n", lambda nbProd: get_product_producers(nbProd)[0])

OmegaConf.register_new_resolver("normal_dist", lambda *args: NormalDistGenerator(*args).getValue())
OmegaConf.register_new_resolver("normal_dist_range", lambda *args: NormalDistRangeGenerator(*args).getValue())
OmegaConf.register_new_resolver("normal_truncated", normal_truncated)


def load_config(filename, saveAs=None):
    """Load configuration from a file. By default, attributes are interpolated at access time.

    Args:
        filename ([type]): an input template config file
        saveAs ([type], optional): When specified, interpolate all attributes and persist to a file. Defaults to None, meaning that attributes will be interpolated at access time.

    Returns:
        [type]: [description]
    """
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
    
def __exec_virtuoso_command(cmd, compose_file, service_name, batch_id):
    container_name = get_docker_containers(compose_file, service_name)[batch_id]
    os.system(f"docker exec {container_name} /opt/virtuoso-opensource/bin/isql \"EXEC={cmd};\"")
    
def virtuoso_kill_all_transactions(compose_file, service_name, batch_id):
    __exec_virtuoso_command("txn_killall(6)", compose_file, service_name, batch_id)
    
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

def wait_for_container(endpoints, outfile, logger, wait=1):
    if isinstance(endpoints, str):
        endpoints = [ endpoints ]
    endpoint_ok = 0
    attempt=1
    logger.info(f"Waiting for all endpoints...")
    while(endpoint_ok < len(endpoints)):
        logger.info(f"Attempt {attempt} ...")
        try:
            for endpoint in endpoints:
                status = ping(endpoint)
                if status == 200:
                    logger.info(f"{endpoint} is ready!")
                    endpoint_ok += 1   
        except: pass
        attempt += 1
        time.sleep(wait)

    with open(f"{outfile}", "w") as f:
        f.write("OK")

def activate_one_container(batch_id, compose_file, service_name, logger, status_file, deploy_if_not_exists=False):
    """Activate one container while stopping all others

    Args:
        batch_id (_type_): _description_
        sparql_compose_file (_type_): _description_
        sparql_service_name (_type_): _description_
        logger (_type_): _description_
        status_file (_type_): _description_
        deploy_if_not_exists (bool, optional): _description_. Defaults to False.

    Raises:
        RuntimeError: _description_

    Returns:
        boolean: whether or not the container is re-initialized
    """
    
    try:
        containers = get_docker_containers(compose_file, service_name)
        batch_id = int(batch_id)
        container_name = containers[batch_id] if "virtuoso" in service_name else containers[0]
    except:
        return False

    if (container_status := check_container_status(compose_file, service_name, container_name)) is None:
        # if deploy_if_not_exists:
        #     deploy_virtuoso(N_BATCH, container_infos_file, sparql_compose_file, sparql_service_name, restart=True)
        # else:
        raise RuntimeError(f"Container {container_name} does not exists!")

    if container_status != "running":
        logger.info("Stopping all containers...")
        os.system(f"docker-compose -f {compose_file} stop {service_name}")
            
        logger.info(f"Starting container {container_name}...")
        os.system(f"docker start {container_name}")
        container_endpoint = get_docker_endpoint_by_container_name(compose_file, service_name, container_name)
        wait_for_container(container_endpoint, status_file, logger , wait=1)
        return True
    return False

def deploy_virtuoso(n_batch, container_infos_file, sparql_compose_file, sparql_service_name, restart=False):
    if restart:
        os.system(f"docker-compose -f {sparql_compose_file} down --remove-orphans")
        os.system("docker volume prune --force")
        time.sleep(2)
    # os.system(f"docker-compose -f {sparql_compose_file} up -d --scale {sparql_service_name}={N_BATCH}")
    # wait_for_container(CONFIG["virtuoso"]["endpoints"], f"{BENCH_DIR}/virtuoso-up.txt", wait=1)
    
    os.system(f"docker-compose -f {sparql_compose_file} create --no-recreate --scale {sparql_service_name}={n_batch} {sparql_service_name}") # For docker-compose version > 2.15.1
    
    pd.DataFrame(get_docker_containers(sparql_compose_file, sparql_service_name), columns=["Name"]).to_csv(str(container_infos_file), index=False)

    os.system(f"docker-compose -f {sparql_compose_file} stop {sparql_service_name}")
    return container_infos_file