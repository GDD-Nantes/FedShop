import json
import numpy as np
from scipy.stats import norm
from omegaconf import OmegaConf
import psutil
from pathlib import Path
import os
import tempfile

class NormalDistrGenerator:
    def __init__(self, mu, sigma, avg) -> None:
        self._avg = avg
        self._mu = mu
        self._sigma = sigma
    
    def getValue(self) -> int:
        randVal: float = norm.ppf(np.random.rand(), loc=self._mu, scale=self._sigma)

        while randVal < 0:
            randVal = norm.ppf(np.random.rand(), loc=self._mu, scale=self._sigma)
        
        return int(((randVal / self._mu) * self._avg) + 1);

class NormalDistrRangeGenerator:
    def __init__(self, mu, sigma, maxValue, normalLimit) -> None:
        self._mu = mu
        self._sigma = sigma
        self._maxValue = maxValue
        self._normalLimit = normalLimit
    
    def getValue(self) -> int:
        randVal: float = norm.ppf(np.random.rand(), loc=self._mu, scale=self._sigma)

        while randVal > self._normalLimit or randVal < 0:
            randVal = norm.ppf(np.random.rand(), loc=self._mu, scale=self._sigma)
        
        return int(((randVal / self._normalLimit) * self._maxValue) + 1);

def divide(*args):
    if len(args) != 2:
        raise ValueError(f"The number of arguments must not exceed 2. Args: {args}")
    
    return int(args[0] / args[1])

OmegaConf.register_new_resolver("multiply", lambda *args: np.prod(args).item())
OmegaConf.register_new_resolver("sum", lambda *args: np.sum(args).item())
OmegaConf.register_new_resolver("divide", divide)
OmegaConf.register_new_resolver("product_type_per_product", lambda nbProd: int(4**np.log10(nbProd).item()))
OmegaConf.register_new_resolver("normal_dist", lambda *args: NormalDistrGenerator(*args).getValue())
OmegaConf.register_new_resolver("normal_dist_range", lambda *args: NormalDistrRangeGenerator(*args).getValue())


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
        config = OmegaConf.to_object(config)
        with open(saveAs, "w+") as tmpfile:
            OmegaConf.save(config, tmpfile)

    return config

def kill_process(proc_pid):
    process = psutil.Process(proc_pid)
    for child in process.children(recursive=True):
        child.kill()
    process.kill()