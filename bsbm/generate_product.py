import sys
import os
import re
from omegaconf import OmegaConf as yaml

wd = os.path.dirname(os.path.realpath(__file__))
config = yaml.load(os.path.join(wd, "config.yaml"))
model = os.path.join(wd, "model")

print(f"Generate products...")
product_out = open(os.path.join(model, config["product"]["template"]), "r").read()

# Replace all params in template
for param, value in config["product"]["params"].items():
    product_out = re.sub(re.escape("{%" + param + "}"), str(value), product_out)

# Write to file
product_outFile = os.path.join(model, "products.txt.tmp")
with open(product_outFile, "w") as productFile:
    productFile.write(product_out)
    productFile.close()

product_scale_factor = config["product"]["scale_factor"]
os.system(f"watdiv -d {product_outFile} {product_scale_factor} > {os.path.join(model, 'products.nt.tmp')}")
os.remove(os.path.join(model, "products.txt.tmp"))
#os.system(f"python utils/to_quad.py {os.path.join(model, 'products.nt.tmp')} > {os.path.join(model, 'products.nq.tmp')}")