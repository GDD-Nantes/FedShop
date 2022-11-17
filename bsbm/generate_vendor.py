import sys
import os
import re
from omegaconf import OmegaConf as yaml

wd = os.path.dirname(os.path.realpath(__file__))
config = yaml.load(os.path.join(wd, "config.yaml"))

vendor_id = int(sys.argv[1])
model = os.path.join(wd, "model")

vendor_template = open(os.path.join(model, config["vendor"]["template"]), "r").read()
# Replace all params in template
for param, value in config["vendor"]["params"].items():
    vendor_template = re.sub(re.escape("{%" + param + "}"), str(value), vendor_template)

# Generate n vendor
vendor_model = f"Vendor{vendor_id}"
vendor_outFile = os.path.join(model, f"{vendor_model}.txt.tmp")
with open(vendor_outFile, "w") as vendorFile:
    vendor_out = re.sub(re.escape("{%vendor_id}"), str(vendor_id), vendor_template)
    vendorFile.write(vendor_out)
    vendorFile.close()

vendor_scale_factor = config["vendor"]["scale_factor"]
os.system(f"watdiv -d {vendor_outFile} {vendor_scale_factor} > {model}/{vendor_model}.nt.tmp")
os.remove(vendor_outFile)
#os.system(f"python utils/to_quad.py {model}/{vendor_model}.nt.tmp > {model}/{vendor_model}.nq.tmp")