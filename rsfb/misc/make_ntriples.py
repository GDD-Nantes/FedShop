import glob
import re
from pathlib import Path
from tqdm import tqdm

for nq_file in tqdm(glob.glob("experiments/bsbm/model/dataset/*.nq")):
    with open(nq_file) as nq_fs, open(Path(nq_file).with_suffix('.nt'), "w") as nt_fs:
        for line in nq_fs.readlines():
            s, p, o, src = re.split(r"\t", line.strip())
            src, punc = re.split(r"\s+", src)
            nt_fs.write("\t".join([s, p, o, punc]) + "\n")