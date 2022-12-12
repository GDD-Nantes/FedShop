import os
from pathlib import Path
import re
from omegaconf import OmegaConf as yaml
import click
import subprocess
from config import load_config

@click.group
def cli():
    pass

@cli.command
@click.argument("configfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("section", type=click.STRING)
@click.argument("output", type=click.Path(file_okay=True, dir_okay=False))
@click.option("--id", type=click.INT, default=0)
def generate(configfile, section, output, id):
    config = load_config(configfile)["generation"]
    schema_config = config["schema"]

    template = open(schema_config[section]["template"], "r").read()
    # Replace all params in template
    params: dict = schema_config[section]["params"]
    if params is not None:
        for param, value in params.items():
            #if param == f"{section}_n": continue
            template = re.sub(re.escape(f"{{%{param}}}"), str(value), template)

    outFile = os.path.join(Path(output).parent, f"{section}{id}.txt.tmp")
    Path(outFile).parent.mkdir(parents=True, exist_ok=True)
    with open(outFile, "w") as outWriter:
        out = re.sub(re.escape(f"{{%{section}_id}}"), str(id), template)
        outWriter.write(out)
        outWriter.close()

    scale_factor = int(schema_config[section]["scale_factor"])
    
    watdiv_proc = subprocess.run(f"{config['generator']['exec']} -d {outFile} {scale_factor}", capture_output=True, shell=True)
    
    if watdiv_proc.returncode != 0:
        raise RuntimeError(watdiv_proc.stderr.decode())
    
    verbose = config["verbose"]
    with open(output, "w") as watdivWriter:
        watdivWriter.write(watdiv_proc.stdout.decode())
        watdivWriter.close()        
        if not verbose: os.remove(outFile)        
if __name__ == "__main__":
    cli()