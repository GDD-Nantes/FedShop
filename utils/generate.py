import os
from pathlib import Path
import re
from omegaconf import OmegaConf as yaml
import click
import subprocess

@click.group
def cli():
    pass

@cli.command
@click.argument("config", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("section", type=click.STRING)
@click.argument("template", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("output", type=click.Path(file_okay=True, dir_okay=False))
@click.argument("id", type=click.INT)
@click.option("--verbose", type=click.BOOL, default=False)
def generate(config, section, template, output, id, verbose):
    config = yaml.load(config)
    template = open(template, "r").read()
    # Replace all params in template
    for param, value in config[section]["params"].items():
        if param == f"{section}_n": continue
        template = re.sub(re.escape(f"{{%{param}}}"), str(value), template)

    # Generate n vendor
    outFile = os.path.join(Path(output).parent, f"{section}{id}.txt.tmp")
    Path(outFile).parent.mkdir(parents=True, exist_ok=True)
    with open(outFile, "w") as outWriter:
        out = re.sub(re.escape(f"{{%{section}_id}}"), str(id), template)
        outWriter.write(out)
        outWriter.close()

    scale_factor = config["vendor"]["scale_factor"]
    
    watdiv_proc = subprocess.run(f"{config['watdiv']['exec']} -d {outFile} {scale_factor}", capture_output=True, shell=True)
    
    if watdiv_proc.returncode != 0:
        raise RuntimeError(watdiv_proc.stderr.decode())
    
    with open(output, "w") as watdivWriter:
        watdivWriter.write(watdiv_proc.stdout.decode())
        watdivWriter.close()        
        if not verbose: os.remove(outFile)        
    
if __name__ == "__main__":
    cli()