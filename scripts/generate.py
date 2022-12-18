import os
from pathlib import Path
import re
import click
import subprocess
from scripts.utils import load_config, kill_process
import psutil

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
    
    # This consumes memory since it waits till the end and store the output in PIPE
    # watdiv_proc = subprocess.run(f"{config['generator']['exec']} -d {outFile} {scale_factor}", capture_output=True, shell=True)
    cmd = f"{config['generator']['exec']} -d {outFile} {scale_factor}"
    watdiv_proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    
    verbose = config["verbose"]
    with open(output, "w") as watdivWriter:
        for line in iter(watdiv_proc.stdout.readline, b''):
            watdivWriter.write(line.decode())
        watdivWriter.close()        
        if not verbose: os.remove(outFile)    

    watdiv_proc.wait()
    if watdiv_proc.returncode != 0:
        raise RuntimeError(watdiv_proc.stderr.read().decode())  

    kill_process(watdiv_proc.pid)  

if __name__ == "__main__":
    cli()