import os
from pathlib import Path
import re
import click
import subprocess
from utils import load_config, kill_process
import psutil

@click.group
def cli():
    pass

@cli.command
@click.argument("configfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("section", type=click.STRING)
@click.argument("output", type=click.Path(file_okay=True, dir_okay=True))
@click.option("--id", type=click.INT, default=0)
def generate(configfile, section, output, id):

    output_base = f"{Path(output).parent}/{section}{id}"

    config = load_config(configfile, saveAs=f"{output_base}.yaml")["generation"]
    schema_config = config["schema"]

    template = open(schema_config[section]["template"], "r").read()
    # Replace all params in template
    params: dict = schema_config[section]["params"]
    if params is not None:
        for param, value in params.items():
            #if param == f"{section}_n": continue
            template = re.sub(re.escape(f"{{%{param}}}"), str(value), template)

    model_file = f"{output_base}.txt.tmp"
    Path(model_file).parent.mkdir(parents=True, exist_ok=True)
    with open(model_file, "w") as outWriter:
        out = re.sub(re.escape("{%provenance}"), schema_config[section]["provenance"], template)
        out = re.sub(re.escape(f"{{%{section}_id}}"), f"{section}{id}", out)
        out = re.sub(re.escape("{%export_output_dir}"), schema_config[section]["export_output_dir"], out)
        if schema_config[section].get("export_dep_output_dir") is not None:
            out = re.sub(re.escape("{%export_dep_output_dir}"), schema_config[section]["export_dep_output_dir"], out)
        outWriter.write(out)
        outWriter.close()

    scale_factor = int(schema_config[section]["scale_factor"])

    cmd = f"{config['generator']['exec']} -d {model_file} {scale_factor}"
    
    # This consumes memory since it waits till the end and store the output in PIPE
    watdiv_proc = subprocess.run(cmd, capture_output=False, shell=True)
    # watdiv_proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

    # verbose = config["verbose"]
    # with open(output, "w") as watdivWriter:
    #     for line in iter(watdiv_proc.stdout.readline, b''):
    #         watdivWriter.write(line.decode())
    #     watdivWriter.close()        
    #     if not verbose: os.remove(model_file)    
    # watdiv_proc.wait()

    if watdiv_proc.returncode != 0:
        raise RuntimeError(watdiv_proc.stderr.read().decode())  

    # try: kill_process(watdiv_proc.pid)  
    # except:
    #     print(f"watdiv proc (PID: {watdiv_proc.pid}) is already killed, skipping...")

if __name__ == "__main__":
    cli()