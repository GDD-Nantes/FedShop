# Import part

import click
import glob

# Example of use : 
# python3 utils/generate-fedx-config-file.py bsbm/model/vendor test/out.ttl

# Goal : Generate a configuration file for RDF4J to set the use of named graph as endpoint thanks to data file

@click.command()
@click.argument("dir_data_file")
@click.argument("config_file")
@click.option("--endpoint", type=str, default="http://localhost:8890/sparql/", help="URL to a SPARQL endpoint")

def generate_fedx_config_file(dir_data_file, config_file, endpoint):
    ssite = set()
    for data_file in glob.glob(f'{dir_data_file}/*.nq'):
        with open(data_file) as file:
            t_file = file.readlines()
            for line in t_file:
                site = line.split()[-1]
                site = site.replace("<", "")
                site = site.replace(">.", "")
                ssite.add(site)
    
    with open(f'{config_file}', 'a') as ffile:
        ffile.write(
"""
@prefix sd: <http://www.w3.org/ns/sparql-service-description#> .
@prefix fedx: <http://rdf4j.org/config/federation#> .

"""
        )
        for s in ssite:
            ffile.write(
f"""
<{s}> a sd:Service ;
    fedx:store "SPARQLEndpoint";
    sd:endpoint "{endpoint}?default-graph-uri={s}";
    fedx:supportsASKQueries false .   

"""
            )

if __name__ == "__main__":
    generate_fedx_config_file()