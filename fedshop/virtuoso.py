"""Virtuoso-to-Graphs proxy module.

This module contains the utility functions to create a proxy between each named graph in virtuoso and the client.

- Once the data is ingested into the virtuoso, the data is stored in named graphs. 
- In virtuoso, the default graph includes all named graphs.
- By updating [DB.DBA.SYS_SPARQL_HOST](https://docs.openlinksw.com/virtuoso/rdfperfindexes/), we expose each named graph to a seperate host:
    SH_HOSTVARCHAR | SH_GRAPH_URIVARCHAR
    localhost:8895 | http://ex.org/swdf
    localhost:8896 | http://ex.org/yago

- However, localhost:xxxx is not readily understandable thus hampers the development process.
- This proxy aims to map SH_GRAPH_URIVARCHAR to SH_HOSTVARCHAR, e.g http://ex.org/swdf -> localhost:8895/sparql
"""

from io import StringIO
import os
import re
import subprocess
import click
import numpy as np
import pandas as pd
from tqdm import tqdm

from .utils import LOGGER

@click.group
def cli():
    pass

@cli.command()
@click.option("--container-name", type=click.STRING)
@click.option("--isql", type=click.STRING, default="/opt/virtuoso-opensource/bin/isql")
@click.option("--exec", type=click.STRING)
@click.option("--return_output", is_flag=True, default=False)
def isql_exec(container_name, isql, exec, return_output):
    if not isql.startswith('"'):
        isql = f'"{isql}"'
    cmd = f'{isql} "EXEC={exec}"'
    if container_name:
        cmd = f'docker exec {container_name} {isql} "EXEC={exec}"'
    
    #click.echo (f"Executing command: {cmd}")
    proc = subprocess.run(cmd, shell=True, check=True, stdout=subprocess.PIPE)
    if return_output:
        return proc.stdout.decode("utf-8")

@cli.command
@click.option("--container-name", type=click.STRING)
@click.option("--isql", type=click.STRING, default="/opt/virtuoso-opensource/bin/isql")
@click.option("--graph-uri", type=click.STRING, help="The URI of the graph.")
@click.pass_context
def retrieve_sparql_host(ctx: click.Context, container_name, isql, graph_uri):
    """Retrieve the SPARQL host for the given graph URI.

    Args:
        graph_uri (str): The URI of the graph.

    Returns:
        str: The host of the graph.
    """
    click.echo(f"Retrieving SPARQL host for graph {graph_uri}.")

    sql = "SELECT SH_HOST, SH_GRAPH_URI FROM DB.DBA.SYS_SPARQL_HOST;"
    if graph_uri:
        sql = f"SELECT SH_HOST, SH_GRAPH_URI FROM DB.DBA.SYS_SPARQL_HOST WHERE SH_GRAPH_URI = '{graph_uri}';"

    output = ctx.invoke(isql_exec, container_name=container_name, isql=isql, exec=sql, return_output=True)
    output = re.search(r".*(SH_HOST[\W\w\s]*\s+)(\d+) Rows\.", output).group(1)

    mapping = {}
    with StringIO(output) as f:
        lines = f.readlines()
        for line in lines:
            line = line.strip()
            if line.startswith(("SH_HOST", "VARCHAR", "_")) or len(line) == 0:
                continue
            
            sh_host, sh_graph = re.split(r"\s+", line)
            print(sh_host, sh_graph)
            mapping[sh_graph] = sh_host
    
    print(mapping)
    return mapping

@cli.command
@click.option("--container-name", type=click.STRING)
@click.option("--isql", type=click.STRING, default="/opt/virtuoso-opensource/bin/isql")
@click.option("--graph-uri", type=click.STRING, help="The URI of the graph.")
@click.pass_context
def retrieve_sparql_endpoints(ctx: click.Context, container_name, isql, graph_uri):
    """Retrieve the SPARQL host for the given graph URI.

    Args:
        graph_uri (str): The URI of the graph.

    Returns:
        str: The host of the graph.
    """

    host_filter = "1"
    if graph_uri:
        mapping = ctx.invoke(retrieve_sparql_host, container_name=container_name, isql=isql, graph_uri=graph_uri)
        host = mapping[graph_uri]
        sh_host, sh_port = host.split(":")
        host_filter = f"HP_HOST = '{sh_host}' AND HP_LISTEN_HOST = ':{sh_port}'"

    click.echo(f"Retrieving SPARQL endpoints for graph {graph_uri}.")

    sql = f"SELECT HP_HOST, HP_LISTEN_HOST FROM DB.DBA.HTTP_PATH WHERE HP_PPATH = '/!sparql/' AND {host_filter};"
    
    output = ctx.invoke(isql_exec, container_name=container_name, isql=isql, exec=sql, return_output=True)
    output = re.search(r".*(HP_HOST[\W\w\s]*\s+)(\d+) Rows\.", output).group(1)

    records = []
    with StringIO(output) as f:
        lines = f.readlines()
        for line in lines:
            line = line.strip()
            if line.startswith(("HP_HOST", "VARCHAR", "_")) or len(line) == 0:
                print(line)
                continue
            
            hp_host, hp_listen_host = re.split(r"\s+", line)
            mapping = { "hp_host": hp_host, "hp_listen_host": hp_listen_host }
            records.append(mapping)
    
    df = pd.DataFrame.from_records(records)
    return df

@cli.command()
@click.option("--container-name", type=click.STRING)
@click.option("--isql", type=click.STRING, default="/opt/virtuoso-opensource/bin/isql")
@click.argument("graph-uri", type=click.STRING)
@click.argument("host", type=click.STRING)
@click.option("--on-duplicate", type=click.Choice(["IGNORE", "REPLACE"]))
@click.pass_context
def update_sparql_host(ctx: click.Context, container_name, isql, graph_uri, host, on_duplicate):
    """Update the SPARQL host for the given graph URI.

    Args:
        graph_uri (str): The URI of the graph.
        host (str): The host of the graph.
    """
    click.echo(f"Updating SPARQL host for graph {graph_uri} to {host}.")
    
    insert_mode = "INTO"
    if on_duplicate:
        insert_mode = "REPLACING" if on_duplicate == "REPLACE" else "SOFT"

    exec_cmd = f"INSERT {insert_mode} DB.DBA.SYS_SPARQL_HOST (SH_HOST, SH_GRAPH_URI) VALUES (\'{host}\', \'{graph_uri}\');"
    ctx.invoke(isql_exec, container_name=container_name, isql=isql, exec=exec_cmd)

@cli.command()
@click.option("--container-name", type=click.STRING)
@click.option("--isql", type=click.STRING, default="/opt/virtuoso-opensource/bin/isql")
@click.option("--graph-uri", type=click.STRING)
@click.option("--host", type=click.STRING)
@click.pass_context
def remove_sparql_host(ctx: click.Context, container_name, isql, graph_uri, host):
    """Update the SPARQL host for the given graph URI.

    Args:
        graph_uri (str): The URI of the graph.
        host (str): The host of the graph.
    """

    delete_condition = "1" # Delete all
    if graph_uri and host:
        click.echo(f"Deleting SPARQL host for graph {graph_uri} to {host}.")
        delete_condition = f"SH_HOST = \'{host}\' and SH_GRAPH_URI = \'{graph_uri}\'"
    elif graph_uri:
        click.echo(f"Deleting SPARQL host for graph {graph_uri}.")
        delete_condition = f"SH_GRAPH_URI = \'{graph_uri}\'"
    elif host:
        click.echo(f"Deleting SPARQL host for host {host}.")
        delete_condition = f"SH_HOST = \'{host}\'"

    exec_cmd = f"DELETE FROM DB.DBA.SYS_SPARQL_HOST WHERE {delete_condition} ;"
    ctx.invoke(isql_exec, container_name=container_name, isql=isql, exec=exec_cmd)

@cli.command()
@click.option("--container-name", type=click.STRING)
@click.option("--isql", type=click.STRING, default="/opt/virtuoso-opensource/bin/isql")
@click.argument("vhost", type=click.STRING)
@click.argument("lhost", type=click.STRING)
@click.argument("lpath", type=click.STRING)
@click.pass_context
def remove_sparql_endpoint(ctx: click.Context, container_name, isql, vhost, lhost, lpath):
    exec_cmd = f"DB.DBA.VHOST_REMOVE(vhost=>\'{vhost}\', lhost=>\'{lhost}\', lpath=>\'{lpath}\') ;"
    ctx.invoke(isql_exec, container_name=container_name, isql=isql, exec=exec_cmd)

    # Remove from DB.DBA.SYS_SPARQL_HOST
    exec_cmd = f"DELETE FROM DB.DBA.SYS_SPARQL_HOST WHERE SH_HOST = \'{vhost}:{lhost}\' ;"
    ctx.invoke(isql_exec, container_name=container_name, isql=isql, exec=exec_cmd)
    
@cli.command()
@click.option("--container-name", type=click.STRING)
@click.option("--isql", type=click.STRING, default="/opt/virtuoso-opensource/bin/isql")
@click.option("--host", type=click.STRING, default="*ini*:*ini*")
@click.argument("graph-uri", type=click.STRING)
@click.option("--lpath", type=click.STRING, default="/sparql")
@click.option("--on-duplicate", type=click.Choice(["IGNORE", "REPLACE"]))
@click.pass_context
def create_sparql_endpoint(ctx: click.Context, container_name, isql, host, graph_uri, lpath, on_duplicate):
    vhost, vport = host.split(":")
    lhost = f":{vport}"

    ctx.invoke(remove_sparql_endpoint, container_name=container_name, isql=isql, vhost=vhost, lhost=lhost, lpath=lpath)
    
    exec_cmd = f"DB.DBA.VHOST_DEFINE(vhost=>\'{vhost}\', lhost=>\'{lhost}\', lpath=>\'{lpath}\', ppath=>\'/!sparql/\', is_dav=>1, vsp_user=>\'dba\',opts=>vector (\'browse_sheet\', \'\', \'noinherit\', \'yes\')) ;"
    #exec_cmd = f"DB.DBA.VHOST_DEFINE(lpath=>\'{lpath}\', ppath=>\'/!sparql/\', is_dav=>1, vsp_user=>\'dba\',opts=>vector (\'browse_sheet\', \'\', \'noinherit\', \'yes\')) ;"
    ctx.invoke(isql_exec, container_name=container_name, isql=isql, exec=exec_cmd)

    #ctx.invoke(remove_sparql_host, container_name=container_name, isql=isql, graph_uri=graph_uri, host=host)
    if vhost == "*ini*": vhost = "localhost"
    if vport == "*ini*": vport = "8890"
    sh_host = f"{vhost}:{vport}" # e.g localhost:8890/vendor0/sparql

    ctx.invoke(update_sparql_host, container_name=container_name, isql=isql, graph_uri=graph_uri, host=sh_host, on_duplicate=on_duplicate)

@cli.command()
@click.option("--container-name", type=click.STRING)
@click.option("--isql", type=click.STRING, default="/opt/virtuoso-opensource/bin/isql")
@click.option("--drop-first", is_flag=True, default=False)
@click.argument("graph-uri", type=click.STRING)
@click.pass_context
def create_graph_group(ctx: click.Context, container_name, isql, drop_first, graph_uri):
    """
    Create a graph group in Virtuoso. 

    Args:
        ctx (click.Context): The Click context.
        container_name: The name of the container.
        isql: The isql command.
        drop_first: A boolean indicating whether to drop the graph group first.
        graph_uri: The URI of the graph group.

    Returns:
        None
    """
    if drop_first:
        ctx.invoke(drop_graph_group, container_name=container_name, isql=isql, graph_uri=graph_uri)

    # Create the graph group
    exec_cmd = f"DB.DBA.RDF_GRAPH_GROUP_CREATE(group_iri=>\'{graph_uri}\', quiet=>1) ;"
    ctx.invoke(isql_exec, container_name=container_name, isql=isql, exec=exec_cmd)

    # Enable unauthenticated users to access the graph
    # bit mask: 1 (read) + 8 (access to group's members) = 9
    exec_cmd = f"DB.DBA.RDF_GRAPH_USER_PERMS_SET (\'{graph_uri}\', \'nobody\', 9);"
    ctx.invoke(isql_exec, container_name=container_name, isql=isql, exec=exec_cmd)

@cli.command()
@click.option("--container-name", type=click.STRING)
@click.option("--isql", type=click.STRING, default="/opt/virtuoso-opensource/bin/isql")
@click.argument("graph-uri", type=click.STRING)
@click.pass_context
def drop_graph_group(ctx: click.Context, container_name, isql, graph_uri):
    exec_cmd = f"DB.DBA.RDF_GRAPH_GROUP_DROP(group_iri=>\'{graph_uri}\', quiet=>1) ;"
    ctx.invoke(isql_exec, container_name=container_name, isql=isql, exec=exec_cmd)

@cli.command()
@click.argument("action", type=click.Choice(["INS", "DEL", "GET"]))
@click.option("--container-name", type=click.STRING)
@click.option("--isql", type=click.STRING, default="/opt/virtuoso-opensource/bin/isql") 
@click.option("--graph-group", type=click.STRING)
@click.option("--member-iri", type=click.STRING)
@click.pass_context
def update_graph_group(ctx: click.Context, action, container_name, isql, graph_group, member_iri):
    exec_cmd = ""
    if action in ["INS", "DEL"]:
        exec_cmd = f"DB.DBA.RDF_GRAPH_GROUP_{action}(group_iri=>\'{graph_group}\', memb_iri=>\'{member_iri}\') ;"
    elif action == "GET":
        if graph_group and member_iri:
            exec_cmd = f"SELECT * FROM DB.DBA.RDF_GRAPH_GROUP_MEMBER WHERE RGGM_GROUP_IID = \'{graph_group}\' and RGGM_MEMBER_IID = \'{member_iri}\' ;"
        elif graph_group:
            exec_cmd = f"SELECT * FROM DB.DBA.RDF_GRAPH_GROUP_MEMBER WHERE RGGM_GROUP_IID = \'{graph_group}\' ;"
        elif member_iri:
            exec_cmd = f"SELECT * FROM DB.DBA.RDF_GRAPH_GROUP_MEMBER WHERE RGGM_MEMBER_IID = \'{member_iri}\' ;"
        else:
            exec_cmd = f"SELECT * FROM DB.DBA.RDF_GRAPH_GROUP_MEMBER ;"

    ctx.invoke(isql_exec, container_name=container_name, isql=isql, exec=exec_cmd)

@cli.command()
@click.option("--container-name", type=click.STRING)
@click.option("--isql", type=click.STRING, default="/opt/virtuoso-opensource/bin/isql")
@click.option("--datapath", type=click.STRING, default="/usr/share/proj/")
@click.option("--datafiles", type=click.STRING, default="*.nq")
@click.pass_context
def ingest_data(ctx: click.Context, container_name, isql, datapath, datafiles):
    """
    Ingests data into the Virtuoso RDF store.

    Args:
        ctx (click.Context): The Click context object.
        container_name (str): The name of the Virtuoso container.
        isql (str): The path to the isql executable.
        datapath (str): The path to the directory containing the data files.

    Returns:
        None
    """
    datafiles = [ datafile.strip() for datafile in datafiles.split(",") ]
    
    # Grant permissions to the SPARQL user
    ctx.invoke(isql_exec, container_name=container_name, isql=isql, exec='grant select on \\\"DB.DBA.SPARQL_SINV_2\\\" to \\\"SPARQL\\\";')
    ctx.invoke(isql_exec, container_name=container_name, isql=isql, exec='grant execute on \\\"DB.DBA.SPARQL_SINV_IMP\\\" to \\\"SPARQL\\\";')

    # Load the data
    for datafile in tqdm(datafiles):
        ctx.invoke(isql_exec, container_name=container_name, isql=isql, exec=f"ld_dir('{datapath}', '{datafile}', 'http://example.com/datasets/default');")    
    
    # Launch the ingest process and checkpoint
    ctx.invoke(isql_exec, container_name=container_name, isql=isql, exec=f"rdf_loader_run(log_enable=>2);")
    ctx.invoke(isql_exec, container_name=container_name, isql=isql, exec=f"checkpoint;")

@cli.command()
@click.option("--container-name", type=click.STRING)
@click.option("--isql", type=click.STRING, default="/opt/virtuoso-opensource/bin/isql")
@click.pass_context
def virtuoso_kill_all_transactions(ctx: click.Context, container_name, isql):
    click.echo("Killing all transactions in Virtuoso.")
    ctx.invoke(isql_exec, container_name=container_name, isql=isql, exec="txn_killall(6)")
    
if __name__ == "__main__":
    cli()