# Import part
import json
import os
import re
import shutil
import click
import glob
import subprocess
import pandas as pd
import numpy as np
from pathlib import Path
import csv
from itertools import zip_longest

import sys

from sklearn.calibration import LabelEncoder
sys.path.append(str(os.path.join(Path(__file__).parent.parent)))

from utils import check_container_status, kill_process, load_config, rsfb_logger, write_empty_result, create_stats
import fedx

logger = rsfb_logger(Path(__file__).name)
# Example of use : 
# python3 utils/generate-fedx-config-file.py experiments/bsbm/model/vendor test/out.ttl

# Goal : Generate a configuration file for RDF4J to set the use of named graph as endpoint thanks to data file

@click.group
def cli():
    pass

@cli.command()
@click.argument("eval-config", type=click.Path(exists=True, file_okay=True, dir_okay=True))
@click.pass_context
def prerequisites(ctx: click.Context, eval_config):
    """Obtain prerequisite artifact for engine, e.g, compile binaries, setup dependencies, etc.

    Args:
        eval_config (_type_): _description_
    """
    
    app_config = load_config(eval_config)["evaluation"]["engines"]["splendid"]
    app = app_config["dir"]
    
    #if not os.path.exists(app) or not os.path.exists(jar) or os.path.exists(lib):
    oldcwd = os.getcwd()
    os.chdir(Path(app))
    os.system("./SPLENDID.sh ignore ignore ignore ignore ignore ignore ignore ignore true false false")
    os.chdir(oldcwd)

@cli.command()
@click.argument("eval-config", type=click.Path(exists=True, file_okay=True, dir_okay=True))
@click.argument("engine-config", type=click.Path(exists=True, file_okay=True, dir_okay=True))
@click.argument("query", type=click.Path(exists=True, file_okay=True, dir_okay=True))
@click.option("--out-result", type=click.Path(exists=False, file_okay=True, dir_okay=True), default="/dev/null")
@click.option("--out-source-selection", type=click.Path(exists=False, file_okay=True, dir_okay=True), default="/dev/null")
@click.option("--query-plan", type=click.Path(exists=False, file_okay=True, dir_okay=True), default="/dev/null")
@click.option("--stats", type=click.Path(exists=False, file_okay=True, dir_okay=True), default="/dev/null")
@click.option("--force-source-selection", type=click.Path(exists=False, file_okay=True, dir_okay=True), default="")
@click.option("--batch-id", type=click.INT, default=-1)
@click.option("--noexec", is_flag=True, default=False)
@click.pass_context
def run_benchmark(ctx: click.Context, eval_config, engine_config, query, out_result, out_source_selection, query_plan, stats, force_source_selection, batch_id, noexec):

    # SPLENDID run part

    config = load_config(eval_config)
    app_config = config["evaluation"]["engines"]["splendid"]
    app = app_config["dir"]
    last_batch = config["generation"]["n_batch"] - 1
    endpoint = config["generation"]["virtuoso"]["endpoints"][last_batch]
    compose_file = config["generation"]["virtuoso"]["compose_file"]
    service_name = config["generation"]["virtuoso"]["service_name"]
    container_name = config["generation"]["virtuoso"]["container_names"][last_batch]
    timeout = int(config["evaluation"]["timeout"])
    #properties = app_config["properties"]
    properties = f"{app}/eval/sail-config/config.properties"
    void_conf = f"eval/sail-config/config.n3"
    #void_conf = str(Path(engine_config).absolute())
    shutil.copy(engine_config,  f"{app}/eval/sail-config/config.n3")
    http_req = "N/A"

    lines = []
    provenance_stat_to_modif = f"{query.split('/')[4]}-{query.split('/')[5]}.csv"
    with open(properties, "r") as properties_file:
        lines = properties_file.readlines()
        for i in range(len(lines)):
            if lines[i].startswith("query.directory"):
                lines[i] = re.sub(r'query\.directory=.+','query.directory='+str(os.getcwd())+'/'+str(query).split("injected.sparql")[0][:-1], lines[i])
            elif lines[i].startswith("output.file"):
                lines[i] = re.sub(r'output\.file=.+','output.file='+provenance_stat_to_modif, lines[i])
            elif lines[i].startswith("sparql.endpoint"):
                lines[i] = re.sub(r'sparql\.endpoint=.+','sparql.endpoint='+endpoint, lines[i])
    
    with open(properties, "w") as properties_file:
        properties_file.writelines(lines)

    oldcwd = os.getcwd()
    cmd = f'./SPLENDID.sh {void_conf} {Path(properties).absolute()} {timeout} ../../{out_result} ../../{out_source_selection} ../../{query_plan} ../../{stats} ../../{query} false true true'

    print("=== SPLENDID ===")
    print(cmd)
    print("================")

    #SPLENDID need to have initialized path
    write_empty_result(out_result)
    write_empty_result(out_source_selection)
    write_empty_result(query_plan)
    write_empty_result(stats)

    os.chdir(Path(app))
    splendid_proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    #splendid_output, splendid_error = splendid_proc.communicate()
    os.chdir(oldcwd)
    try: 
        splendid_proc.wait(timeout=timeout)
        if splendid_proc.returncode == 0:
            logger.info(f"{query} benchmarked sucessfully") 

            # Write stats
            logger.info(f"Writing stats to {stats}")
             
            # stats_df = pd.read_csv(stats)
            
            # basicInfos = re.match(r".*/(\w+)/(q\w+)/instance_(\d+)/batch_(\d+)/attempt_(\d+)/stats.csv", stats)
            # queryName = basicInfos.group(2)
            # instance = basicInfos.group(3)
            # batch = basicInfos.group(4)
            # attempt = basicInfos.group(5)
    
            # stats_df = stats_df \
            #     .replace('injected.sparql',str(queryName)) \
            #     .replace('instance_id',str(instance)) \
            #     .replace('batch_id',str(batch)) \
            #     .replace('attempt_id',str(attempt))
                
            # stats_df.to_csv(stats, index=False)
            
            create_stats(stats)
            
            def report_error(reason):
                logger.error(f"{query} yield no results!")
                #write_empty_result(out_result)
                #os.system(f"docker stop {container_name}")
                create_stats(stats, reason)
                #raise RuntimeError(f"{query} yield no results!")
                
            try: 
                results_df = pd.read_csv(out_result).replace("null", None)
                if results_df.empty or os.stat(out_result).st_size == 0: 
                    errorFile = f"{Path(stats).parent}/error.txt"
                    if os.path.exists(errorFile):
                        with open(errorFile, "r") as f:
                            reason = f.read()
                            report_error(reason) 
                    else:
                        report_error("error_runtime")       
            except pd.errors.EmptyDataError:
                report_error("error_runtime")
                        
        else:
            logger.error(f"{query} reported error {splendid_proc.returncode}")    
            #write_empty_result(out_result)
            #write_empty_result(stats)
            errorFile = f"{Path(stats).parent}/error.txt"
            if os.path.exists(errorFile):
                with open(errorFile, "r") as f:
                    reason = f.read()
                    create_stats(stats, reason)
            else:
                create_stats(stats, "error_runtime")
    except subprocess.TimeoutExpired: 
        logger.exception(f"{query} timed out!")
        if (container_status := check_container_status(compose_file, service_name, container_name)) != "running":
            logger.debug(container_status)
            raise RuntimeError("Backend is terminated!")
        logger.info("Writing empty stats...")
        create_stats(stats, "timeout")
        #write_empty_result(out_result)    
    finally:
        #os.system('pkill -9 -f "bin"')
        kill_process(splendid_proc.pid)

@cli.command()
@click.argument("infile", type=click.Path(exists=False, file_okay=True, dir_okay=False))
@click.argument("outfile", type=click.Path(exists=False, file_okay=True, dir_okay=False))
@click.pass_context
def transform_results(ctx: click.Context, infile, outfile):
    shutil.copy(infile, outfile)

@cli.command()
@click.argument("infile", type=click.Path(exists=False, file_okay=True, dir_okay=False))
@click.argument("outfile", type=click.Path(exists=False, file_okay=True, dir_okay=False))
@click.argument("prefix-cache", type=click.Path(exists=False, file_okay=True, dir_okay=False))
@click.pass_context
def transform_provenance(ctx: click.Context, infile, outfile, prefix_cache):
    raw_source_selection = pd.read_csv(infile, sep=";")[["triples", "sources"]]
    
    tp_composition = f"{Path(prefix_cache).parent}/provenance.sparql.comp"
    with    open(tp_composition, "r") as comp_fs,\
            open(prefix_cache, "r") as prefix_cache_fs \
    :
        prefix_cache_dict = json.load(prefix_cache_fs)
        
        comp = { k: " ".join(v) for k, v in json.load(comp_fs).items() }
        inv_comp = {}
        for k,v in comp.items():
            if inv_comp.get(v) is None:
                inv_comp[v] = []
            inv_comp[v].append(k) 
        
        def get_triple_id(x):
            result = re.sub(r"[\[\]]", "", x).strip()
            for prefix, alias in prefix_cache_dict.items():
                result = re.sub(rf"<{re.escape(prefix)}(\w+)>", rf"{alias}:\1", result)
                        
            return inv_comp[result] 
        
        def pad(x, max_length):
            encoder = LabelEncoder()
            encoded = encoder.fit_transform(x)
            result = np.pad(encoded, (0, max_length-len(x)), mode="constant", constant_values=-1)                
            decoded = [ encoder.inverse_transform([item]).item() if item != -1 else "" for item in result ]
            return decoded
        
        raw_source_selection["triples"] = raw_source_selection["triples"].apply(lambda x: re.split(r"\s*,\s*", x))
        raw_source_selection = raw_source_selection.explode("triples")
        raw_source_selection["triples"] = raw_source_selection["triples"].apply(get_triple_id)
        raw_source_selection = raw_source_selection.explode("triples")
        raw_source_selection["tp_number"] = raw_source_selection["triples"].str.replace("tp", "", regex=False).astype(int)
        raw_source_selection.sort_values("tp_number", inplace=True)
        raw_source_selection["sources"] = raw_source_selection["sources"].apply(lambda x: re.split(r"\s*,\s*", re.sub(r"[\[\]]", "",x)))
        
        # If unequal length (as in union, optional), fill with nan
        max_length = raw_source_selection["sources"].apply(len).max()
        raw_source_selection["sources"] = raw_source_selection["sources"].apply(lambda x: pad(x, max_length))
               
        out_df = raw_source_selection.set_index("triples")["sources"] \
            .to_frame().T \
            .apply(pd.Series.explode) \
            .reset_index(drop=True) 
        
        out_df.to_csv(outfile, index=False)
    

@cli.command()
@click.argument("datafiles", type=click.Path(exists=True, dir_okay=False, file_okay=True), nargs=-1)
@click.argument("outfile", type=click.Path(exists=False, file_okay=True, dir_okay=False))
@click.argument("eval-config", type=click.Path(exists=True, dir_okay=False, file_okay=True))
@click.argument("batch_id", type=click.INT)
@click.argument("endpoint", type=str)
@click.pass_context
def generate_config_file(ctx: click.Context, datafiles, outfile, eval_config, batch_id, endpoint):
    splended_config = load_config(eval_config)["evaluation"]["engines"]["splendid"]
    app_dir = splended_config["dir"]
    properties = f"{app_dir}/eval/sail-config/config.properties"
    with open(properties, "r+") as props_fs:
        props = props_fs.read()
        if endpoint not in props:
            props = re.sub(r"(sparql\.endpoint=)(.*)", rf"\1{re.escape(endpoint)}", props)
            props_fs.write(props)
            
    if not os.path.exists(outfile):
        Path(outfile).parent.mkdir(parents=True, exist_ok=True)
        Path(outfile).touch(exist_ok=True)
        
        #with open(str(splended_config["dir"])+'/eval/sail-config/config.n3','w+') as outputfile:
        with open(outfile, 'w') as outputfile:
            outputfile.write("################################################################################\n")
            outputfile.write("# Sesame configuration for SPLENDID Federation.\n")
            outputfile.write("#\n")
            outputfile.write("# ATTENTION: the Sail implementing the sail:sailType must be published\n")
            outputfile.write("#            in META-INF/services/org.eclipse.rdf4j.sail.SailFactory\n")
            outputfile.write("################################################################################\n")
            outputfile.write("@prefix void: <http://rdfs.org/ns/void#>.\n")
            outputfile.write("@prefix rep:  <http://www.openrdf.org/config/repository#>.\n")
            outputfile.write("@prefix sr:   <http://www.openrdf.org/config/repository/sail#>.\n")
            outputfile.write("@prefix sail: <http://www.openrdf.org/config/sail#>.\n")
            outputfile.write("@prefix fed:  <http://west.uni-koblenz.de/config/federation/sail#>.\n")
            # for i in range(len(files)):
            #    outputfile.write("@prefix src"+str(i)+": <http://www."+str(files[i]).split(".")[0]+".fr>.\n")
            outputfile.write("\n")
            outputfile.write("[] a rep:Repository ;\n")
            outputfile.write("\trep:repositoryTitle \"SPLENDID Federation\" ;\n")
            outputfile.write("\trep:repositoryID \"SPLENDID\" ;\n")
            outputfile.write("\trep:repositoryImpl [\n")
            outputfile.write("\t\trep:repositoryType \"openrdf:SailRepository\" ;\n")
            outputfile.write("\t\tsr:sailImpl [\n")
            outputfile.write("\t\t\tsail:sailType \"west:FederationSail\" ;\n")
            outputfile.write("\n")
            outputfile.write("\t\t\tfed:sourceSelection [\n")
            outputfile.write("\t\t\t\tfed:selectorType \"INDEX_ASK\";\n")
            outputfile.write("\t\t\t\tfed:useTypeStats false ;\n")
            outputfile.write("\t\t\t] ;\n")
            outputfile.write("\n")
            outputfile.write("\t\t\tfed:queryOptimization [\n")
            outputfile.write("\t\t\t\tfed:optimizerType \"DYNAMIC_PROGRAMMING\" ;\n")
            outputfile.write("\n")
            outputfile.write("\t\t\t\tfed:cardEstimator \"VOID_PLUS\" ;\n")
            outputfile.write("\n")
            outputfile.write("\t\t\t\tfed:groupBySource true ;\n")
            outputfile.write("\t\t\t\tfed:groupBySameAs false ;\n")
            outputfile.write("\n")
            outputfile.write("\t\t\t\tfed:useBindJoin false ;\n")
            outputfile.write("\t\t\t\tfed:useHashJoin true ;\n")
            outputfile.write("\t\t\t] ;\n")
            outputfile.write("\t\t\tfed:member [\n")
            for i in range(len(datafiles)):
                domain_name = str(datafiles[i]).split("/")[-1].split(".")[0]
                void_file = f"{app_dir}/eval/void/{domain_name}.n3"

                outputfile.write("\t\t\t\trep:repositoryType \"west:VoidRepository\" ;\n")
                outputfile.write(f"\t\t\t\tfed:voidDescription <{Path(void_file).absolute()}> ;\n")
                outputfile.write("\t\t\t\tvoid:sparqlEndpoint <http://www."+domain_name+".fr/>\n")
                if i == len(datafiles)-1:
                    outputfile.write("\t\t\t]\n")
                    outputfile.write("\t\t]\n")
                    outputfile.write("\t] .\n")
                else:
                    outputfile.write("\t\t\t], [\n")

    for file in datafiles:             
        # print("Generate void description for "+str(file)+"...")
        domain_name = str(file).split("/")[-1].split(".")[0]
        void_file = f"{app_dir}/eval/void/{domain_name}.n3"
        if os.path.exists(void_file) and os.stat(void_file).st_size > 0: continue
        
        Path(void_file).parent.mkdir(parents=True, exist_ok=True)
        with open(file) as inputfile:
            lines = inputfile.readlines()
            pred = dict()
            sbj = dict()
            obj = dict()
            type = dict()
            p_list = dict()
            arr_po = dict()
            arr_ps = dict()
            map_pred_type = dict()
            count = 0
            for rawline in lines:
                splittedline = rawline.split()
                if(splittedline[1] not in pred.keys()):
                    pred[str(splittedline[1])]=1
                else:
                    pred[str(splittedline[1])]+=1
                if(splittedline[0] not in sbj.keys()):
                    sbj[str(splittedline[0])]=1
                else:
                    sbj[str(splittedline[0])]+=1
                if(splittedline[2] not in obj.keys()):
                    obj[str(splittedline[2])]=1
                else:
                    obj[str(splittedline[2])]+=1
                if(splittedline[1] == "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"):
                    if(splittedline[2] not in type.keys()):
                        type[str(splittedline[2])]=1
                    else:
                        type[str(splittedline[2])]+=1
                if(splittedline[1] not in p_list.keys()):
                    count+=1
                    p_list[str(splittedline[1])]=count
                if(splittedline[2] in arr_po.keys()):
                    arr_po[str(splittedline[2])]+=","+str(p_list[str(splittedline[1])])
                else:
                    arr_po[str(splittedline[2])]=str(p_list[str(splittedline[1])])
                if(splittedline[0] in arr_ps.keys()):
                    arr_ps[str(splittedline[0])]+=","+str(p_list[str(splittedline[1])])
                else:
                    arr_ps[str(splittedline[0])]=str(p_list[str(splittedline[1])])
            s_stat = dict()
            for no in arr_ps.keys():
                id_list = arr_ps[no].split(',')
                occ = dict()
                for i in range(len(id_list)):
                    if(id_list[i] not in occ.keys()):
                        occ[id_list[i]]=1
                    else:
                        occ[id_list[i]]+=1
                for i in occ.keys():
                    if i not in s_stat.keys():
                        s_stat[i]=1
                    else:
                        s_stat[i]+=1
            o_stat = dict()
            for no in arr_po.keys():
                id_list = arr_po[no].split(',')
                occ = dict()
                for i in range(len(id_list)):
                    if(id_list[i] not in occ.keys()):
                        occ[id_list[i]]=1
                    else:
                        occ[id_list[i]]+=1
                for i in occ.keys():
                    if i not in o_stat.keys():
                        o_stat[i]=1
                    else:
                        o_stat[i]+=1
            for no in pred.keys():
                map_pred_type["P:"+str(no)]=str(pred[no])+":"+str(s_stat[str(p_list[no])])+":"+str(o_stat[str(p_list[no])])
            for no in type.keys():
                map_pred_type["T:"+str(no)]=str(type[no])
            for s in sbj.keys():
                if "NS:" not in map_pred_type.keys():
                    map_pred_type["NS:"]=str(sbj[s])
                else:
                    map_pred_type["NS:"]=str(int(map_pred_type["NS:"])+int(sbj[s]))
            for o in obj.keys():
                if "NO:" not in map_pred_type.keys():
                    map_pred_type["NO:"]=str(obj[o])
                else:
                    map_pred_type["NO:"]=str(int(map_pred_type["NO:"])+int(obj[o]))
            map_pred_type=dict(sorted(map_pred_type.items(), key=lambda item: item[1].split(":")[1] if "P:" in item[0] else item[1]))
            map_pred_type=dict(sorted(map_pred_type.items(), key=lambda item: item[0].split(":")[0]))
            map_count = len(map_pred_type)
            pred_count = 0
            triple_count = 0
            type_count = 0
            sbj_count = map_pred_type["NS:"]
            obj_count = map_pred_type["NO:"]
            for i in map_pred_type.keys():
                value=i
                tripl=map_pred_type[i]
                tripl=tripl.split(":")[0]
                if("P:" in value):
                    pred_count+=1
                    triple_count+=int(tripl)
                elif("T:" in value):
                    type_count+=1
            with open(void_file, 'w') as outputfile:
                outputfile.write("@prefix void: <http://rdfs.org/ns/void#> .\n")
                outputfile.write("@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n")
                outputfile.write("@prefix dc: <http://purl.org/dc/elements/1.1/> .\n")
                outputfile.write("@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .")
                outputfile.write("\n")
                outputfile.write("[] a void:Dataset ;\n")
                class_count=map_count-pred_count
                outputfile.write("\tvoid:triples \""+str(triple_count)+"\" ;\n")
                outputfile.write("\tvoid:classes \""+str(class_count)+"\" ;\n")
                outputfile.write("\tvoid:properties \""+str(pred_count)+"\" ;\n")
                outputfile.write("\tvoid:distinctSubjects \""+str(sbj_count)+"\" ;\n")
                outputfile.write("\tvoid:distinctObjects \""+str(obj_count)+"\" ;\n")
                outputfile.write("\tvoid:sparqlEndpoint <http://www."+str(file).split("/")[-1].split(".")[0]+".fr/> ;\n")

                i1 = 0
                i2 = 0
                for j in map_pred_type.keys():
                    if "P:" in j:
                        pred=j
                        pred=pred.split("P:")[1]
                        vals=map_pred_type[j].split(":")
                        trpl=vals[0]
                        if i1 == 0:
                            outputfile.write("\tvoid:propertyPartition [\n")
                        else:
                            outputfile.write("\t] , [\n")
                        outputfile.write("\t\tvoid:property "+pred+" ;\n")
                        outputfile.write("\t\tvoid:triples \""+trpl+"\" ;\n")
                        outputfile.write("\t\tvoid:distinctSubjects \""+vals[1]+"\" ;\n")
                        outputfile.write("\t\tvoid:distinctObjects \""+vals[2]+"\" ;\n")
                        i1+=1
                    elif "T:" in j:
                        type=j
                        type=type.split("T:")[1]
                        if i2 == 0:
                            outputfile.write("\t] ;\n")
                            outputfile.write("\tvoid:classPartition [\n")
                        else:
                            outputfile.write("\t] , [\n")
                        outputfile.write("\t\tvoid:class "+type+" ;\n")
                        outputfile.write("\t\tvoid:entities \""+map_pred_type[j]+"\" ;\n")
                        i2+=1
                outputfile.write("\t] .\n")

    #ctx.invoke(fedx.generate_config_file, datafiles=datafiles, outfile=outfile, eval_config=eval_config, endpoint=endpoint)

if __name__ == "__main__":
    cli()