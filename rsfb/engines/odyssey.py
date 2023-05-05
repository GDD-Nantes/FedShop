# Import part
import json
import os
import re
import click
import glob
import subprocess
import pandas as pd
import numpy as np
from pathlib import Path
import csv
from itertools import zip_longest

import sys
sys.path.append(str(os.path.join(Path(__file__).parent.parent)))
#sys.set_int_max_str_digits(0)

from utils import kill_process, load_config, str2n3
# Example of use : 
# python3 utils/generate-fedx-config-file.py experiments/bsbm/model/vendor test/out.ttl

# Goal : Generate a configuration file for RDF4J to set the use of named graph as endpoint thanks to data file

@click.group
def cli():
    pass

@cli.command()
@click.argument("eval-config", type=click.Path(exists=True, file_okay=True, dir_okay=True))
def prerequisites(eval_config):
    """Obtain prerequisite artifact for engine, e.g, compile binaries, setup dependencies, etc.

    Args:
        eval_config (_type_): _description_
    """
    
    app_config = load_config(eval_config)["evaluation"]["engines"]["fedx"]
    app = app_config["dir"]
    jar = os.path.join(app, "FedX-1.0-SNAPSHOT.jar")
    lib = os.path.join(app, "lib/*")
    
    #if not os.path.exists(app) or not os.path.exists(jar) or os.path.exists(lib):
    oldcwd = os.getcwd()
    os.chdir(Path(app).parent)
    os.system("mvn clean && mvn install dependency:copy-dependencies package")
    os.chdir(oldcwd)

    app_config = load_config(eval_config)["evaluation"]["engines"]["odyssey"]
    app = app_config["odyssey_dir"]

    oldcwd = os.getcwd()
    os.chdir(Path(app))
    os.system("./Odyssey.sh")
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

    # Odyssey run part

    app_config = load_config(eval_config)["evaluation"]["engines"]["odyssey"]
    app = app_config["dir"]
    jar = os.path.join(app, "FedX-1.0-SNAPSHOT.jar")
    lib = os.path.join(app, "lib/*")
    timeout = int(app_config["timeout"])
    properties = app_config["properties"]
    script = app_config["script"]
    work_dir = app_config["work_dir"]
    properties = work_dir+properties

    lines = []
    provenance_stat_to_modif = f"{query.split('/')[4]}-{query.split('/')[5]}.out"
    with open(properties, "r") as properties_file:
        lines = properties_file.readlines()
        for i in range(len(lines)):
            if lines[i].startswith("queryFolder"):
                lines[i] = re.sub(r'queryFolder=.+',r'queryFolder='+str(work_dir)+str(query).split("injected.sparql")[0][:-1], lines[i])
            elif lines[i].startswith("outputFile"):
                lines[i] = re.sub(r'outputFile=\$\{federatedOptimizerPath\}\/results\/.+',r'outputFile=${federatedOptimizerPath}/results/'+provenance_stat_to_modif, lines[i])
                print(lines[i])
    with open(properties, "w") as properties_file:
        properties_file.writelines(lines)

    cmd = f'cd {work_dir}/engines/Odyssey/scripts && bash {script} && cd {work_dir}'

    print("=== Odyssey ===")
    print(cmd)
    print("===============")

    odyssey_proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    #odyssey_output, splendid_error = odyssey_proc.communicate()
    try: 
        odyssey_proc.wait(timeout=timeout)
        if odyssey_proc.returncode == 0:
            print(f"{query} benchmarked sucessfully") 
            create_stats(stats)
        else:
            # print(f"{query} reported error")
            # create_stats()
            # write_empty_result("error_runtime")
            raise RuntimeError(f"{query} reported error")
           
    except subprocess.TimeoutExpired: 
        print(f"{query} timed out!")
        create_stats()
        write_empty_result("timeout")
        kill_process(odyssey_proc.pid)

    # FedX run part

    r_root = Path(result).parent
    print(r_root)

    #stat = f"{r_root}/stats.csv"
    input_provenance_file = f"{work_dir}engines/Odyssey/results/{provenance_stat_to_modif}"
    output_provenance_file = f"{work_dir}engines/Odyssey/results/{provenance_stat_to_modif}"

    data_transform(input_provenance_file, output_provenance_file)

    ideal_ss = f"engines/Odyssey/results/{provenance_stat_to_modif}"

    args = [engine_config, query, result, stats, source_selection, ideal_ss]
    args = " ".join(args)
    #timeoutCmd = f'timeout --signal=SIGKILL {timeout}' if timeout != 0 else ""
    timeoutCmd = ""
    cmd = f'{timeoutCmd} java -classpath "{jar}:{lib}" org.example.FedX {args}'.strip() 
    
    def create_stats():
        with open(stats, "w+") as fout:
            fout.write("query;engine;instance;batch;mode;exec_time;distinct_ss\n")
            basicInfos = re.match(r".*/(\w+)/(q\d+)/instance_(\d+)/batch_(\d+)/(\w+)/results", result)
            engine = basicInfos.group(1)
            queryName = basicInfos.group(2)
            instance = basicInfos.group(3)
            batch = basicInfos.group(4)
            mode = basicInfos.group(5)
            fout.write(";".join([queryName, engine, instance, batch, mode, "nan", "nan"])+"\n")
            fout.close()
    
    def write_empty_result(msg):
        with open(result, "w+") as fout:
            fout.write(msg)
            fout.close()

    fedx_proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try: 
        fedx_proc.wait(timeout=timeout)
        if fedx_proc.returncode == 0:
            print(f"{query} benchmarked sucessfully")  
        else:
            # print(f"{query} reported error")
            # create_stats()
            # write_empty_result("error_runtime")
            raise RuntimeError(f"{query} reported error")
           
    except subprocess.TimeoutExpired: 
        print(f"{query} timed out!")
        create_stats()
        write_empty_result("timeout")
        kill_process(fedx_proc.pid)

@cli.command()
@click.argument("datafiles", type=click.Path(exists=True, dir_okay=False, file_okay=True), nargs=-1)
@click.argument("outfile", type=click.Path(exists=False, file_okay=True, dir_okay=False))
@click.argument("eval-config", type=click.Path(exists=True, dir_okay=False, file_okay=True))
@click.argument("batch_id", type=click.INT)
@click.argument("endpoint", type=str)
@click.pass_context
def generate_config_file(ctx: click.Context, datafiles, outfile, eval_config, batch_id, endpoint):

    # FedX config part

    ssite = set()
    #for data_file in glob.glob(f'{dir_data_file}/*.nq'):
    for data_file in datafiles:
        with open(data_file) as file:
            t_file = file.readlines()
            for line in t_file:
                site = line.rsplit()[-2]
                site = re.search(r"<(.*)>", site).group(1)
                ssite.add(site)
    
    with open(f'{outfile}', 'a') as ffile:
        ffile.write(
"""
@prefix sd: <http://www.w3.org/ns/sparql-service-description#> .
@prefix fedx: <http://rdf4j.org/config/federation#> .

"""
        )
        for s in sorted(ssite):
            ffile.write(
f"""
<{s}> a sd:Service ;
    fedx:store "SPARQLEndpoint";
    sd:endpoint "{endpoint}/?default-graph-uri={s}";
    fedx:supportsASKQueries false .   

"""
            )

    # Odyssey config part

    ODYSSEY = load_config(configfile)["evaluation"]["engines"]["odyssey"]

    with open(ODYSSEY["federation_file"],'w+') as outputfile:
        outputfile.write("@prefix fluid: <http://fluidops.org/config#>.\n\n")
        for file in datafiles:
            source = str(file).split('/')[-1].split('.')[0]
            outputfile.write("<http://www."+source+".fr/> fluid:store \"SPARQLEndpoint\";\n")
            outputfile.write("fluid:SPARQLEndpoint \""+endpoint+"\";\n")
            outputfile.write("fluid:supportsASKQueries \"false\" .\n\n")

    with open(ODYSSEY["config"],'w+') as outputfile:
        for file in datafiles:
            source = str(file).split('/')[-1].split('.')[0]
            outputfile.write(source+"\thttp://www."+source+".fr/\n")

    for file in datafiles:
        # print("Generate void description for "+str(file)+"...")
        with open(str(ODYSSEY["data_summaries"])+str(file).split('/')[-1].split('.')[0]+'_void.n3','w+') as outputfile:
            outputfile.write("")
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
            with open(str(ODYSSEY["data_summaries"])+str(file).split('/')[-1].split('.')[0]+'_void.n3','a') as outputfile:
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
                outputfile.write("\tvoid:distinctObjetcs \""+str(obj_count)+"\" ;\n")

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
                        outputfile.write("\t\tvoid:entitie "+map_pred_type[j]+" ;\n")
                        i2+=1
                outputfile.write("\t] .\n")

@cli.command()
@click.argument("infile", type=click.Path(exists=False, file_okay=True, dir_okay=False))
@click.argument("outfile", type=click.Path(exists=False, file_okay=True, dir_okay=False))
@click.argument("prefix-cache", type=click.Path(exists=False, file_okay=True, dir_okay=False))
def transform_result(infile, outfile, prefix_cache):
    in_df = pd.read_csv(infile)
    
    prefix2alias = json.load(open(prefix_cache, "r"))    
    composition = json.load(open(os.path.join(Path(prefix_cache).parent, "provenance.sparql.comp"), "r"))
    inv_composition = {f"{' '.join(v)}": k for k, v in composition.items()}
    
    def extract_triple(x):
        fedx_pattern = r"StatementPattern\s+(\(new scope\)\s+)?Var\s+\((name=\w+,\s+value=(.*),\s+anonymous|name=(\w+))\)\s+Var\s+\((name=\w+,\s+value=(.*),\s+anonymous|name=(\w+))\)\s+Var\s+\((name=\w+,\s+value=(.*),\s+anonymous|name=(\w+))\)"
        match = re.match(fedx_pattern, x)
        
        #print(x)

        s = match.group(3)
        if s is None: s = f"?{match.group(4)}"
        
        p = match.group(6)
        if p is None: p = f"?{match.group(7)}"
        
        o = match.group(9) 
        if o is None: o = f"?{match.group(10)}"
        
        result = " ".join([s, p, o])
                
        for prefix, alias in prefix2alias.items():
            result = result.replace(prefix, f"{alias}:")
            
        if s.startswith("http"):
            result = result.replace(s, str2n3(s))
            
        if o.startswith("http"):
            result = result.replace(o, str2n3(o))
        
        return result

    def extract_source_selection(x):
        fex_pattern = r"StatementSource\s+\(id=sparql_([a-z]+(\.\w+)+\.[a-z]+)_,\s+type=[A-Z]+\)"
        result = [ cgroup[0] for cgroup in re.findall(fex_pattern, x) ]
        return result
    
    def lookup_composition(x: str):
        return inv_composition[x]
        
    
    in_df["triple"] = in_df["triple"].apply(extract_triple)
    in_df["tp_name"] = in_df["triple"].apply(lookup_composition)
    in_df["tp_number"] = in_df["tp_name"].str.replace("tp", "", regex=False).astype(int)
    in_df.sort_values("tp_number", inplace=True)
    in_df["source_selection"] = in_df["source_selection"].apply(extract_source_selection)
    
    # If unequal length (as in union, optional), fill with nan
    max_length = in_df["source_selection"].apply(len).max()
    in_df["source_selection"] = in_df["source_selection"].apply(lambda x: np.pad(x, (0, max_length-len(x)), mode="empty"))
        
    out_df = in_df.set_index("tp_name")["source_selection"] \
        .to_frame().T \
        .apply(pd.Series.explode) \
        .reset_index(drop=True) 
    out_df.to_csv(outfile, index=False)

def data_transform(input_provenance_file, output_provenance_file):
    line_count=0
    columns = []
    with open(input_provenance_file, 'r') as input_file:
        csvreader = csv.reader(input_file, delimiter=";")
        for line in csvreader:
            if line_count != 0:
                columns.append(['tp'+str(line_count),str(line[-1])])
                line_count+=1
            else:
                line_count+=1
    output = []
    for column in columns:
        temp_list = [str(column[0])]
        temp_list.extend(column[1].split(','))
        output.append(temp_list)
    export_output = zip_longest(*output, fillvalue = '')
    with open(output_provenance_file, "w+", newline='') as output_file:
        csvwriter = csv.writer(output_file)
        csvwriter.writerows(export_output)

if __name__ == "__main__":
    cli()