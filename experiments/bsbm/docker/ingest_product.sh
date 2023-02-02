#!/bin/bash
nqDir="experiments/bsbm/model/tmp/product/" 
nbNQFiles=$(find $nqDir -name "*.nq" | wc -l)
for file in experiments/bsbm/model/tmp/product/**/*.nq; do
    pathFile=$(echo $file | sed "s#experiments/bsbm/model/tmp/product/##g")
    docker exec bsbm-product-only /usr/local/virtuoso-opensource/bin/isql-v "EXEC=ld_dir('/usr/local/virtuoso-opensource/share/virtuoso/vad/', '$pathFile', 'http://example.com/datasets/default');" >> /dev/null
    echo $pathFile
done | tqdm --total $nbNQFiles --unit files >> /dev/null
docker exec bsbm-product-only /usr/local/virtuoso-opensource/bin/isql-v "EXEC=rdf_loader_run(log_enable=>2);" &&
docker exec bsbm-product-only /usr/local/virtuoso-opensource/bin/isql-v "EXEC=checkpoint;"&&
exit 0
