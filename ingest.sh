#!bin/bash
DATASET=$1
docker exec bsbm-virtuoso isql "EXEC=ld_dir('/usr/share/proj/', '*.nq', 'http://example.com/datasets/$DATASET');" &&
docker exec bsbm-virtuoso isql "EXEC=rdf_loader_run(log_enable=>2);" &&
docker exec bsbm-virtuoso isql "EXEC=checkpoint;"