#!bin/bash
DATASET=$1

ISQL=/usr/local/virtuoso-opensource/bin/isql-v

echo "Starting docker container..."
#docker compose up -d bsbm-virtuoso &
#./wait-for-it.sh localhost:8890/sparql --strict &&
docker exec bsbm-virtuoso /usr/local/virtuoso-opensource/bin/isql-v "EXEC=ld_dir('/usr/local/virtuoso-opensource/share/virtuoso/vad/', '*.nq', 'http://example.com/datasets/default');" 
docker exec bsbm-virtuoso /usr/local/virtuoso-opensource/bin/isql-v "EXEC=rdf_loader_run(log_enable=>2);" &&
docker exec bsbm-virtuoso /usr/local/virtuoso-opensource/bin/isql-v "EXEC=checkpoint;"