#!bin/bash
DATASET=$1

ISQL=/usr/local/virtuoso-opensource/bin/isql-v
VIRT_ALLOWED_DIR="/usr/local/virtuoso-opensource/share/virtuoso/vad"
#VIRT_ALLOWED_DIR="/usr/share/proj/"
echo "Starting docker container..."
#docker compose up -d bsbm-virtuoso &
#./wait-for-it.sh localhost:8890/sparql --strict &&
docker exec bsbm-virtuoso $ISQL "EXEC=ld_dir('$VIRT_ALLOWED_DIR', '*.nq', 'http://example.com/datasets/$DATASET');" &&
docker exec bsbm-virtuoso $ISQL "EXEC=rdf_loader_run(log_enable=>2);" &&
docker exec bsbm-virtuoso $ISQL "EXEC=checkpoint;"