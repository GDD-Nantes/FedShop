#!/bin/bash

function probe(){
    response_code=$(curl -o /dev/null --noproxy '*' --silent --head --write-out '%{http_code}\n' $1)
    echo "$response_code"
}

# PARAMS
n_containers=10
reset_database=true

OPENLINK_CONTAINER_PATH_TO_ISQL="/opt/virtuoso-opensource/bin/isql"
OPENLINK_CONTAINER_PATH_TO_DATA="/usr/share/proj/" 

TENFORCE_CONTAINER_PATH_TO_ISQL="/usr/local/virtuoso-opensource/bin/isql-v" 
TENFORCE_CONTAINER_PATH_TO_DATA="/usr/local/virtuoso-opensource/share/virtuoso/vad"

# MAIN

if [ "$reset_database" = true ]; then
    echo "Removing database..."
    docker-compose -f experiments/bsbm/docker/virtuoso.yml down --remove-orphans &&
    docker volume prune --force &&
    docker-compose -f experiments/bsbm/docker/virtuoso.yml create --no-recreate --scale bsbm-virtuoso=$n_containers
    docker volume ls | awk 'NR > 1 {print $2}' | xargs docker volume rm
fi

for container_id in $(seq 1 $n_containers)
do
    container_name="docker-bsbm-virtuoso-${container_id}"
    echo "Stopping all containers..."
    docker-compose -f experiments/bsbm/docker/virtuoso.yml stop bsbm-virtuoso

    echo "Starting $container_name ..."
    docker start $container_name
    container_port=$(docker port $container_name 8890 | grep -oP '0\.0\.0\.0:\K([0-9]+)')
    container_endpoint="http://localhost:$container_port/sparql"

    if [ -z "$container_port" ]; then
        echo "Container endpoint not found!" && exit -1
    fi

    attempt=0
    while [ "$(probe $container_endpoint)" != "200" ]; do
        echo "Waiting for $container_endpoint, attempt = $attempt..."
        sleep 1
        attempt=$(expr $attempt + 1)
    done

    batch_id=$(expr $container_id - 1)
    vendor_ingest_file="experiments/bsbm/model/virtuoso/ingest_vendor_batch${batch_id}.sh"
    echo "$vendor_ingest_file"

    # sed -Ei "s#$TENFORCE_CONTAINER_PATH_TO_ISQL#$OPENLINK_CONTAINER_PATH_TO_ISQL#g" $vendor_ingest_file
    # sed -Ei "s#$TENFORCE_CONTAINER_PATH_TO_DATA#$OPENLINK_CONTAINER_PATH_TO_DATA#g" $vendor_ingest_file

    # sed -Ei "s#$OPENLINK_CONTAINER_PATH_TO_ISQL#$TENFORCE_CONTAINER_PATH_TO_ISQL#g" $vendor_ingest_file
    # sed -Ei "s#$OPENLINK_CONTAINER_PATH_TO_DATA#$TENFORCE_CONTAINER_PATH_TO_DATA#g" $vendor_ingest_file

    sh "$vendor_ingest_file" || exit -1

    ratingsite_ingest_file="experiments/bsbm/model/virtuoso/ingest_ratingsite_batch${batch_id}.sh"
    echo "$ratingsite_ingest_file"

    # sed -Ei "s#$TENFORCE_CONTAINER_PATH_TO_ISQL#$OPENLINK_CONTAINER_PATH_TO_ISQL#g" $ratingsite_ingest_file
    # sed -Ei "s#$TENFORCE_CONTAINER_PATH_TO_DATA#$OPENLINK_CONTAINER_PATH_TO_DATA#g" $ratingsite_ingest_file

    # sed -Ei "s#$OPENLINK_CONTAINER_PATH_TO_ISQL#$TENFORCE_CONTAINER_PATH_TO_ISQL#g" $ratingsite_ingest_file
    # sed -Ei "s#$OPENLINK_CONTAINER_PATH_TO_DATA#$TENFORCE_CONTAINER_PATH_TO_DATA#g" $ratingsite_ingest_file

    sh "$ratingsite_ingest_file" || exit -1
done

