#!/bin/bash

function probe(){
    response_code=$(curl -o /dev/null --silent --head --write-out '%{http_code}\n' $1)
    echo "$response_code"
}

# PARAMS
n_containers=10
reset_database=false

# MAIN

if [ "$reset_database" = true ]; then
    echo "Removing database..."
    docker-compose -f experiments/bsbm/docker/virtuoso.yml down --remove-orphans &&
    docker volume prune --force &&
    docker-compose -f experiments/bsbm/docker/virtuoso.yml create --no-recreate --scale bsbm-virtuoso=10
    # docker volume ls | awk 'NR > 1 {print $2}' | xargs docker volume rm
fi

for container_id in $(seq 1 $n_containers)
do
    container_name="docker-bsbm-virtuoso-${container_id}"
    echo "Stopping all containers..."
    docker-compose -f experiments/bsbm/docker/virtuoso.yml stop bsbm-virtuoso

    echo "Starting $container_name ..."
    docker start $container_name
    container_infos=$(docker ps --all --format '{{.Names}} {{.Ports}}')
    container_port=$(echo "$container_infos" | grep "$container_name " | awk '{print $4}' | sed -E "s#0\.0\.0\.0:([0-9]+).*#\1#g")
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
    echo "experiments/bsbm/model/virtuoso/ingest_vendor_batch${batch_id}.sh"
    sh "experiments/bsbm/model/virtuoso/ingest_vendor_batch${batch_id}.sh" || exit -1

    echo "experiments/bsbm/model/virtuoso/ingest_ratingsite_batch${batch_id}.sh"
    sh "experiments/bsbm/model/virtuoso/ingest_ratingsite_batch${batch_id}.sh" || exit -1
done

