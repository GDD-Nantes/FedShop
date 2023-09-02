
docker-compose -f experiments/bsbm/docker/jena.yml exec jena-fuseki /bin/bash -c '/jena-fuseki/tdbloader2 --loc /fuseki/databases/FedShop  /staging/*.nq'
docker restart docker-jena-fuseki-1