#!/usr/bin/env bash
set -e

# start the container
echo "Starting the postgres container..."
docker run \
  --name datastore-postgres \
  -d postgres:9.6.1 \
  -e 'POSTGRES_USER=opentraffic' \
  -e 'POSTGRES_PASSWORD=changeme' \
  -e 'POSTGRES_DB=opentraffic'


echo "Starting the datastore container..."
docker run \
  -d \
  -p 8003:8003 \
  --name datastore \
  --link datastore-postgres:postgres \
  -v ${PWD}/data:/data \
  -e 'POSTGRES_USER=opentraffic' \
  -e 'POSTGRES_PASSWORD=changeme' \
  -e 'POSTGRES_DB=opentraffic' \
  -e 'POSTGRES_HOST=postgres'
  opentraffic/datastore

sleep 15

# basic json validation
echo "Validating json request data..."
jq "." tests/datastore_request.json >/dev/null

# test the generated data against the service
echo "Running the test data through the datastore service..."
curl -s --data tests/datastore_request.json localhost:8003/store?

echo "Done!"
