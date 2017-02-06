#!/usr/bin/env bash
set -e

# start the container
echo "Starting the postgres container..."
docker run \
  --name datastore-postgres \
  -d postgres:9.6.1

sleep 30

echo "Starting the datastore container..."
docker run \
  -d \
  -p 8003:8003 \
  --name datastore \
  --link datastore-postgres:postgres \
  -v ${PWD}/data:/data \
  opentraffic/datastore

sleep 30

# basic json validation
echo "Validating json request data..."
jq "." tests/datastore_request.json >/dev/null

# test the generated data against the service
echo "Running the test data through the datastore service..."
curl -s --max-time 25 --retry 5 --retry-delay 5 --data tests/datastore_request.json localhost:8003/store?

echo "Done!"
