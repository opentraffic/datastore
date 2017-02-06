#!/usr/bin/env bash
set -e

# start the container
echo "Starting the postgres container..."
docker run \
  -d \
  --name datastore-postgres \
  -e 'POSTGRES_USER=opentraffic' \
  -e 'POSTGRES_PASSWORD=changeme' \
  -e 'POSTGRES_DB=opentraffic' \
  postgres:9.6.1


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
  -e 'POSTGRES_HOST=postgres' \
  opentraffic/datastore

echo "Container is running, sleeping to allow creation of database..."
sleep 10

# basic json validation
echo "Validating json request data..."
jq "." tests/datastore_request.json >/dev/null

# test the generated data against the service
echo "Running the test data through the datastore service..."
curl --fail --max-time 15 --connect-timeout 15 --data tests/datastore_request.json localhost:8003/store?

echo "Done!"
