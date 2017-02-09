#!/usr/bin/env bash
set -e

datastore_port=8003

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
  -p ${datastore_port}:${datastore_port} \
  --name datastore \
  --link datastore-postgres:postgres \
  -v ${PWD}/data:/data \
  -e 'POSTGRES_USER=opentraffic' \
  -e 'POSTGRES_PASSWORD=changeme' \
  -e 'POSTGRES_DB=opentraffic' \
  -e 'POSTGRES_HOST=postgres' \
  $AWS_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/opentraffic/datastore:$CIRCLE_SHA1

echo "Container is running, sleeping to allow creation of database..."
sleep 10

# basic json validation
echo "Validating json request data..."
jq "." tests/datastore_request.json >/dev/null

# test the generated data against the service
echo "Running the test data through the datastore service..."
curl --fail --max-time 15 --retry 3 --retry-delay 5 --data @tests/datastore_request.json localhost:${datastore_port}/store?

echo "Done!"
