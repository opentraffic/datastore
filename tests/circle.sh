#!/usr/bin/env bash
set -e

echo "Starting the datastore container..."
docker run \
  -d \
  --name datastore
  datastore:latest

# basic json validation
echo "Validating json request data..."
jq "." tests/datastore_request.json >/dev/null

echo "Done!"
