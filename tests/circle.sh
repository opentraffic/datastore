#!/usr/bin/env bash
set -e

echo "Starting the datastore container..."
docker run \
  --name datastore \
  datastore:latest \
  echo

echo "Done!"
