#!/usr/bin/env bash
set -e

echo "Logging into dockerhub..."
docker login -e ${DOCKER_EMAIL} -u ${DOCKER_USER} -p ${DOCKER_PASS}

echo "Tagging and pushing latest build..."
docker tag datastore:latest opentraffic/datastore:latest
docker push opentraffic/datastore:latest

echo "Tagging and pushing ${CIRCLE_SHA1}..."
docker tag datastore:latest opentraffic/datastore:${CIRCLE_SHA1}
docker push opentraffic/datastore:${CIRCLE_SHA1}

echo "Done!"
