#!/usr/bin/env bash

docker login -e ${DOCKER_EMAIL} -u ${DOCKER_USER} -p ${DOCKER_PASS}

docker tag datastore:latest opentraffic/datastore:latest
docker push opentraffic/datastore:latest

docker tag datastore:latest opentraffic/datastore:${CIRCLE_SHA1}
docker push opentraffic/datastore:${CIRCLE_SHA1}
