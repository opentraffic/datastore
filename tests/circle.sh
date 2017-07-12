#!/usr/bin/env bash
set -e

echo "Starting the datastore container..."
docker run \
  --name datastore \
  -v ${PWD}/tests/work-data:/work \
  datastore:latest \
  sh -c 'datastore-histogram-tile-writer -b $((1478023200/3600)) -t $(((2140 << 3) | 0)) -v -f flatbuffer.fb /work/1478023200_1478026799/0/2140/* 1>verbose.txt'

#should have made a flatbuffer
if [[ ! -f ${PWD}/tests/work-data/flatbuffer.fb ]] || [[ ! -s ${PWD}/tests/work-data/flatbuffer.fb ]]; then
  echo "Flatbuffer output doesn't exist or is zero length!"
  exit 1
fi

#should have some text output
measurements=$(cat ${PWD}/tests/work-data/1478023200_1478026799/0/2140/* | grep -cF AUTO)
counted=$(grep -cF Measurement ${PWD}/tests/work-data/verbose.txt)
if [[ ${counted} != ${measurements} ]]; then
  echo "Failed to garner the right number of measurements"
  exit 1
fi

#fold them in twice to see you get double the output
docker run \
  --name datastore2 \
  -v ${PWD}/tests/work-data:/work \
  datastore:latest \
  sh -c 'datastore-histogram-tile-writer -b $((1478023200/3600)) -t $(((2140 << 3) | 0)) -v flatbuffer.fb /work/1478023200_1478026799/0/2140/* 1>verbose2.txt'
doubled=$(grep -cF Measurement ${PWD}/tests/work-data/verbose2.txt)
if [[ ${counted2} != $((counted*2)) ]]; then
  echo "Failed to garner the right number of measurements after folding"
  exit 1
fi
