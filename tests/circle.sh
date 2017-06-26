#!/usr/bin/env bash
set -e

output_file=/output/output.fb

mkdir output

echo "Starting the datastore container..."
docker run \
  --name datastore \
  -v ${PWD}/tests/work-data:/work \
  -v ${PWD}/output:/output \
  datastore:latest \
  datastore-histogram-tile-writer -b $((1478023200/3600)) -t $(((2140 << 3) | 0)) -v -f ${output_file} /work/1478023200_1478026799/0/2140/*

if [ -f ${output_file} ]; then
  echo "Success!"
  ls -l ${output_file}
  exit 0
else
  echo "Failed: output file doesn't exist or is zero length!"
  exit 1
fi
