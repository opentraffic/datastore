#!/bin/bash

# Call python script to convert all reporter results to flatbuffer files

reporter_results="/data/opentraffic/reporter/results"
flatbuffer_output="/data/opentraffic/datastore_output_local"

./scripts/convert_to_fb.py --reporter-path  ${reporter_results} --datastore-bucket ${flatbuffer_output}
