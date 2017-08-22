#!/bin/bash

# Call python script to convert all reporter results to flatbuffer files

reporter_results="/data/reporter/results"
flatbuffer_output="/data/datastore/flatbuffer_output"

./convert_to_fb.py --reporter-path  ${reporter_results} --datastore-bucket ${flatbuffer_output}
