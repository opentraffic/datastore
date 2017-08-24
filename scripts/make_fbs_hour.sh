#!/bin/bash

#select your 1 hour epoch time period
epoch_time_bucket_seconds=1483358400
end_epoch_time_bucket_seconds=1483361999
epoch_time_bucket_hour=$((${epoch_time_bucket_seconds}/3600))
echo "epoch_time_bucket_hour=${epoch_time_bucket_hour}"

######################################################################

#setup a place for your datastore output
datastore_output_base_dir="/data/opentraffic/datastore_output_local"
date_dir="2017/01/2"
hour="12"

######################################################################

#update to the tile index you want to test
tile_index=2415
level=0
tile_id=$(((${tile_index} << 3) | ${level}))
echo "level=${level} tile_index=${tile_index} tile_id=${tile_id}"
target/datastore-histogram-tile-writer -b ${epoch_time_bucket_hour} -t ${tile_id} -f ${datastore_output_base_dir}/${date_dir}/${hour}/${level}/${tile_index}.fb /data/opentraffic/reporter/results/${epoch_time_bucket_seconds}_${end_epoch_time_bucket_seconds}/${level}/${tile_index}/*
######################################################################
tile_index=37740
level=1
tile_id=$(((${tile_index} << 3) | ${level}))
echo "level=${level} tile_index=${tile_index} tile_id=${tile_id}"
target/datastore-histogram-tile-writer -b ${epoch_time_bucket_hour} -t ${tile_id} -f ${datastore_output_base_dir}/${date_dir}/${hour}/${level}/${tile_index}.fb /data/opentraffic/reporter/results/${epoch_time_bucket_seconds}_${end_epoch_time_bucket_seconds}/${level}/${tile_index}/*
######################################################################
tile_index=37741
level=1
tile_id=$(((${tile_index} << 3) | ${level}))
echo "level=${level} tile_index=${tile_index} tile_id=${tile_id}"
target/datastore-histogram-tile-writer -b ${epoch_time_bucket_hour} -t ${tile_id} -f ${datastore_output_base_dir}/${date_dir}/${hour}/${level}/${tile_index}.fb /data/opentraffic/reporter/results/${epoch_time_bucket_seconds}_${end_epoch_time_bucket_seconds}/${level}/${tile_index}/*
=========================================
