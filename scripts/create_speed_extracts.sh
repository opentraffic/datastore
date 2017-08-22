#!/bin/bash

######################################################################
### YEAR/WEEK/X/Y.file contains all the hour-by-hour speeds and count indexes
### 2017/<week>/<level/<tileid>
year=2017
week=0
osmlr_input="/data/opentraffic/osmlr-tiles/v1.0/pbf"
fb_input="/data/opentraffic/datastore_output_local"
speed_output_base_dir="speed-extracts/${year}/${week}"
date_dir="2017/01/2"
#hour="12" #uncomment if running for a specified

rm -fr "${speed_output_base_dir}"

######################################################################
x=002
y=415
tile_index=2415
level=0
echo "level=${level} tile_index=${tile_index}"
mkdir -p "${speed_output_base_dir}/${level}/${x}"

./make_speeds.py --output-prefix ${speed_output_base_dir}/${level}/${x}/${y}.spd --separate-next-segments-prefix ${speed_output_base_dir}/${level}/${x}/${y}.nex --osmlr ${osmlr_input}/${level}/${x}/${y}.osmlr --fb-path ${fb_input} --level ${level} --tile-id ${tile_index}

######################################################################
x=037
y=740
tile_index=37740
level=1
echo "level=${level} tile_index=${tile_index}"
mkdir -p "${speed_output_base_dir}/${level}/${x}"

./make_speeds.py --output-prefix ${speed_output_base_dir}/${level}/${x}/${y}.spd --separate-next-segments-prefix ${speed_output_base_dir}/${level}/${x}/${y}.nex --osmlr ${osmlr_input}/${level}/${x}/${y}.osmlr --fb-path ${fb_input} --level ${level} --tile-id ${tile_index}

######################################################################
x=037
y=741
tile_index=37741
level=1
echo "level=${level} tile_index=${tile_index}"
mkdir -p "${speed_output_base_dir}/${level}/${x}"

./make_speeds.py --output-prefix ${speed_output_base_dir}/${level}/${x}/${y}.spd --separate-next-segments-prefix ${speed_output_base_dir}/${level}/${x}/${y}.nex --osmlr ${osmlr_input}/${level}/${x}/${y}.osmlr --fb-path ${fb_input} --level ${level} --tile-id ${tile_index}

######################################################################
# gzip the files
files=`find ${speed_output_base_dir} -type f`
for i in ${files}
do
  echo "gzip ${i}..."
  gzip -c ${i} > ${i}.gz
done

==============================================================================



