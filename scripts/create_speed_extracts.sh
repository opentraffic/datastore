#!/bin/bash

######################################################################
### YEAR/WEEK/X/Y.file contains all the hour-by-hour speeds and count indexes
### 2017/<week>/<level/<tileid>
year=2017
week=0
osmlr_base_dir="/data/opentraffic/osmlr-tiles/v1.0/pbf"
fb_path="/data/opentraffic/datastore_output_local"
speed_output_base_dir="speed-extracts/${year}/${week}"
time_range_start=1483228800
time_range_end=1483833600
time_unit_size=604800
time_entry_size=3600
time_range_description="168 ordinal hours of week 0 of year 2017"

rm -fr "${speed_output_base_dir}"

######################################################################
x=002
y=415
tile_index=2415
level=0
osmlr="${osmlr_base_dir}/${level}/${x}/${y}.osmlr"
extract="${speed_output_base_dir}/${level}/${x}/${y}"
echo "level=${level} tile_index=${tile_index}"
mkdir -p "${speed_output_base_dir}/${level}/${x}"

./make_speeds.py --osmlr ${osmlr} --fb-path ${fb_path} --output-prefix ${extract}.spd --separate-next-segments-prefix ${extract}.nex --time-range-start ${time_range_start} --time-range-end ${time_range_end} --time-unit-size ${time_unit_size} --time-entry-size ${time_entry_size} --time-range-description "${time_range_description}" --level ${level} --tile-id ${tile_index} --verbose

######################################################################
x=037
y=740
tile_index=37740
level=1
osmlr="${osmlr_base_dir}/${level}/${x}/${y}.osmlr"
extract="${speed_output_base_dir}/${level}/${x}/${y}"
echo "level=${level} tile_index=${tile_index}"
mkdir -p "${speed_output_base_dir}/${level}/${x}"

./make_speeds.py --osmlr ${osmlr} --fb-path ${fb_path} --output-prefix ${extract}.spd --separate-next-segments-prefix ${extract}.nex --time-range-start ${time_range_start} --time-range-end ${time_range_end} --time-unit-size ${time_unit_size} --time-entry-size ${time_entry_size} --time-range-description "${time_range_description}" --level ${level} --tile-id ${tile_index} --verbose

######################################################################
x=037
y=741
tile_index=37741
level=1
osmlr="${osmlr_base_dir}/${level}/${x}/${y}.osmlr"
extract="${speed_output_base_dir}/${level}/${x}/${y}"
echo "level=${level} tile_index=${tile_index}"
mkdir -p "${speed_output_base_dir}/${level}/${x}"

./make_speeds.py --osmlr ${osmlr} --fb-path ${fb_path} --output-prefix ${extract}.spd --separate-next-segments-prefix ${extract}.nex --time-range-start ${time_range_start} --time-range-end ${time_range_end} --time-unit-size ${time_unit_size} --time-entry-size ${time_entry_size} --time-range-description "${time_range_description}" --level ${level} --tile-id ${tile_index} --verbose

######################################################################
# gzip the files
files=`find ${speed_output_base_dir} -type f`
for i in ${files}
do
  echo "gzip ${i}..."
  gzip -c ${i} > ${i}.gz
done

