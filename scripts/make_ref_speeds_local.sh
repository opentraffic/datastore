#!/bin/bash

######################################################################
### YEAR/WEEK/X/Y.file contains all the hour-by-hour speeds and count indexes
### 2017/<week>/<level/<tileid>
bucket="speed-extracts"
year=2017
week=01
extract_base_dir="speed-extracts/${year}/${week}"


######################################################################
x=002
y=415
tile_index=2415
level=0
extract_path="${extract_base_dir}/${level}/${x}/"
extract="${extract_path}${y}"
speedtile_list=`ls ${extract}.spd.* | grep -v "gz$" | tr '\n' ' '`
echo "speedtile_list=${speedtile_list}"
echo "level=${level} tile_index=${tile_index}"
echo "Removing ${extract}.ref..."
rm -f ${extract}.ref

./make_ref_speeds.py --speedtile-list ${speedtile_list} --bucket ${bucket} --year ${year} --level ${level} --tile-id ${tile_index} --ref-tile-path ${extract_path} --local --verbose

######################################################################
x=037
y=740
tile_index=37740
level=1
extract_path="${extract_base_dir}/${level}/${x}/"
extract="${extract_path}${y}"
speedtile_list=`ls ${extract}.spd.* | grep -v "gz$" | tr '\n' ' '`
echo "speedtile_list=${speedtile_list}"
echo "level=${level} tile_index=${tile_index}"
echo "Removing ${extract}.ref..."
rm -f ${extract}.ref

./make_ref_speeds.py --speedtile-list ${speedtile_list} --bucket ${bucket} --year ${year} --level ${level} --tile-id ${tile_index} --ref-tile-path ${extract_path} --local --verbose

######################################################################
x=037
y=741
tile_index=37741
level=1
extract_path="${extract_base_dir}/${level}/${x}/"
extract="${extract_path}${y}"
speedtile_list=`ls ${extract}.spd.* | grep -v "gz$" | tr '\n' ' '`
echo "speedtile_list=${speedtile_list}"
echo "level=${level} tile_index=${tile_index}"
echo "Removing ${extract}.ref..."
rm -f ${extract}.ref

./make_ref_speeds.py --speedtile-list ${speedtile_list} --bucket ${bucket} --year ${year} --level ${level} --tile-id ${tile_index} --ref-tile-path ${extract_path} --local --verbose

######################################################################
# gzip the ref files
files=`find ${extract_base_dir} -type f | grep ".ref$"`
for i in ${files}
do
  echo "gzip ${i}..."
  gzip -c ${i} > ${i}.gz
done

