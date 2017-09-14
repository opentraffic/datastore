#!/usr/bin/env python

import argparse
import os
import sys
import boto3
import math
import datetime
import time

#world bb
minx_ = -180
miny_ = -90
maxx_ = 180
maxy_ = 90

def get_prefixes_keys(client, bucket, prefixes):
  keys = []
  pres = []
  for prefix in prefixes:
    token = None
    first = True
    while first or token:
      if token:
        objects = client.list_objects_v2(Bucket=bucket, Delimiter='/', Prefix=prefix, ContinuationToken=token)
      else:
        objects = client.list_objects_v2(Bucket=bucket, Delimiter='/', Prefix=prefix)
      if 'Contents' in objects:
        keys.extend([ o['Key'] for o in objects['Contents'] ])
      if 'CommonPrefixes' in objects:
        pres.extend([ o['Prefix'] for o in objects['CommonPrefixes'] ])
      token = objects.get('NextContinuationToken')
      first = False
  return pres, keys

class BoundingBox(object):

  def __init__(self, min_x, min_y, max_x, max_y):
     self.minx = min_x
     self.miny = min_y
     self.maxx = max_x
     self.maxy = max_y

class TileHierarchy(object):

  def __init__(self):
    self.levels = {}
    # local
    self.levels[2] = Tiles(BoundingBox(minx_,miny_,maxx_,maxy_),.25)
    # arterial
    self.levels[1] = Tiles(BoundingBox(minx_,miny_,maxx_,maxy_),1)
    # highway
    self.levels[0] = Tiles(BoundingBox(minx_,miny_,maxx_,maxy_),4)

class Tiles(object):

  def __init__(self, bbox, size):
     self.bbox = bbox
     self.tilesize = size

     self.ncolumns = int(math.ceil((self.bbox.maxx - self.bbox.minx) / self.tilesize))
     self.nrows = int(math.ceil((self.bbox.maxy - self.bbox.miny) / self.tilesize))
     self.max_tile_id = ((self.ncolumns * self.nrows) - 1)

  def Row(self, y):
    #Return -1 if outside the tile system bounds
    if (y < self.bbox.miny or y > self.bbox.maxy):
      return -1

    #If equal to the max y return the largest row
    if (y == self.bbox.maxy):
      return nrows - 1
    else:
      return int((y - self.bbox.miny) / self.tilesize)

  def Col(self, x):
    #Return -1 if outside the tile system bounds
    if (x < self.bbox.minx or x > self.bbox.maxx):
      return -1

    #If equal to the max x return the largest column
    if (x == self.bbox.maxx):
      return self.ncolumns - 1
    else:
      col = (x - self.bbox.minx) / self.tilesize
      return int(col) if (col >= 0.0) else int(col - 1)

  def Digits(self, number):
    digits = 1 if (number < 0) else 0
    while long(number):
       number /= 10
       digits += 1
    return long(digits)

  # Get the bounding box of the specified tile.
  def TileBounds(self, tileid):
    row = tileid / self.ncolumns
    col = tileid - (row * self.ncolumns)

    x = self.bbox.minx + (col * self.tilesize)
    y = self.bbox.miny + (row * self.tilesize)
    return BoundingBox(x, y, x + self.tilesize, y + self.tilesize)

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='Generate Coverage Map', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
  parser.add_argument('--ref-speed-bucket', type=str, help='AWS Ref Bucket location.', required=True)
  parser.add_argument('--ref-bucket-version', type=str, help='Version within the AWS Bucket (e.g., v1.0)', required=True)
  parser.add_argument('--output-file', default=None, type=str, help='geojson output file')
  parser.add_argument('--upload-results', help='Upload the results to the AWS Bucket', action='store_true')

  # parse the arguments
  args = parser.parse_args()
  
  if not args.output_file:
    args.output_file = args.ref_bucket_version + '.geojson'
   
  print('[INFO] Output file: ' + args.output_file)
  tile_hierarchy = TileHierarchy()
  client = boto3.client('s3')
  batch_client = boto3.client('batch')

  print('[INFO] Getting keys from bucket')
  # version with only level 1
  prefixes, _ = get_prefixes_keys(client, args.ref_speed_bucket, [args.ref_bucket_version + "/1"] )
  # tile dirs
  prefixes, _ = get_prefixes_keys(client, args.ref_speed_bucket, prefixes)
  # physical tiles
  prefixes, keys = get_prefixes_keys(client, args.ref_speed_bucket, prefixes)
  
  geojson = '{"type": "FeatureCollection","features": ['
  first = True
  epoch = datetime.datetime(1970,1,1)
  
  #only dealing with level 1
  level = 1
  print('[INFO] Done.  Getting keys from bucket')
  print('[INFO] Getting metadata from keys and building geojson')
  for k in keys:
    path_list = k.split(os.sep)
    prefix_tileid = str(path_list[path_list.index(args.ref_bucket_version)+2])
    suffix_tileid = str(os.path.splitext(os.path.splitext(os.path.basename(k))[0])[0])
    tileid = int(prefix_tileid + suffix_tileid)
    response = client.head_object(Bucket=args.ref_speed_bucket, Key=k)
    if 'Metadata' in response:
      metadata = response['Metadata']
      print metadata
      if metadata:
        if 'rangestart' in metadata and 'rangeend' in metadata:
          if not first:
            geojson += ','
          first = False

          begin = time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime(int(metadata['rangestart'])))   
          end = time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime(int(metadata['rangeend'])))

          geojson += '{"properties": {"fillOpacity": 0.33,"color": "#50bf40","fill": "#50bf40",'
          geojson += '"fillColor": "#50bf40","contour": 15,"opacity": 0.33,"fill-opacity": 0.33,'
          geojson += '"tileid": ' + str(tileid) + ','
          geojson += '"rangeStart": ' + metadata['rangestart'] + ','
          geojson += '"rangeEnd": ' + metadata['rangeend'] + ','
          geojson += '"rangeStartDate": "' + begin + '",'
          geojson += '"rangeEndDate": "' + end + '"},'          
          geojson += '"type": "Feature","geometry": {"coordinates": [['
   
          bb = tile_hierarchy.levels[level].TileBounds(tileid)
          geojson += '[' + str(bb.minx) + ',' + str(bb.miny) + '],' 
          geojson += '[' + str(bb.maxx) + ',' + str(bb.miny) + '],' 
          geojson += '[' + str(bb.maxx) + ',' + str(bb.maxy) + '],' 
          geojson += '[' + str(bb.minx) + ',' + str(bb.maxy) + '],' 
          geojson += '[' + str(bb.minx) + ',' + str(bb.miny) + ']'
          geojson += ']],"type": "Polygon"}}'
  geojson += ']}'
  
  with open(args.output_file, "w") as f:
    f.write(geojson) 

  print('[INFO] Coverage Map generation complete!')
  #upload to s3
  if args.upload_results:
    print('[INFO] Uploading file....')
    s3_client = boto3.client("s3")
    #push up 
    with open(args.output_file) as f :
      object_data = f.read()
      s3_client.put_object(Body=object_data, Bucket=args.ref_speed_bucket, Key= args.ref_bucket_version + "/" + args.output_file)
    print('[INFO] Done.  Uploading file....')
