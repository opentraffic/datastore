#!/usr/bin/env python
import argparse
import random
import math
import os
import errno
import sys
import boto3
import botocore
import shutil
import gzip
import logging as log

try:
  import speedtile_pb2
except ImportError:
  print 'You need to generate protobuffer source via: protoc --python_out . --proto_path ../proto ../proto/*.proto'
  sys.exit(1)

###############################################################################
#world bb
minx_ = -180
miny_ = -90
maxx_ = 180
maxy_ = 90

def get_tile_count(filename):
  #lets load the protobuf speed tile
  spdtile = speedtile_pb2.SpeedTile()
  with open(directory + key, 'rb') as f:
    spdtile.ParseFromString(f.read())
  # 0 based so subtract 1
  return int(round(spdtile.subtiles[0].totalSegments / (spdtile.subtiles[0].subtileSegments * 1.0))) - 1

###############################################################################
### tile 2415
### segment0{refspdlist[hour0:avgspd, hour1:avgspd, hour2:avgspd, etc]} -> sort lo to hi, index in to get refspd20:int, refspd40:int, refspd60:int, refspd80:int
### segment1{refspdlist[hour0:avgspd, hour1:avgspd, hour2:avgspd, etc]}
# get the avg speeds for a given segment for each hour (168 hr/day) over 52 weeks

def createAvgSpeedList(fileNames):
  #each segment has its own list of speeds, we dont know how many segments for the avg speed list to start with
  segments = []

  #need to loop thru all of the speed tiles for a given tile id
  for fileName in fileNames:
    #lets load the protobuf speed tile
    spdtile = speedtile_pb2.SpeedTile()
    with open(fileName, 'rb') as f:
      spdtile.ParseFromString(f.read())

    #we now need to retrieve all of the speeds for each segment in this tile
    for subtile in spdtile.subtiles:

      #make sure that there are enough lists in the list for all segments
      missing = spdtile.subtiles[0].totalSegments - len(segments)
      if missing > 0:
        segments.extend([ [] for i in range(0, missing) ])
      
      print 'total # created in segments ' + str(subtile.totalSegments)
      entries = subtile.unitSize / subtile.entrySize
      print '# of entries per segment : ' + str(entries)
      for i, speed in enumerate(subtile.speeds):
        if speed > 0:
          segments[subtile.startSegmentIndex + int(math.floor(i/entries))].append(speed)
          #print 'index=' + str(i) + ' | speeds=' + str(speed) + '| startSegmentIndex+i= ' + str(subtile.startSegmentIndex + (i%entries))

  #sort each list of speeds per segment
  for i, segment in enumerate(segments):
    if len(segment) > 0:
      print '# of valid average speeds in segment ' + str(i) + ' is ' + str(len(segment))
    segment.sort()

  return segments

###############################################################################
def createRefSpeedTile(path, fileName, speedListPerSegment):
  log.debug('createRefSpeedTiles ###############################################################################')

  tile = speedtile_pb2.SpeedTile()
  st = tile.subtiles.add()
  #st.level = 0 #TODO: get from osmlr
  #st.index = 415 #TODO: get from osmlr
  st.startSegmentIndex = 0
  st.totalSegments = len(speedListPerSegment)
  st.subtileSegments = len(speedListPerSegment)
  #time stuff
  #st.rangeStart = #TODO: first second since epoch of first week you have data for
  #st.rangeEnd = #TODO: last second since epoch of last week you have data for
  st.unitSize = st.rangeEnd - st.rangeStart
  st.entrySize = st.unitSize
  st.description = 'Reference speeds over 1 year from 08.2016 through 07.2017' #TODO: get this from range start and end

  print 'speedListPerSegment length: ' + str(len(speedListPerSegment))
  #bucketize avg speeds into 20%, 40%, 60% and 80% reference speed buckets
  #for each segment
  for segment in speedListPerSegment:
    size = len(segment)
    st.referenceSpeeds20.append(segment[int(size * .2)] if size > 0 else 0) #0 here represents no data
    st.referenceSpeeds40.append(segment[int(size * .4)] if size > 0 else 0) #0 here represents no data
    st.referenceSpeeds60.append(segment[int(size * .6)] if size > 0 else 0) #0 here represents no data
    st.referenceSpeeds80.append(segment[int(size * .8)] if size > 0 else 0) #0 here represents no data

  #print str(st.referenceSpeeds20)
  #print str(st.referenceSpeeds40)
  #print str(st.referenceSpeeds60)
  #print str(st.referenceSpeeds80)

  #write it out
  with open(path + fileName, 'ab') as f:
    f.write(tile.SerializeToString())

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

  def Digits(self, number):
    digits = 1 if (number < 0) else 0
    while long(number):
       number /= 10
       digits += 1
    return long(digits)

  # get the File based on tile_id and level
  def GetFile(self, tile_id, level):
    max_length = self.Digits(self.max_tile_id)

    remainder = max_length % 3
    if remainder:
       max_length += 3 - remainder

    #if it starts with a zero the pow trick doesn't work
    if level == 0:
      file_suffix = '{:,}'.format(int(pow(10, max_length)) + tile_id).replace(',', '/')
      file_suffix += "."
      file_suffix += "spd.0"
      file_suffix = "0" + file_suffix[1:]
      return file_suffix

    #it was something else
    file_suffix = '{:,}'.format(level * int(pow(10, max_length)) + tile_id).replace(',', '/')
    file_suffix += "."
    file_suffix += "spd.0"

    return file_suffix

###############################################################################

# Read in protobuf files from the datastore output in AWS to read in the lengths, speeds & next segment ids and generate the segment speed files in proto output format
if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='Generate speed tiles', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
  parser.add_argument('--speedtile-list', type=str, nargs='+', help='A list of the PDE speed tiles containing the average speeds per segment per hour of day for one week')
  parser.add_argument('--ref-tile-path', type=str, help='The public data extract speed tile containing the average speeds per segment per hour of day for one week', required=True)
  parser.add_argument('--ref-tile-file', type=str, help='The ref tile file name.')
  parser.add_argument('--bucket', type=str, help='AWS bucket location', required=True)
  parser.add_argument('--year', type=str, help='The year you wish to get', required=True)
  parser.add_argument('--level', type=int, help='The level to target', required=True)
  parser.add_argument('--tile-id', type=int, help='The tile id to target', required=True)
  parser.add_argument('--no-separate-subtiles', help='If present all subtiles will be in the same tile', action='store_true')
  parser.add_argument('--local', help='Enable local file processing', action='store_true')
  parser.add_argument('--verbose', '-v', help='Turn on verbose output i.e. DEBUG level logging', action='store_true')

  # parse the arguments
  args = parser.parse_args()

  # setup log
  if args.verbose:
    log.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stdout, level=log.DEBUG)
    log.debug('ref-tile-path=' + args.ref_tile_path)
    log.debug('bucket=' + args.bucket)
    log.debug('year=' + args.year)
    log.debug('level=' + str(args.level))
    log.debug('tile-id=' + str(args.tile_id))
  else:
    log.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stdout)

  ################################################################################
  # download the speed tiles from aws and decompress
  if not args.local:
    log.debug('Download the speed tiles from AWS and decompress...')
    spdFileNames = []
    directory = "ref_working_dir/"
    shutil.rmtree(directory, ignore_errors=True)
    tile_hierarchy = TileHierarchy()
    key_prefix = args.year + "/"

    s3 = boto3.client('s3')

    weeks_per_year = 52
    week = 0
    while ( week < weeks_per_year):
      key = key_prefix + str(week) + "/"
      file_name = tile_hierarchy.levels[args.level].GetFile(args.tile_id, args.level)
      key += file_name
      try:
        file_path = os.path.dirname(key + ".gz" )
        if not os.path.exists(directory + file_path):
          try:
            os.makedirs(directory + file_path)
          except OSError as e:
            if e.errno != errno.EEXIST:
              raise
        with open(directory + key + ".gz" , "wb") as f:
          s3.download_fileobj(args.bucket, key + ".gz", f)
      except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] != "404":
          raise
      decompressedFile = gzip.GzipFile(directory + key + ".gz", mode='rb')
      with open(directory + key , 'w') as outfile:
        outfile.write(decompressedFile.read())

      spdFileNames.append(outfile.name)
      week += 1
  ################################################################################

  print 'getting avg speeds from list of protobuf speed tile extracts'
  if not args.local:
    log.debug('AWS speed processing...')
    speedListPerSegment = createAvgSpeedList(spdFileNames)
  else:
    log.debug('LOCAL speed processing...')
    speedListPerSegment = createAvgSpeedList(args.speedtile_list)
  
  print 'create reference speed tiles for each segment'
  createRefSpeedTile(args.ref_tile_path, args.ref_tile_file, speedListPerSegment)

  if args.verbose:
    log.debug('loop over segments ###############################################################################')
    for i, speed in enumerate(speedListPerSegment):
      log.debug('index=' + str(i) + ' | speeds=' + str(speed))

    log.debug('DONE loop over segments ###############################################################################')

  print 'done'

