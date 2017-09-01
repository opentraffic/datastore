#!/usr/bin/env python
import argparse
import random
import math
import os
import errno
import sys
import logging as log

try:
  import speedtile_pb2
except ImportError:
  print 'You need to generate protobuffer source via: protoc --python_out . --proto_path ../proto ../proto/*.proto'
  sys.exit(1)

#wget https://s3.amazonaws.com/speed-extracts/2017/0/0/002/415.spd.*.gz

###############################################################################
### tile 2415
### segment0{refspdlist[hour0:avgspd, hour1:avgspd, hour2:avgspd, etc]} -> sort lo to hi, index in to get refspd20:int, refspd40:int, refspd60:int, refspd80:int
### segment1{refspdlist[hour0:avgspd, hour1:avgspd, hour2:avgspd, etc]}
# get the avg speeds for a given segment for each hour (168 hr/day) over 52 weeks
def createAvgSpeedList(fileName):
  spdtile = speedtile_pb2.SpeedTile()
  with open(fileName, 'rb') as f:
    spdtile.ParseFromString(f.read())
    
  #get out the average speed
  avgspdlist = []
  for subtile in spdtile.subtiles:
    print 'total # of segments ' + str(subtile.totalSegments) 
    print '# of speeds for a subtile ' + str(len(subtile.speeds))
    for speed in subtile.speeds:
      if speed > 0:
        avgspdlist.append(speed)
  del spdtile
    
  print '# of valid average speeds in list ' + str(len(avgspdlist))
  #sort lo to hi
  avgspdlist.sort()  
  return avgspdlist
  
###############################################################################
  
def remove(path):
  try:
    os.remove(path)
  except OSError as e:
    if e.errno != errno.ENOENT:
      raise

###############################################################################
def write(path, name, tile, count):
  name += '.' + str(count)
  print path + name  
  with open(path + name, 'ab') as f:
    f.write(tile.SerializeToString())

###############################################################################

def createReferenceSpeedTiles(path, fileName, avgspdlist, count):
  #bucketize avg speeds into 20%, 40%, 60% and 80% reference speed buckets
  size = len(avgspdlist)
  p20 = avgspdlist[int(math.ceil((size * 20) / 100)) - 1]
  p40 = avgspdlist[int(math.ceil((size * 40) / 100)) - 1]
  p60 = avgspdlist[int(math.ceil((size * 60) / 100)) - 1]
  p80 = avgspdlist[int(math.ceil((size * 80) / 100)) - 1]
  
  print ('for '+ fileName + ' :: reference speeds(20/40/60/80) :: ' + str(p20) + ',' + str(p40) + ',' + str(p60) + ',' + str(p80))
  createRefSpeedTiles(path, fileName, count, p20, p40, p60, p80)
  

def createRefSpeedTiles(path, fileName, count, p20, p40, p60, p80):
  log.debug('createRefSpeedTiles ###############################################################################')

  subtile=None
  tile = speedtile_pb2.SpeedTile()
  subtile = tile.subtiles.add()
  #calculate 4 ref speeds for each segment
  subtile.referenceSpeed20.append(p20)
  subtile.referenceSpeed40.append(p40)
  subtile.referenceSpeed60.append(p60)
  subtile.referenceSpeed80.append(p80)

  print subtile

  #get the last one written
  if subtile is not None:
    write(path, fileName, subtile, count)
    del subtile
    del tile


#Read in protobuf files from the datastore output in AWS to read in the lengths, speeds & next segment ids and generate the segment speed files in proto output format
if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='Generate speed tiles', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
  parser.add_argument('--spd-tile', type=str, help='The public data extract speed tile containing the average speeds per segment per hour of day for one week', required=True)
  parser.add_argument('--ref-tile-path', type=str, help='The public data extract speed tile containing the average speeds per segment per hour of day for one week', required=True)
  parser.add_argument('--output-prefix', type=str, help='The file name prefix to give to output tiles. The first tile will have no suffix, after that they will be numbered starting at 1. e.g. tile.ref, tile.ref.1, tile.ref.2', default='tile.ref')
  parser.add_argument('--verbose', '-v', help='Turn on verbose output i.e. DEBUG level logging', action='store_true')

  # parse the arguments
  args = parser.parse_args()

  print 'getting avg speeds from pb speed tile extracts'
  avgspdlist = createAvgSpeedList(args.spd_tile)
  
  count = 0
  #print 'create reference speed tiles for each segment'
  createReferenceSpeedTiles(args.ref_tile_path, args.output_prefix, avgspdlist, count)

  if args.verbose:
    log.debug('loop over segments ###############################################################################')
    for k,v in segments.iteritems():
      log.debug('k=' + str(k) + ' | v=' + str(v))
    log.debug('DONE loop over segments ###############################################################################')

  print 'done'

