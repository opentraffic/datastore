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

def createAvgSpeedList(fileNameList):
  #each segment has its own list of speeds, we dont know how many segments for the avg speed list to start with
  speedListPerSegment = []

  #need to loop thru all of the speed tiles for a given tile id
  for fileName in fileNameList:
    #lets load the protobuf speed tile
    spdtile = speedtile_pb2.SpeedTile()
    with open(fileName, 'rb') as f:
      spdtile.ParseFromString(f.read())

    #we now need to retrieve all of the speeds for each segment in this tile
    for subtile in spdtile.subtiles:

      #make sure that there are enough lists in the list for all segments
      missing = subtile.totalSegments - len(speedListPerSegment)
      if missing > 0:
        speedListPerSegment.extend([ [] for i in range(0, missing) ])
      #print 'total # created in speedListPerSegment ' + str(len(speedListPerSegment))

      for i, speed in enumerate(subtile.speeds):
        if speed and speed > 0:
          speedListPerSegment.insert(subtile.startSegmentIndex + i, speed)
          #print 'index=' + str(i) + ' | speeds=' + str(speed) + '| startSegmentIndex+1= ' + str(subtile.startSegmentIndex + i)

      speedListPerSegment = filter(None, speedListPerSegment)
      #sort hi to lo
      speedListPerSegment.sort()

  return speedListPerSegment

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


  #for each segment
  print 'speedListPerSegment length: ' + str(len(speedListPerSegment))
  #bucketize avg speeds into 20%, 40%, 60% and 80% reference speed buckets
  size = len(speedListPerSegment)
  print speedListPerSegment[int(size * .2)]
  print speedListPerSegment[int(size * .4)]
  print speedListPerSegment[int(size * .6)]
  print speedListPerSegment[int(size * .8)]
  st.referenceSpeed20.append(speedListPerSegment[int(size * .2)])
  st.referenceSpeed40.append(speedListPerSegment[int(size * .4)])
  st.referenceSpeed60.append(speedListPerSegment[int(size * .6)])
  st.referenceSpeed80.append(speedListPerSegment[int(size * .8)])

  #write it out
  with open(path + fileName, 'ab') as f:
    f.write(tile.SerializeToString())

#Read in protobuf files from the datastore output in AWS to read in the lengths, speeds & next segment ids and generate the segment speed files in proto output format
if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='Generate speed tiles', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
  parser.add_argument('--speedtile-list', type=str, nargs='+', help='A list of the PDE speed tiles containing the average speeds per segment per hour of day for one week')
  parser.add_argument('--ref-tile-path', type=str, help='The public data extract speed tile containing the average speeds per segment per hour of day for one week', required=True)
  parser.add_argument('--output-prefix', type=str, help='The file name prefix to give to output tile.')
  parser.add_argument('--verbose', '-v', help='Turn on verbose output i.e. DEBUG level logging', action='store_true')

  # parse the arguments
  args = parser.parse_args()

  print 'getting avg speeds from list of protobuf speed tile extracts'
  speedListPerSegment = createAvgSpeedList(args.speedtile_list)
  
  #print 'create reference speed tiles for each segment'
  createRefSpeedTile(args.ref_tile_path, args.output_prefix, speedListPerSegment)

  if args.verbose:
    log.debug('loop over segments ###############################################################################')
    for i, speed in enumerate(speedListPerSegment):
      log.debug('index=' + str(i) + ' | speeds=' + str(speed))

    log.debug('DONE loop over segments ###############################################################################')

  print 'done'

