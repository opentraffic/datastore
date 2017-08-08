#!/usr/bin/env python
import argparse
import random
import os
import errno
import sys

try:
  import segment_pb2
  import tile_pb2
  import speedtile_pb2
except ImportError:
  print 'You need to generate protobuffer source via: protoc --python_out . --proto_path ../proto ../proto/*.proto'
  sys.exit(1)

try:
  import io.opentraffic.datastore.flatbuffer.Entry
  import io.opentraffic.datastore.flatbuffer.Histogram
  import io.opentraffic.datastore.flatbuffer.Segment
  import io.opentraffic.datastore.flatbuffer.VehicleType
except ImportError:
  print 'You need to generate the flatbuffer soruce via: flatc --python ../src/main/fbs/histogram-tile.fbs'
  sys.exit(1)

#try this fat tile: wget https://s3.amazonaws.com/osmlr-tiles/v0.1/pbf/2/000/724/159.osmlr

def getIds(fileName):
  osmlr = tile_pb2.Tile()
  with open(fileName, 'rb') as f:
    osmlr.ParseFromString(f.read())

  #get out the segment ids
  segId = 0
  segmentIds = []
  for entry in osmlr.entries:
    if entry.segment:
      segmentIds.append(segId)
    else:
      segmentIds.append(-1)
    segId += 1

  del osmlr
  return segmentIds

def remove(path):
  try:
    print 'Removing ' + path
    os.remove(path)
  except OSError as e:
    if e.errno != errno.ENOENT:
      raise

def write(name, count, tile, should_remove):
  name += '.' + str(count)
  if should_remove:
    remove(name)
  print 'writing subtile to ' + name
  with open(name, 'ab') as f:
    f.write(tile.SerializeToString())
  print 'wrote subtile to ' + name

def next(startIndex, total, nextName):
  print 'creating new subtile starting at ' + str(startIndex)
  tile = speedtile_pb2.SpeedTile()
  subtile = tile.subtiles.add()
  if nextName:
    nextTile = speedtile_pb2.SpeedTile()
    nextSubtile = nextTile.subtiles.add()
  else:
    nextTile = tile
    nextSubtile = subtile
  for st in [subtile, nextSubtile]:
    #geo stuff
    st.level = 0      #TODO: get from osmlr
    st.index = 2415   #TODO: get from osmlr
    st.startSegmentIndex = startIndex
    st.totalSegments = total
    #time stuff
    st.rangeStart = 1483228800 #TODO: get from input
    st.rangeEnd = 1483833600   #TODO: get from input
    st.unitSize = 604800       #TODO: get from input
    st.entrySize = 3600        #TODO: get from input
    st.description = '168 ordinal hours of week 0 of year 2017' #TODO: get from input
  return tile, subtile, nextTile, nextSubtile

def simulate(segmentIds, fileName, subTileSize, nextName, separate):
  random.seed(0)

  #fake a segment for each entry in the osmlr
  tile = None
  nextTile = None
  subTileCount = 0
  first = True
  for i, sid in enumerate(segmentIds):
    #its time to write a subtile
    if i % subTileSize == 0:
      #writing tile
      if tile is not None:
        write(fileName, subTileCount, tile, first or separate)
        #writing next data if its separated
        if nextTile is not tile:
          write(nextName, subTileCount, nextTile, first or separate)
        #dont delete the files from this point on
        first = False
        #if the subtiles are to be separate increment
        if separate:
          subTileCount += 1
        #release all memory
        del subtile
        del tile
        del nextSubtile
        del nextTile
      #set up new pbf messages to write into
      tile, subtile, nextTile, nextSubtile = next(i, len(segmentIds), nextName)

    #continue making fake data
    segment = subtile.segments.add()
    if sid == -1: #need blank one to keep the indices correct
      continue
    segment.referenceSpeed = random.randint(20, 100)
    nextIds = [ (random.randint(0,2**21)<<25)|(subtile.index<<3)|subtile.level for i in range(0, random.randint(0,3)) ]
    for i in range(0, subtile.unitSize/subtile.entrySize):
      entry = segment.entries.add()
      entry.speed = random.randint(20, 100)
      entry.speedVariance = random.uniform(0,1)
      entry.prevalence = random.randint(1, 100)
      entry.nextSegmentIndex = len(subtile.nextSegments)
      entry.nextSegmentCount = len(nextIds)
      for nid in nextIds:
        nextSegment = nextSubtile.nextSegments.add();
        nextSegment.id = nid
        nextSegment.delay = random.randint(0,30)
        nextSegment.delayVariance = random.uniform(0,1)
        nextSegment.queueLength = random.randint(0,200)
        nextSegment.queueLengthVariance = random.uniform(0,1)

  #get the last one written
  if tile is not None:
    write(fileName, subTileCount, tile, first or separate)
    if nextTile is not tile:
      write(nextName, subTileCount, nextTile, first or separate)
    del subtile
    del tile
    del nextSubtile
    del nextTile

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='Generate fake speed tiles', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
  parser.add_argument('--output-prefix', type=str, help='The file name prefix to give to output tiles. The first tile will have no suffix, after that they will be numbered starting at 1. e.g. tile.spd, tile.spd.1, tile.spd.2', default='tile.spd')
  parser.add_argument('--max-segments', type=int, help='The maximum number of segments to have in a single subtile message', default=10000)
  parser.add_argument('--no-separate-subtiles', help='If present all subtiles will be in the same tile', action='store_true')
  parser.add_argument('--separate-next-segments-prefix', type=str, help='The prefix for the next segments output tiles if they should be separated from the primary speed entries. If omitted they will not be separate')
  parser.add_argument('--osmlr', type=str, help='The osmlr tile containing the relevant segments definitions')
  parser.add_argument('flatbuffers', metavar='N', type=str, nargs='+', help='The flatbuffer tiles for the time period in question')
  #TODO: add the time period argument
  #TODO: add the tile id argument until we can get it from osmlr
  args = parser.parse_args()

  print 'getting osmlr segments'
  ids = getIds(args.osmlr)
  
  print 'simulating 1 week of speeds at hourly intervals for ' + str(len(ids)) + ' segments'
  simulate(ids, args.output_prefix, args.max_segments, args.separate_next_segments_prefix, not args.no_separate_subtiles)

  print 'done'
