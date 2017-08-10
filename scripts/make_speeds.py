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
  import flatbuffers
  from Histogram import Histogram
  from Segment import Segment
  from Entry import Entry
  from VehicleType import VehicleType
except ImportError:
  print 'You need to generate the flatbuffer source via: sed -e "/namespace.*/d" ../src/main/fbs/histogram-tile.fbs > schema.fbs && flatc --python schema.fbs'
  sys.exit(1)

#try this fat tile: wget https://s3.amazonaws.com/datastore_output_prod/2017/1/1/0/0/2415.fb

LEVEL_BITS = 3
TILE_INDEX_BITS = 22
SEGMENT_INDEX_BITS = 21

LEVEL_MASK = (2**LEVEL_BITS) - 1
TILE_INDEX_MASK = (2**TILE_INDEX_BITS) - 1
SEGMENT_INDEX_MASK = (2**SEGMENT_INDEX_BITS) - 1

def get_level(segment_id):
  return segment_id & LEVEL_MASK
def get_tile_index(segment_id):
  return (segment_id >> LEVEL_BITS) & TILE_INDEX_MASK
def get_segment_index(segment_id):
  return (segment_id >> (LEVEL_BITS + TILE_INDEX_BITS)) & SEGMENT_INDEX_MASK

def getHistogram(path, target_level, target_tile_id):
  print('Looping for level=' + str(target_level) + ' and tile_id=' + str(target_tile_id) + ' here:' + path)
  fbList = []
  for root, dirs, files in os.walk(path):
    for file in files:
      if (root + os.sep + file).endswith('.fb'):
        buf = open(root + os.sep + file, 'rb').read()
        hist = Histogram.GetRootAsHistogram(bytearray(buf), 0)
        level = get_level(hist.TileId())
        tile_index = get_tile_index(hist.TileId())
        if ((level == target_level) and (tile_index == target_tile_id)):
          fbList.append(hist)

  histogram = {"histogram" : fbList}
  #processHistogram(histogram)

  return histogram

#def processHistogram(histoList):


#try this fat tile: wget https://s3.amazonaws.com/osmlr-tiles/v0.1/pbf/0/002/415.osmlr

def getLengths(fileName):
  osmlr = tile_pb2.Tile()
  with open(fileName, 'rb') as f:
    osmlr.ParseFromString(f.read())

  #get out the length
  lengths = []
  for entry in osmlr.entries:
    length = 0
    if entry.segment:
      for loc_ref in entry.segment.lrps:
        if loc_ref.length:
          length = length + loc_ref.length

      lengths.append(length)
    else:
      lengths.append(-1)

  del osmlr
  return lengths

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

def next(startIndex, total, nextName, subtileSegments):
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
    st.subtileSegments = subtileSegments
    #time stuff
    st.rangeStart = 1483228800 #TODO: get from input
    st.rangeEnd = 1483833600   #TODO: get from input
    st.unitSize = 604800       #TODO: get from input
    st.entrySize = 3600        #TODO: get from input
    st.description = '168 ordinal hours of week 0 of year 2017' #TODO: get from input
  return tile, subtile, nextTile, nextSubtile

def createSpeedTiles(lengths, fileName, subTileSize, nextName, separate):
  tile = None
  nextTile = None
  subTileCount = 0
  first = True
  for i, length in enumerate(lengths):
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
      tile, subtile, nextTile, nextSubtile = next(i, len(lengths), nextName, subTileSize)

    #continue making fake data
    subtile.referenceSpeeds.append(random.randint(20, 100) if length != -1 else 0)
    #dead osmlr ids have no next segment data
    nextIds = [ (random.randint(0,2**21)<<25)|(subtile.index<<3)|subtile.level for i in range(0, random.randint(0,3)) ] if length != -1 else []
    #do all the entries
    for i in range(0, subtile.unitSize/subtile.entrySize):
      #any time its a dead one we put in 0's for the data
      subtile.speeds.append(random.randint(20, 100) if length != -1 else 0)
      subtile.speedVariances.append(int(random.uniform(0,127.5) * 2 if length != -1 else 0))
      subtile.prevalences.append(random.randint(1, 100) if length != -1 else 0)
      subtile.nextSegmentIndices.append(len(subtile.nextSegmentIds) if length != -1 else 0)
      subtile.nextSegmentCounts.append(len(nextIds) if length != -1 else 0)
      for nid in nextIds:
        nextSubtile.nextSegmentIds.append(nid)
        nextSubtile.nextSegmentDelays.append(random.randint(0,30))
        nextSubtile.nextSegmentDelayVariances.append(int(random.uniform(0,100)))
        nextSubtile.nextSegmentQueueLengths.append(random.randint(0,200))
        nextSubtile.nextSegmentQueueLengthVariances.append(int(random.uniform(0,200)))

  #get the last one written
  if tile is not None:
    write(fileName, subTileCount, tile, first or separate)
    if nextTile is not tile:
      write(nextName, subTileCount, nextTile, first or separate)
    del subtile
    del tile
    del nextSubtile
    del nextTile
    
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
      tile, subtile, nextTile, nextSubtile = next(i, len(segmentIds), nextName, subTileSize)

    #continue making fake data
    subtile.referenceSpeeds.append(random.randint(20, 100) if sid != -1 else 0)
    #dead osmlr ids have no next segment data
    nextIds = [ (random.randint(0,2**21)<<25)|(subtile.index<<3)|subtile.level for i in range(0, random.randint(0,3)) ] if sid != -1 else []
    #do all the entries
    for i in range(0, subtile.unitSize/subtile.entrySize):
      #any time its a dead one we put in 0's for the data
      subtile.speeds.append(random.randint(20, 100) if sid != -1 else 0)
      subtile.speedVariances.append(int(random.uniform(0,127.5) * 2 if sid != -1 else 0))
      subtile.prevalences.append(random.randint(1, 100) if sid != -1 else 0)
      subtile.nextSegmentIndices.append(len(subtile.nextSegmentIds) if sid != -1 else 0)
      subtile.nextSegmentCounts.append(len(nextIds) if sid != -1 else 0)
      for nid in nextIds:
        nextSubtile.nextSegmentIds.append(nid)
        nextSubtile.nextSegmentDelays.append(random.randint(0,30))
        nextSubtile.nextSegmentDelayVariances.append(int(random.uniform(0,100)))
        nextSubtile.nextSegmentQueueLengths.append(random.randint(0,200))
        nextSubtile.nextSegmentQueueLengthVariances.append(int(random.uniform(0,200)))

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
  parser.add_argument('--fb-path', type=str, help='The flatbuffer tile path to load the files necessary for the time period given')
  parser.add_argument('--level', type=int, help='The level to target')
  parser.add_argument('--tile-id', type=int, help='The tile id to target')
  #TODO: add the time period argument
  args = parser.parse_args()

  print 'getting osmlr lengths'
  lengths = getLengths(args.osmlr)

  print 'getting speed averages from fb Histogram'
  histogram = getHistogram(args.fb_path, args.level, args.tile_id)
  
  print 'simulating 1 week of speeds at hourly intervals for ' + str(len(lengths)) + ' segments'
  #simulate(lengths, args.output_prefix, args.max_segments, args.separate_next_segments_prefix, not args.no_separate_subtiles)
  #createSpeedTiles(lengths, args.output_prefix, args.max_segments, args.separate_next_segments_prefix, not args.no_separate_subtiles)

  print 'done'
