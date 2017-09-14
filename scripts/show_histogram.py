#!/usr/bin/env python
import argparse
import random
import os
import errno
import sys
import logging
import pprint

log = logging.getLogger('make_speeds')

try:
  import flatbuffers
  from dsfb.Histogram import Histogram
  from dsfb.Segment import Segment
  from dsfb.Entry import Entry
  from dsfb.VehicleType import VehicleType
except ImportError:
  print 'You need to generate the flatbuffer source via: sed -e "s/namespace.*/namespace dsfb;/g" ../src/main/fbs/histogram-tile.fbs > schema.fbs && flatc --python schema.fbs'
  sys.exit(1)

#try this fat tile: wget https://s3.amazonaws.com/datastore_output_prod/2017/1/1/0/0/2415.fb
###############################################################################
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


###############################################################################
#the step sizes, which increase as the high 2 bits increase to provide variable precision.
STEP_SIZES = [ 1, 2, 5, 10 ]#offset of each step, derived from the above.
STEP_OFFSETS = [ 0, 64, 192, 512, 1152 ]

def unquantise(val):
  hi = 0
  lo = 0
  if val < 0:
    hi = 2 | (((-val) & 64) >> 6)
    lo = (-val) & 63
  else:
    hi = (val & 192) >> 6
    lo = val & 63
  if hi >= 0 and hi < 4 and lo >= 0 and lo < 64:
    return STEP_OFFSETS[hi] + STEP_SIZES[hi] * lo
  raise (hi, lo)

###############################################################################
def getSegments(path):
  segments = {}
  with open(path) as filehandle:
    log.info('Loading ' + path + '...')
    hist = Histogram.GetRootAsHistogram(bytearray(filehandle.read()), 0)
  level = get_level(hist.TileId())
  tile_index = get_tile_index(hist.TileId())
  log.info('Processing ' + path + '...')
  #for each segment
  for i in range(0, hist.SegmentsLength()):
    segment = hist.Segments(i)
    if segment.EntriesLength() > 0:
      processSegment(segments, segment)
  del hist
  return segments

###############################################################################
def processSegment(segments, segment):
  for i in range(0, segment.EntriesLength()):
    e = segment.Entries(i)

    #get the right segment
    if segment.SegmentId() not in segments:
      segments[segment.SegmentId()] = { }
    hours = segments[segment.SegmentId()]

    #get the right hour in there
    if e.EpochHour() not in hours:
       hours[e.EpochHour()] = { }
    nexts = hours[e.EpochHour()]

    #if you dont have the right next segment in there
    if segment.NextSegmentIds(e.NextSegmentIdx()) not in nexts:
      nexts[segment.NextSegmentIds(e.NextSegmentIdx())] = {'count': 0, 'duration': 0, 'queue': 0 }
    totals = nexts[segment.NextSegmentIds(e.NextSegmentIdx())]

    #continuing a previous pair
    totals['count'] += e.Count()
    totals['duration'] += unquantise(e.DurationBucket()) * e.Count()
    totals['queue'] += (e.Queue()/255.0) * e.Count()

#Read in OSMLR & flatbuffer tiles from the datastore output in AWS to read in the lengths, speeds & next segment ids and generate the segment speed files in proto output format
if __name__ == "__main__":
  if log.level == logging.NOTSET:
    log.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(fmt='%(asctime)s %(levelname)s %(message)s'))
    log.addHandler(handler)

  print 'getting speed averages from fb Histogram'
  segments = getSegments(sys.argv[1])
  pprint.PrettyPrinter(indent=4).pprint(segments)
