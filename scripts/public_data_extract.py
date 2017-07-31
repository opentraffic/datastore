#!/usr/bin/env python

import os
import sys
import random
import tile_pb2
import flatbuffers
import json, gzip
from Histogram import Histogram
from Segment import Segment
from Entry import Entry
from VehicleType import VehicleType

class public_data_extract():

  def intersects(epochHour):
    return index == epochHour

  
  def intersects(minEpochSeconds, maxEpochSeconds):
    minBucket = minEpochSeconds/3600L
    maxBucket = maxEpochSeconds/3600L
    return index >= minBucket and index <= maxBucket
  

  #Read in Manila OSMLR tiles to generate the segment speed files to get the list of segment ids 
  #ie. week0_2017/level/tileid/tile.json (output same as directory structure output as OSMLR input)
  def loadOSMLR():
    osmlr_tile = tile_pb2.Tile()
    f = open(sys.argv[3], "rb")
    osmlr_tile.ParseFromString(f.read())
    f.close()
    
    return osmlr_tile

  #Read in flatbuffer tiles (168 hours in a week) from the datastore output in AWS to read in the speeds & next segment ids and generate the segment speed files
  #ie. 2017/week/level/tileid/tile.json (output same as directory structure output as OSMLR input)
  def loadFB(new_hour):
    buf = open(new_hour, 'rb').read()
    histogram = Histogram.GetRootAsHistogram(bytearray(buf), 0)
        
    return histogram
    

  #Iterates through the segments, listing the lengths
  def list_segment_data(level, tile_id, fb_tile, osmlr_tile, histogram):
    id = 0
    index = 0
    segments_dict = {}
    entries = []
    hist_segments = []
    
    #iterate over the FB segment ids
    print ("Histogram Tile Id:: " + str(histogram.TileId()))
    for hist_idx in range(histogram.SegmentsLength()):
      #do only if speed segment exists
      if histogram.Segments(hist_idx).SegmentId() is not 0:
        hist_segments.append(histogram.Segments(hist_idx).SegmentId())

    print hist_segments    

    for entry in osmlr_tile.entries:
      length = 0
      segment_id = (id << 25) | (tile_id << 3) | level
      print ("FROM OSMLR :: " + str(segment_id))
      id += 1
      if segment_id in hist_segments:
        if entry.HasField("segment"): 
          for loc_ref in entry.segment.lrps:
            if loc_ref.HasField('length'):
              length = length + loc_ref.length
         
          print segment_id, level, tile_id, length
    
    '''
    while (histogram.TileId() == fb_tile and index < hist_idx):
      print ("Histogram Tile Id:: " + histogram.TileId())
      hist_segment = histogram.Segments(index)
      index += 1
      segmentId = (hist_segment.SegmentId() << (3L + 22L)) | histogram.TileId()
      vtype = [histogram.VehicleType()]
      print ("segment id:: " + str(segmentId) + ", vehicle type:: " + str(vtype))
      
              
      for i in range(0,segment.EntriesLength()):
        e = segment.Entries(i);
        if (TimeBucket.intersects is not (e.epochHour())):
          continue
        nextSegmentId = segment.NextSegmentIds(e.NextSegmentIdx())
        #duration = 
        count = int(e.Count())
        print e, nextSegmentId, count         


    #next_segment_ids = [random.randrange(1000000000,9999999999) for i in range(0,random.randint(0,3))]
    next_segment_ids = [histogram.NextSegmentIds(i) for i in range(0, histogram.NextSegmentIdsLength())]
    print next_segment_ids  
    #Iterate over hours in week
    for hpw in range(histogram.EntriesLength()):
      next_segments_dict = {}
      if level is not 2:
        for next_id in next_segment_ids:
          next_segments_dict[next_id] = {
            "prevalence": round(random.uniform(0.0, 10.0),2),
            "id" : next_id,
            "delay_variance": random.randint(0, 100),
            "queue_variance" : round(random.uniform(0.0, 2.0),2),
            "queue_length" : round((length * .20), 2)
          }
      entry_dict = {
        "entry" : hpw,
        "id" : segment_id,  
        "speed" : random.randint(30, 100),
        "speed_variance" : round(random.uniform(0.0, 2.0),2),
        "prevalence" : round(random.uniform(0.0, 10.0),2),
        "next_segments" : next_segments_dict
      }
      entries.append(entry_dict)
  
      segments_dict[segment_id] = {
        "reference_speed": random.randint(30, 100),
        "entries" : entries
      }
    '''
    return segments_dict    


  #contain weekly avg speeds over 1 calendar year (ordinal)
  #dump segment ids to json into the "ordinal" dummy json format
  def createOrdinalSegmentsOut(segment_list):
    #create a dict, append to it and output the segement_ids
    ordinal_out = {
      "range_type" : "ordinal",
      "unit_type" : "hour",
      "year" : 2017,
      "week" : 0,
      "unit_entries" : 168,
      "description" : "168 ordinal hours of week 0 of year 2017",
      "segments" : segment_list
      }
      
  
  #In case we need to do any of this later
  '''
  if '#' not in sys.argv[2]:
    output_path = 'data-extracts/' + str(ordinal_out['year']) + '/' + str(ordinal_out['week']) + '/' + str(int(sys.argv[2])) + '/' + sys.argv[3] + '/'
  else:
    #for level 2, we append 2 tile levels with # (ie. 000/601 = 000#601)
    tile_ids = sys.argv[2].split("#")
    output_path = 'data-extracts/' + str(ordinal_out['year']) + '/' + str(ordinal_out['week']) + '/' + str(int(sys.argv[2])) + '/' + str(tile_ids[0]) + '/' + str(tile_ids[1]) + '/'

  if not os.path.exists(os.path.dirname(output_path)):
    try:
      os.makedirs(os.path.dirname(output_path))
    except OSError as exc: # Guard against race condition
        if exc.errno != errno.EEXIST:
            raise    

  #should we output to stdout and let user decide where to put it
  #sys.stdout.write(json.dumps(ordinal_out, separators=(',', ':')))
  #sys.stdout.flush()
  #for now, need to output to file for s3
  with open(output_path + input_file + '.json', 'w') as fout:
     ordinal_result = (json.dumps(ordinal_out, separators=(',', ':'))).encode('utf-8')
     fout.write(ordinal_result)
  '''          
  '''
  with gzip.GzipFile(output_path + input_file + '.json' + '.gz', 'w') as fout:
     ordinal_result = (json.dumps(ordinal_out, separators=(',', ':'))).encode('utf-8') 
     fout.write(ordinal_result)
  '''


  # Main procedure:  Reads an OSMLR tile and outputs dummy json segment speed data
  if __name__ == '__main__':
    if len(sys.argv) != 5:
      print "Usage:", sys.argv[0], "<level>, <tile_id>, <OSMLR pbf file>, <flatbuffer(s)>"
      sys.exit(-1)

    #random.seed(123)
    level = int(sys.argv[1])
    tile_id = int(sys.argv[2])  
    osmlr_input_file = sys.argv[3]
    print ("OSMLR input file: " + osmlr_input_file) 
    osmlr_tile = loadOSMLR()
    
    fbpath = str(sys.argv[4])
    fb_tile = fbpath.split('/')[-1].split('.fb')[0]
    print ("level :: " + str(level) + ", osmlr tile  :: " + str(tile_id) + ", fb tile :: " + str(fb_tile))

    staticmonth_path = fbpath.rsplit('/', 4)[0]
    #for now, only loading 2 days worth instead of 7
    #for wk in range(1,8):
    for wk in range(1,3):
      week = fbpath.replace(fbpath.rsplit('/', 3)[0], str(wk))
      new_week = staticmonth_path + "/" + week

      #for hr in range(0,24):
      #for now, only loading 3 hours worth instead of 24
      for hr in range(0,4):
        hour = new_week.replace(new_week.rsplit('/', 2)[0], str(hr))
        new_hour = new_week.rsplit('/',3)[0] + "/" + hour

        histogram = loadFB(new_hour)
        #TODO: load in a list of fb to capture the hours in a week 
        print ("Flatbuffer input file: " + new_hour) 
        segment_list = list_segment_data(level, tile_id, fb_tile, osmlr_tile, histogram)
    
    #output the generated encoded json to a gzip
    #createOrdinalSegmentsOut(segment_list)

