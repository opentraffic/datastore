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

#the step sizes, which increase as the high 2 bits increase to provide variable precision.
STEP_SIZES = { 1, 2, 5, 10 }

#offset of each step, derived from the above.
#STEP_OFFSET[i] = STEP_OFFSET[i-1] + 2^6 * STEP_SIZES[i-1]
STEP_OFFSETS = { 0, 64, 192, 512, 1152 }


class public_data_extract():
  #Read in Manila OSMLR tiles to generate the segment speed files to get the list of segment ids 
  #ie. week0_2017/level/tileid/tile.json (output same as directory structure output as OSMLR input)
  def loadOSMLR():
    osmlr_tile = tile_pb2.Tile()
    f = open(sys.argv[2], "rb")
    osmlr_tile.ParseFromString(f.read())
    f.close()

    return osmlr_tile

  #Read in flatbuffer tiles (168 hours in a week) from the datastore output in AWS to read in the speeds & next segment ids and generate the segment speed files
  #ie. 2017/week/level/tileid/tile.json (output same as directory structure output as OSMLR input)
  def loadFB(new_hour):
    buf = open(new_hour, 'rb').read()
    histogram = Histogram.GetRootAsHistogram(bytearray(buf), 0)
        
    return histogram
    
  def unquantise(self, byte):
    hi = 0
    lo = 0
    if (val < 0):
      hi = 2 | (((-val) & 64) >> 6)
      lo = (-val) & 63
    else:
      hi = (val & 192) >> 6
      lo = val & 63

    assert (hi >= 0);
    assert (hi < 4);
    assert (lo >= 0);
    assert (lo < 64);

    return STEP_OFFSETS[hi] + STEP_SIZES[hi] * lo


  #Iterates through the segments, listing the lengths
  def list_segment_data(level, tile, osmlr_tile, histogram, dy, hr):
    id = 0
    tileMatch = False
    segments_dict = {}
    entries = []
    #1-168 hours in week
    hpw = (hr + 1) * dy

    #the tile id should match the first osmlr segment id
    #Loop thru osmlr map and histogram map and when the osmlr id hits a match in the histo map, then capture length from osmlr
    #and rest of info in histogram for that segment id
    for osmlr in osmlr_tile.entries:
      length = 0
      o_segmentId = (id << 25) | (tile << 3) | level
      if id == 0:
        o_tileId = o_segmentId
        tileMatch = True
      id += 1

      if tileMatch:
        if osmlr.HasField("segment"):
          for loc_ref in osmlr.segment.lrps:
            if loc_ref.HasField('length'):
              length = length + loc_ref.length


        for h_index in range(histogram.SegmentsLength()):
          h_seg = histogram.Segments(h_index)
          h_segmentId = (h_seg.SegmentId() << 25) | (histogram.TileId())
          #print ("FROM OSMLR :: " + str(o_segmentId) + " tile id :: " + str(o_tileId) + ", FROM FB :: " + str(h_segmentId) + " tile id :: " + str(histogram.TileId()))

          for i in range(h_seg.EntriesLength()):
            #calculations
            e = h_seg.Entries(i);
            #TODO: decode duration bucket to # of secs.  then do length / time
            speed_secs = self.unquantise(e.DurationBucket().Bytes())
            speed_secs_list.append(speed_secs)
            speed = length / speed_secs
            speed_list.append(speed)

            segs_per_spd_count = sum(speed_list.count(speed))
            #seg_speed_arr {25:4, 30:10, 35:5}
            seg_speed_arr = {speed:speed_list.count(speed) for speed in speed_list}
            avg_duration = sum(seg_speed_arr.keys() * seg_speed_arr.values()) / segs_per_spd_count

            avg_speed = length / avg_duration
            #for i in range(speed_list):  speed_variance += (avg_speed - i) ** 2
            speed_variance += (avg_speed - (i in range(speed_list))) ** 2
            min_duration = min(speed_secs_list)

            next_segment_ids = [h_seg.NextSegmentIds(e.NextSegmentIdx())]
            next_segments_dict = {}
            if level is not 2:
              for next_id in next_segment_ids:
                queue_list.append(e.Queue())
                #for i in range(queue_list):  queue_variance += (avg_queue - i) ** 2
                avg_queue = sum(queue_list) / sum(queue_list.count(e.Queue()))
                queue_variance += (avg_queue - (i in range(queue_list))) ** 2

                delay = avg_duration - min_duration
                delay_list.append(delay)
                avg_delay = sum(delay_list) / sum(delay_list.count(delay))
                delay_variance += (avg_delay - (i in range(delay_list))) ** 2

                next_segments_dict[next_id] = {
                  #"prevalence": int(e.Count()),  #do we need this here?
                  "id" : next_id,
                  "delay_variance": delay_variance,
                  "queue_variance" : queue_variance,
                  "queue_length" : e.Queue()
                }
            entry_dict = {
              "entry" : hpw,
              "id" : o_segmentId,
              "avg_speed" : avg_speed,
              "speed_variance" : speed_variance,
              "prevalence" : int(e.Count()),  #how should we show this if we can't show actual counts?
              "next_segments" : next_segments_dict
            }
            entries.append(entry_dict)

            segments_dict[o_segmentId] = {
              "reference_speed": 37,
              "entries" : entries
            }
            print segments_dict
  
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
    if len(sys.argv) != 4:
      print "Usage:", sys.argv[0], "<level>, <OSMLR pbf file>, <flatbuffer(s)>"
      sys.exit(-1)

    #random.seed(123)
    level = int(sys.argv[1])
    tile = 0
    osmlr_input_file = sys.argv[2]
    print ("OSMLR input file: " + osmlr_input_file) 
    osmlr_tile = loadOSMLR()
    
    fbpath = str(sys.argv[3])
    tile = int(fbpath.split('/')[-1].split('.fb')[0])

    staticmonth_path = fbpath.rsplit('/', 4)[0]
    #for now, only loading 2 days worth instead of 7
    #for dy in range(1,8):
    for dy in range(1,3):
      day = fbpath.replace(fbpath.rsplit('/', 3)[0], str(dy))
      new_day = staticmonth_path + "/" + day

      #for hr in range(0,24):
      #for now, only loading 3 hours worth instead of 24
      for hr in range(0,4):
        hour = new_day.replace(new_day.rsplit('/', 2)[0], str(hr))
        new_hour = new_day.rsplit('/',3)[0] + "/" + hour

        histogram = loadFB(new_hour)
        print ("Flatbuffer input file: " + new_hour) 
        segment_list = list_segment_data(level, tile , osmlr_tile, histogram, dy, hr)
    
    #output the generated encoded json to a gzip
    #createOrdinalSegmentsOut(segment_list)

