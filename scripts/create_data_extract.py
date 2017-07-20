#!/usr/bin/env python

import os
import sys
import tile_pb2
import random
import json, gzip


#Read in v0.1 geojson OSMLR tiles from AWS to generate segment speed files that 
#ie. week0_2017/level/0/tileid/segment_speeds.json (same as directory structure output as OSMLR input)
#https://s3.amazonaws.com/osmlr-tiles/v0.1/pbf/0/000/747.osmlr
def loadOSMLR():
  osmlr_tile = tile_pb2.Tile()
  f = open(sys.argv[1], "rb")
  osmlr_tile.ParseFromString(f.read())
  f.close()
  
  return osmlr_tile


#Iterates through the segments, listing the lengths
def list_segment_lengths(osmlr_tile, level, tile_id):
  id = 0
  segments_dict = {'segments':{}}
  tile_ids = []
  tile_ids = (tile_id if '#' not in sys.argv[3] else sys.argv[3].split("#"))
  print tile_ids
  for entry in osmlr_tile.entries:
    length = 0
    segment_id = (id << 25) + (int(tile_ids[0]) << 3) + level
    id += 1
    if entry.HasField("segment"): 
      for loc_ref in entry.segment.lrps:
        if loc_ref.HasField('length'):
          length = length + loc_ref.length
      #print segment_id, level, tile_id, id, length
   
    entries= []
    next_segment_ids = [random.randrange(1000000000,9999999999) for i in range(0,random.randint(0,3))]
    #hours per week = entry
    for hpw in range(0, 168):
      next_segments_dict = {}
      if int(sys.argv[2]) is not 2:
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
  
      segments_dict['segments'][segment_id] = {
        "reference_speed": random.randint(30, 100),
        "entries" : entries
      }
    
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

  if '#' not in sys.argv[3]:
    output_path = 'data-extracts/week' + str(ordinal_out['week']) + '_' + str(ordinal_out['year']) + '/' + str(int(sys.argv[2])) + '/' + sys.argv[3] + '/'
  else:
    #for level 2, we append 2 tile levels with # (ie. 000/601 = 000#601)
    tile_ids = sys.argv[3].split("#")
    output_path = 'data-extracts/week' + str(ordinal_out['week']) + '_' + str(ordinal_out['year']) + '/' + str(int(sys.argv[2])) + '/' + str(tile_ids[0]) + '/' + str(tile_ids[1]) + '/'

  if not os.path.exists(os.path.dirname(output_path)):
    try:
      os.makedirs(os.path.dirname(output_path))
    except OSError as exc: # Guard against race condition
        if exc.errno != errno.EEXIST:
            raise

  with gzip.GzipFile(output_path + sys.argv[4] + '.gz', 'w') as fout:
     ordinal_result = (json.dumps(ordinal_out, separators=(',', ':'))).encode('utf-8') 
     fout.write(ordinal_result)
  

#contain hourly avg speeds per week over 1 calendar year (periodic)open
#def createPeriodicSegmentsOut():

  
# Main procedure:  Reads an OSMLR tile and prints all segment Ids and lengths
if __name__ == '__main__':
  if len(sys.argv) != 5:
    print "Usage:", sys.argv[0], "<OSMLR pbf file> <level> <tile_id> <output_file>"
    sys.exit(-1)

  random.seed(123)

  osmlr_tile = loadOSMLR()
  # Read the existing address book. /2/000/601/683
  level = int(sys.argv[2])  #ie. 0
  tile_id = sys.argv[3] #for level 2, just append 2 tile levels with # (ie. 000/601 = 000#601)
  osmlr = int(sys.argv[4]) #ie. 683

  segment_list = list_segment_lengths(osmlr_tile, level, tile_id)
  #output the generated encoded json to a gzip
  createOrdinalSegmentsOut(segment_list)
  



