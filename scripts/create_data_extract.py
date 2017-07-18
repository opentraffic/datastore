#!/usr/bin/env python

import os
import sys
import tile_pb2
import random
import json, gzip


#Read in v0.1 geojson OSMLR tiles from AWS to generate segment speed files that 
#ie. week0_2017/level/0/tileid/segment_speeds.json (same as directory structure output as OSMLR input)
#https://s3.amazonaws.com/osmlr-tiles/v0.1/pbf/0/000/xxx.osmlr
def loadOSMLR():
  osmlr_tile = tile_pb2.Tile()
  f = open(sys.argv[1], "rb")
  osmlr_tile.ParseFromString(f.read())
  f.close()
  
  return osmlr_tile


#Iterates through the segments, listing the lengths
def list_segment_lengths(osmlr_tile, level, tile_id):
  id = 0
  segments_dict = {"segments":[]}
  for entry in osmlr_tile.entries:
    length = 0
    segment_id = (id << 25) + (tile_id << 3) + level
    if entry.HasField("segment"): 
      for loc_ref in entry.segment.lrps:
        if loc_ref.HasField('length'):
          length = length + loc_ref.length
      #print segment_id, level, tile_id, id, length
   
    next_segments_dict = {}
    for i in range(0,random.randint(0,4)):
      next_id = random.randint(0, 2**64)
      next_segments_dict[next_id] = {
        "prevalence": round(random.uniform(0.0, 10.0),2),
        "id" : next_id,
        "delay": random.randint(0, 100),
        "variance" : round(random.uniform(0.0, 2.0),2)
      }
      
    entries= []
    for hpw in range(0, 168):
      entry_dict = {
        "entry" : hpw,
        "id" : segment_id,  
        "speed" : random.randint(30, 100),
        "variance" : round(random.uniform(0.0, 2.0),2),
        "prevalence" : round(random.uniform(0.0, 10.0),2),
        "queue_length" : round((length * .20), 2),
        "next_segments" : next_segments_dict
      }
      entries.append(entry_dict)
  
      
  segment_dict = {
    segment_id : {
      "reference_speed": 37,
      "entries" : entries
    }
  }
    
  segments_dict.update(segment_dict)
  id += 1
  
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
    
  with gzip.GzipFile('data_extracts/test_segment_speed.gz', 'w') as fout:
     ordinal_result = (json.dumps(ordinal_out, separators=(',', ':'))).encode('utf-8') 
     fout.write(ordinal_result)
  

#contain hourly avg speeds per week over 1 calendar year (periodic)open
#def createPeriodicSegmentsOut():

  
# Main procedure:  Reads an OSMLR tile and prints all segment Ids and lengths
if __name__ == '__main__':
  if len(sys.argv) != 4:
    print "Usage:", sys.argv[0], "<OSMLR pbf file> <level> <tile_id>"
    sys.exit(-1)

  osmlr_tile = loadOSMLR()
  # Read the existing address book.
  level = int(sys.argv[2])
  tile_id = int(sys.argv[3])

  segment_list = list_segment_lengths(osmlr_tile, level, tile_id)
  #output the generated encoded json to a gzip
  createOrdinalSegmentsOut(segment_list)
  



