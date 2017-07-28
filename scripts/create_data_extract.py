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
  segments_dict = {}
  for entry in osmlr_tile.entries:
    length = 0
    segment_id = (id << 25) | (tile_id << 3) | level
    id += 1
    if entry.HasField("segment"): 
      for loc_ref in entry.segment.lrps:
        if loc_ref.HasField('length'):
          length = length + loc_ref.length
        print segment_id, level, tile_id, id, length
   
    entries= []
    next_segment_ids = [random.randrange(1000000000,9999999999) for i in range(0,random.randint(0,3))]
    #hours per week = entry
    for hpw in range(0, 168):
      next_segments_dict = {}
      if level is not 2:
        for next_id in next_segment_ids:
          next_segments_dict[next_id] = {
            "p": round(random.uniform(0.0, 10.0),2), #prevalence
            "id" : next_id,
            "dv": random.randint(0, 100), #delay_variance
            "qv" : round(random.uniform(0.0, 2.0),2), #queue_variance
            "ql" : round((length * .20), 2) #queue_length
          }
      entry_dict = {
        "e" : hpw, #entry
        "id" : segment_id,  
        "sp" : random.randint(30, 100), #speed
        "spv" : round(random.uniform(0.0, 2.0),2), #speed_variance
        "p" : round(random.uniform(0.0, 10.0),2), #prevalence
        "nsegs" : next_segments_dict #next_segments
      }
      entries.append(entry_dict)
  
      segments_dict[segment_id] = {
        "rsp": random.randint(30, 100), #reference_speed
        "ets" : entries #entries
      }
    
  return segments_dict


#contain weekly avg speeds over 1 calendar year (ordinal)
#dump segment ids to json into the "ordinal" dummy json format
def createOrdinalSegmentsOut(segment_list):
  #create a dict, append to it and output the segement_ids
  ordinal_out = {
    "rt" : "ordinal", #range_type
    "ut" : "hour", #unit_type
    "y" : 2017, #year
    "w" : 0, #week
    "ue" : 168, #unit_entries
    "d" : "168 ordinal hours of week 0 of year 2017", #description
    "segs" : segment_list #segments
    }


  #In case we need to do any of this later
  if '#' not in sys.argv[3]:
    output_path = 'data-extracts/' + str(ordinal_out['y']) + '/' + str(ordinal_out['w']) + '/' + str(int(sys.argv[2])) + '/' + sys.argv[3] + '/'
  else:
    #for level 2, we append 2 tile levels with # (ie. 000/601 = 000#601)
    tile_ids = sys.argv[3].split("#")
    output_path = 'data-extracts/' + str(ordinal_out['y']) + '/' + str(ordinal_out['w']) + '/' + str(int(sys.argv[2])) + '/' + str(tile_ids[0]) + '/' + str(tile_ids[1]) + '/'

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
  with gzip.GzipFile(output_path + input_file + '.json' + '.gz', 'w') as fout:
     ordinal_result = (json.dumps(ordinal_out, separators=(',', ':'))).encode('utf-8') 
     fout.write(ordinal_result)
'''

# Main procedure:  Reads an OSMLR tile and outputs dummy json segment speed data
if __name__ == '__main__':
  if len(sys.argv) != 4:
    print "Usage:", sys.argv[0], "<OSMLR pbf file>, <level>, <tile_id>"
    sys.exit(-1)

  random.seed(123)

  osmlr_tile = loadOSMLR()
  # Read the existing address book. /0/002/415
  input_file = sys.argv[1].split('.osmlr')[0].rsplit('/', 1)[1]
  print ("input file: " + input_file) #ie. 415
  level = int(sys.argv[2])  #ie. 0
  tile_id = int(sys.argv[3]) #ie. 2415

  segment_list = list_segment_lengths(osmlr_tile, level, tile_id)
  #output the generated encoded json to a gzip
  createOrdinalSegmentsOut(segment_list)



