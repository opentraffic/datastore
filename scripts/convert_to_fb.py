#!/usr/bin/env python
""" convert Reporter output to Flatbuffer """
import os
import sys
import argparse
import glob
import time
import subprocess
import itertools


def convert(dictionary):
  print('[INFO] running conversion process')
    
  print len(dictionary)
  for key, val in dictionary.items():
  
    epoch_seconds=str(key[3])
    hour = str(int(epoch_seconds.split('_')[0]) // 3600 % 24)
    tile_bucket_hour = str(int(epoch_seconds.split('_')[0])/3600)
    level = str(key[1])
    tile_index = str(key[2])
    tile_id = str((int(tile_index) << 3) | int(level))
    date = "/2017/1/1/"
    file_path = args.datastore_bucket + date + hour + "/" + level + '/' 
    out_dir = os.path.dirname(file_path)
    if not os.path.exists(out_dir):
      os.makedirs(out_dir)
    
    fb_out_file = out_dir + '/' + tile_index + '.fb'
    time_bucket = glob.glob(args.reporter_path + '/' + epoch_seconds + '/' + level + '/' + tile_index + '/*')
    print time_bucket
    
    try:
      subprocess.check_output(['target/datastore-histogram-tile-writer', '-b', tile_bucket_hour, '-t', tile_id, '-v', '-f', fb_out_file] + time_bucket, universal_newlines=True, stderr=subprocess.STDOUT)

    except subprocess.CalledProcessError as tilewriter:
      print('[ERROR] Failed running datastore-histogram-tile-writer:', tilewriter.returncode, tilewriter.output)
      sys.exit([tilewriter.returncode])
    print('[INFO] Finished running conversion')


def build_dictionary(reporter_buckets, bucket_interval, reporter_path):
    """ create a dictionary with a key of type tuple of (time bucket,
    tile_level, tile_index), with the value as a list of
    files: dictionary = {(23, 32, 43): 'tuple as key'}
    dictionary[time_bucket, tile_level, tile_index] = ['/path/1', '/path/2'] """
    dictionary = {}
    for bucket in reporter_buckets:
      epoch_start_seconds = int(bucket.split('_')[0])
      epoch_end_seconds = int(bucket.split('_')[1])

      # calculate the tile id: shift the tile
      # index 3 bits to the left then add the
      # tile level.
      for root, dirs, files in os.walk(reporter_path + '/' + bucket):
        tile_level = root.rsplit('/',3)[2]
        tile_index = root.rsplit('/',3)[3]

        for file in files:
          if (root + os.sep + file):
            # create the dictionary, sorting on time_bucket, tile_level, tile_index
            for time_bucket in range(epoch_start_seconds/bucket_interval, epoch_end_seconds/bucket_interval + 1):
              if (time_bucket, tile_level, tile_index, bucket) in dictionary:
                  dictionary[(time_bucket, tile_level, tile_index, bucket)]
              else:
                  dictionary[(time_bucket, tile_level, tile_index, bucket)] = [bucket]
    return dictionary

            
if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='Generate flatbuffer tiles from Reporter output', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
  parser.add_argument('--reporter-path', type=str, help='The directory path to the root of the Reporter output containing the epochHour generated folders.')
  parser.add_argument('--datastore-bucket', type=str, help='The directory path to the where the flatbuffer files are generated.')
  args = parser.parse_args()
  
  bucket_interval = 3600 # an hour would be 3600
 
  reporter_path = args.reporter_path
  datastore_bucket = args.datastore_bucket

  reporter_buckets = [f for f in next(os.walk(reporter_path))[1]]
  dictionary = build_dictionary(reporter_buckets, bucket_interval, reporter_path)
  convert(dictionary)
  
  print 'done'            
