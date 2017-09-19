#!/usr/bin/env python3

import sys
import time
import glob
import argparse
import subprocess
import boto3
import logging
import _thread
import threading
import math
import time
from botocore.exceptions import ClientError

logger = logging.getLogger('make_histograms')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(fmt='%(asctime)s %(levelname)s %(message)s'))
logger.addHandler(handler)

def parse_prefix(prefix):
  # input prefix: epochsecond_epochsecond/tile_level/tile_index
  # dest key: year/month/day/hour/tile_level/tile_index
  parts = prefix.split('/')
  epoch = int(parts[0].split('_')[0])
  to_time = time.gmtime(epoch)
  dest = str(to_time[0]) + '/' + str(to_time[1]) + '/' + str(to_time[2]) + '/' + str(to_time[3]) + '/' + parts[1] + '/' + parts[2] + '.fb'
  #time_bucket, tile_id, destination key
  return int(epoch / 3600), (int(parts[2]) << 3) | int(parts[1]), dest

def convert(time_bucket, tile_id, dest_key):
  logger.info('running conversion process')
  fb_out_file = dest_key.split('/')[-1]
  
  # TODO: no idea if the exception handling works
  try:
    output = subprocess.check_output(['datastore-histogram-tile-writer', '-b', str(time_bucket), '-t', str(tile_id), '-f', fb_out_file] + glob.glob('*'), timeout=180, universal_newlines=True, stderr=subprocess.STDOUT)
    for line in output.splitlines():
      logger.info(line)
  except subprocess.CalledProcessError as tilewriter:
    logger.error('Failed running datastore-histogram-tile-writer: ' + str(tilewriter.returncode) + ' ' + str(tilewriter.output))
    sys.exit(tilewriter.returncode)
  logger.info('Finished running conversion')

def upload(dest_key, s3_datastore_bucket):
  s3_client = boto3.client('s3')
  with open(dest_key.split('/')[-1], 'rb') as data:
    
    logger.info('Uploading to ' + s3_datastore_bucket + ' ' + dest_key)
    s3_client.put_object(Bucket=s3_datastore_bucket, ContentType='binary/octet-stream', Body=data, Key=dest_key)

def delete_keys(keys, s3_reporter_bucket):
  s3_client = boto3.client('s3')
  for key in keys:
    secs = 1
    while True:
      try:
        s3_client.delete_object(Bucket=s3_reporter_bucket, Key=key)
        s3_client.head_object(Bucket=s3_reporter_bucket, Key=key)
      except Exception as e:
        if e.response['ResponseMetadata']['HTTPStatusCode'] == 404:
          break
        logger.error('Failed to delete ' + key + ': ' + str(d))
        logger.info('Sleeping %d seconds until next try' % secs)
        time.sleep(secs)
        secs *= 2
    logger.info('Deleted from ' + s3_reporter_bucket + ' ' + key)

def delete(keys, s3_reporter_bucket):
  # delete the files
  chunks = split(keys, 10)
  threads = []
  for chunk in chunks:
    threads.append(threading.Thread(target=delete_keys, args=(chunk, s3_reporter_bucket)))
    threads[-1].start()
  for t in threads:
    t.join()

def get_files(keys, s3_reporter_bucket, s3_datastore_bucket):
  session = boto3.session.Session()
  s3_resource = session.resource('s3')
  retries = 10
  for key in keys:
    object_id = key.rsplit('/', 1)[-1]
    secs = 1
    while True:
      try:
        if key.endswith('.fb'):
          s3_resource.Object(s3_datastore_bucket, key).download_file(object_id)
          logger.info('downloaded ' + key + ' as ' + object_id + ' from s3 bucket: ' + s3_datastore_bucket)
        else:
          s3_resource.Object(s3_reporter_bucket, key).download_file(object_id)
          logger.info('downloaded ' + key + ' as ' + object_id + ' from s3 bucket: ' + s3_reporter_bucket)
        break
      except Exception as e:
        logger.error('Failed to download: %s' % e)
        if key.endswith('.fb'):
          break
        logger.warn('%d retries remaining for %s' % (retries, object_id))
        retries -= 1
        if retries == 0:
          logger.error('Reached maximum retries downloading: %s' % object_id)
          _thread.interrupt_main()
        time.sleep(secs)
        secs *= 2

def split(l, n):
  size = int(math.ceil(len(l)/float(n)))
  cutoff = len(l) % n
  result = []
  pos = 0
  for i in range(0, n):
    end = pos + size if cutoff == 0 or i < cutoff else pos + size - 1
    result.append(l[pos:end])
    pos = end
  return result

def get_prefixes_keys(client, bucket, prefixes):
  keys = []
  pres = []
  for prefix in prefixes:
    token = None
    first = True
    while first or token:
      if token:
        objects = client.list_objects_v2(Bucket=bucket, Delimiter='/', Prefix=prefix, ContinuationToken=token)
      else:
        objects = client.list_objects_v2(Bucket=bucket, Delimiter='/', Prefix=prefix)
      if 'Contents' in objects:
        keys.extend([ o['Key'] for o in objects['Contents'] ])
      if 'CommonPrefixes' in objects:
        pres.extend([ o['Prefix'] for o in objects['CommonPrefixes'] ])
      token = objects.get('NextContinuationToken')
      first = False
  return pres, keys

def download_data(prefix, s3_reporter_bucket, s3_datastore_bucket, dest_key):
  client = boto3.client('s3')

  # get the keys for the files in this tile
  _, keys = get_prefixes_keys(client, s3_reporter_bucket, [prefix])
  if not keys:
    return []
 
  # add the key for the existing file
  keys.append(dest_key)

  # download the files
  chunks = split(keys, 10)
  threads = []
  for chunk in chunks:
    threads.append(threading.Thread(target=get_files, args=(chunk, s3_reporter_bucket, s3_datastore_bucket)))
    threads[-1].start()
  for t in threads:
    t.join()

  return keys[:-1]

if __name__ == "__main__":
  # build args
  parser = argparse.ArgumentParser()
  parser.add_argument('--s3-reporter-bucket', type=str, help='Bucket (e.g. reporter-work-prod) in which the data we wish to process is located')
  parser.add_argument('--s3-datastore-bucket', type=str, help='Bucket (e.g. datastore-output-prod) into which we will place transformed data')
  parser.add_argument('--s3-reporter-prefix', type=str, help='S3 prefix under which tiles will be found, should look like epochsecond_epochsecond/level/tile_index')
  args = parser.parse_args()

  logger.info('reporter input bucket: ' + args.s3_reporter_bucket)
  logger.info('datastore output bucket: ' + args.s3_datastore_bucket)
  logger.info('reporter prefix: ' + str(args.s3_reporter_prefix))

  #where will this thing end up
  time_bucket, tile_id, dest_key = parse_prefix(args.s3_reporter_prefix)

  #go download all the tiles
  keys = download_data(args.s3_reporter_prefix, args.s3_reporter_bucket, args.s3_datastore_bucket, dest_key)

  if not keys:
    logger.error('Prefix was empty!')
    sys.exit(1)

  #turn the downloaded files into
  convert(time_bucket, tile_id, dest_key)

  #upload the finished product
  upload(dest_key, args.s3_datastore_bucket)

  #delete the input data
  delete(keys, args.s3_reporter_bucket)

  logger.info('run complete')
