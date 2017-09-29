#!/usr/bin/env python2

import argparse
import boto3
import datetime
import urllib
import math
import os
import logging
import gzip
import datetime
import calendar
import logging
import StringIO
from botocore.exceptions import ClientError

logger = logging.getLogger('make_speeds')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(fmt='%(asctime)s %(levelname)s %(message)s'))
logger.addHandler(handler)

# import make_speeds after logger has been setup
import make_speeds

valhalla_tiles = [{'level': 2, 'size': 0.25}, {'level': 1, 'size': 1.0}, {'level': 0, 'size': 4.0}]

def url_suffix(tile_level, tile_index):
  tile_set = filter(lambda x: x['level'] == tile_level, valhalla_tiles)[0]
  max_index = int(360.0/tile_set['size'] * 180.0/tile_set['size']) - 1
  num_dirs = int(math.ceil(len(str(max_index)) / 3.0))
  suffix = list(str(tile_index).zfill(3 * num_dirs))
  for i in range(0, num_dirs):
    suffix.insert(i * 3 + i, '/')
  suffix = '/' + str(tile_level) + ''.join(suffix) 
  return suffix

def upload(dest_bucket, level, index, week, speed_tiles):
  logger.info('Uploading data to bucket: ' + dest_bucket)
  client = boto3.client('s3')
  prefix = week + '/'.join(url_suffix(int(level), int(index)).split('/')[:-1])
  for tile in speed_tiles:
    key = prefix + '/' + tile.split('/')[-1] + '.gz'
    logger.info('Uploading ' + tile + ' as ' + key)
    zipped = StringIO.StringIO()
    with open(tile, 'rb') as f_in, gzip.GzipFile(mode='wb',fileobj=zipped) as f_out:
      f_out.write(f_in.read())
    client.put_object(
      ACL='public-read',
      Bucket=dest_bucket,
      ContentType='application/octet-stream',
      ContentEncoding='gzip',
      Body=zipped.getvalue(),
      Key=key)

def convert(level, index, week, histograms):
  url = 'http://s3.amazonaws.com/osmlr-tiles/v1.0/pbf' + url_suffix(level, index) + '.osmlr'
  logger.info('Fetching osmlr tile: ' + url)
  osmlr = str(level) + '_' + str(index) + '.osmlr'
  urllib.URLopener().retrieve(url, osmlr)

  logger.info('Getting segment lengths from osmlr')
  lengths = make_speeds.getLengths(osmlr)

  date = datetime.datetime.strptime(week + '/1','%Y/%W/%w')
  start = calendar.timegm(date.timetuple())
  info = {'rangeStart': start, 'rangeEnd': start + 604800, 'unitSize': 604800, 'entrySize': 3600,
    'description': 'Hourly speeds for the week starting on ' + str(date), 'level': level, 'index': index}

  logger.info('Accumulating segment speeds from histograms')
  segments = make_speeds.getSegments('.', info, lengths)

  logger.info('Creating speed tiles')
  prefix = url_suffix(int(level), int(index)).split('/')[-1]
  return make_speeds.createSpeedTiles(lengths, prefix + '.spd', 10000, prefix + '.nex', True, segments, info), osmlr

def download(src_bucket, tile_level, tile_index, week):
  #we need to get all the histograms for all the hours of this week
  resource = boto3.resource('s3')
  date = datetime.datetime.strptime(week + '/1','%Y/%W/%w')
  downloaded = []
  logger.info('Downloading files for week ' + '{d.year}/{d.month}/{d.day} tile {l}/{t}.fb'.format(d=date, l=tile_level, t=tile_index))
  for hour in range(0, 24 * 7):
    key = '{d.year}/{d.month}/{d.day}/{d.hour}/{l}/{t}.fb'.format(d=(date + datetime.timedelta(hours=hour)), l=tile_level, t=tile_index)
    try:
      resource.Object(src_bucket, key).download_file(key.replace('/', '_'))
      downloaded.append(key.replace('/', '_'))
      logger.info('Downloaded s3://' + src_bucket + '/' + key)
    except Exception as e:
      pass
  return downloaded

if __name__ == '__main__':
  #build args
  parser = argparse.ArgumentParser()
  parser.add_argument('--src-bucket', type=str, help='Bucket (e.g. datastore-output-prod) in which the data we wish to process is located')
  parser.add_argument('--dest-bucket', type=str, help='Bucket (e.g. speedtiles-prod) into which we will place transformed data')
  parser.add_argument('--reference-dest-bucket', type=str, help='Bucket (e.g. referencetiles-prod) into which we will place reference tile data')
  parser.add_argument('--tile-level', type=int, help='The tile level used to get the input data from the src_bucket')
  parser.add_argument('--tile-index', type=int, help='The tile index used to get the input data from the src_bucket')
  parser.add_argument('--week', type=str, help='The week used to get the input data from the src_bucket')
  args = parser.parse_args()

  #generate the list of the parent tile and all its subtiles, level 0 and 1 only right now
  tiles = [(args.tile_level, args.tile_index)]
  level = filter(lambda x: x['level'] == args.tile_level, valhalla_tiles)[0]
  next_level = filter(lambda x: x['level'] == args.tile_level + 1, valhalla_tiles)[0]
  per_row = int(360 / level['size'])
  row = args.tile_index / per_row
  col = args.tile_index - row * per_row
  scale = int(level['size'] / next_level['size'])
  row *= scale
  col *= scale
  per_row = int(360 / next_level['size'])
  for y in range(row, row + scale):
    for x in range(col, col + scale):
      tiles.append((next_level['level'],  y*per_row + x))

  #for each tile
  for tile in tiles:
    logger.info('Histogram input bucket: ' + args.src_bucket)
    logger.info('Speedtile output bucket: ' + args.dest_bucket)
    logger.info('Referencetiles output bucket: ' + args.reference_dest_bucket)
    logger.info('Tile level: ' + str(tile[0]))
    logger.info('Tile index: ' + str(tile[1]))
    logger.info('Week: ' + args.week)  
    #go get the histogram data
    histograms = download(args.src_bucket, tile[0], tile[1], args.week)
    if histograms:
      #make the speed tile
      speed_tiles, osmlr = convert(tile[0], tile[1], args.week, histograms)
      #move the speed tile to its destination
      upload(args.dest_bucket, tile[0], tile[1], args.week, speed_tiles)
      # create the corresponding referencetile jobs
      job_name = '_'.join([week.replace('/', '-'), str(t[0]), str(t[1])])
      job = {'speed_bucket': args.src_bucket, 'ref_speed_bucket': args.reference_dest_bucket, 'tile_level': str(tile[0]), 'tile_index': str(tile[1]), 'week': week}
      logger.info('Submitting reference tile job ' + job_name)
      logger.info('Job parameters ' + str(job))
      submitted = batch_client.submit_job(
        jobName = job_name,
        jobQueue = ref_job_queue,
        jobDefinition = ref_job_def,
        parameters = job,
        containerOverrides={
          'memory': 8192,
          'vcpus': 2,
          'command': ['/scripts/ref-tile-work.py', '--speed-bucket', 'Ref::speed_bucket', '--ref-speed-bucket', 'Ref::ref_speed_bucket', '--end-week', 'Ref::week', '--weeks', '52', '--tile-level', 'Ref::tile_level', '--tile-index', 'Ref::tile_index']
        }
      )
      logger.info('Job %s was submitted and got id %s' % (job_name, submitted['jobId']))
      #clean up the files
      for f in histograms + speed_tiles + [osmlr]:
        os.remove(f)
    else:
      logger.info('No histogram data');

  logger.info('Run complete')
