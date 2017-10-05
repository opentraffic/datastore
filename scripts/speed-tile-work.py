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
import multiprocessing
import Queue
import random
import functools

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

def interrupt_wrapper(func):
  try:
    func()
  except (KeyboardInterrupt, SystemExit):
    logger.error('Interrupted or killed')

def upload(speed_bucket, level, index, week, speed_tiles):
  logger.info('Uploading data to bucket: ' + speed_bucket)
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
      Bucket=speed_bucket,
      ContentType='application/octet-stream',
      ContentEncoding='gzip',
      Body=zipped.getvalue(),
      Key=key)

def load(histograms, sub_segments, info, lengths):
  segments = {}
  while True:
    try:
      file_name = histograms.get(block=False)
      make_speeds.addSegments(file_name, info, lengths, segments)
    except Queue.Empty:
      break
    except (KeyboardInterrupt, SystemExit) as e:
      raise e
    except Exception as e:
      logger.error('Failed to load segments from %s' % file_name)

  #send back the info
  sub_segments.put(segments)

def convert(level, index, week, histograms, concurrency):
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
  #we share the segments dict in a process safe way
  sub_segments = multiprocessing.Queue()
  processes = []
  #start them up and wait for them to get all the results back
  for i in xrange(concurrency):
    bound = functools.partial(load, histograms, sub_segments, info, lengths)
    processes.append(multiprocessing.Process(target=interrupt_wrapper, args=(bound,)))
    processes[-1].start()

  #then harvest all the sub_segments
  segments = {}
  for i in xrange(concurrency):
    #get out the segments and merge them all together
    sub = sub_segments.get()
    while len(sub):
      seg_id = sub.iterkeys().next()
      hours = sub.pop(seg_id)
      #since each histogram is a specific hour no two dicts will have the same hour
      segments.setdefault(seg_id, {}).update(hours)
    logger.info('Merged result segments from sub process %d' % i)

  #then make some tiles
  logger.info('Creating speed tiles')
  prefix = url_suffix(int(level), int(index)).split('/')[-1]
  return make_speeds.createSpeedTiles(lengths, prefix + '.spd', 10000, prefix + '.nex', True, segments, info), osmlr

def fetch(histogram_bucket, keys, results):
  session = boto3.session.Session()
  resource = session.resource('s3')
  for key in keys:
    try:
      dest = key.replace('/', '_')
      resource.Object(histogram_bucket, key).download_file(dest)
      if os.path.isfile(dest):
        logger.info('Downloaded s3://' + histogram_bucket + '/' + key)
        results.put(dest)
    except (KeyboardInterrupt, SystemExit) as e:
      raise e
    except Exception as e:
      pass

def download(histogram_bucket, tile_level, tile_index, week, concurrency):
  #we need to get all the histograms for all the hours of this week
  date = datetime.datetime.strptime(week + '/1','%Y/%W/%w')
  logger.info('Downloading files for week ' + '{d.year}/{d.month}/{d.day} tile {l}/{t}.fb'.format(d=date, l=tile_level, t=tile_index))
  keys = []
  for hour in xrange(0, 24 * 7):
    key = '{d.year}/{d.month}/{d.day}/{d.hour}/{l}/{t}.fb'.format(d=(date + datetime.timedelta(hours=hour)), l=tile_level, t=tile_index)
    keys.append(key)
  random.shuffle(keys)
  keys = split(keys, concurrency)

  processes = []
  downloaded = multiprocessing.Queue()
  for i in xrange(concurrency):
    bound = functools.partial(fetch, histogram_bucket, keys[i], downloaded)
    processes.append(multiprocessing.Process(target=interrupt_wrapper, args=(bound,)))
    processes[-1].start()
  for p in processes:
    if p.is_alive():
      p.join()
  return downloaded  

if __name__ == '__main__':
  #build args
  parser = argparse.ArgumentParser()
  parser.add_argument('--environment', type=str, help='The environment prod or dev to use when computing bucket names and batch job queues and definitions', required=True)
  parser.add_argument('--tile-level', type=int, help='The tile level used to get the input data from the histogram bucket', required=True)
  parser.add_argument('--tile-index', type=int, help='The tile index used to get the input data from the histogram bucket', required=True)
  parser.add_argument('--week', type=str, help='The week used to get the input data from the histogram bucket', required=True)
  parser.add_argument('--concurrency', type=int, help='The week used to get the input data from the histogram bucket', default=1)
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

  #need this to submit jobs
  batch_client = boto3.client('batch')
  histogram_bucket = 'datastore-output-' + args.environment
  speed_bucket = 'speedtiles-' + args.environment
  job_queue = 'referencetiles-' + args.environment
  job_def = 'referencetiles-' + args.environment

  try:
    #for each tile
    for tile in tiles:
      logger.info('Histogram input bucket: ' + histogram_bucket)
      logger.info('Speedtile output bucket: ' + speed_bucket)
      logger.info('Tile level: ' + str(tile[0]))
      logger.info('Tile index: ' + str(tile[1]))
      logger.info('Week: ' + args.week)  
      #go get the histogram data
      histograms = download(histogram_bucket, tile[0], tile[1], args.week, args.concurrency)
      if histograms:
        #make the speed tile
        speed_tiles, osmlr = convert(tile[0], tile[1], args.week, histograms, args.concurrency)
        #move the speed tile to its destination
        upload(speed_bucket, tile[0], tile[1], args.week, speed_tiles)
        # create the corresponding referencetile jobs
        job_name = '_'.join([args.week.replace('/', '-'), str(tile[0]), str(tile[1])])
        job = {'environment': args.environment, 'tile_level': str(tile[0]), 'tile_index': str(tile[1]), 'week': args.week}
        logger.info('Submitting reference tile job ' + job_name)
        logger.info('Job parameters ' + str(job))
        submitted = batch_client.submit_job(
          jobName = job_name,
          jobQueue = job_queue,
          jobDefinition = job_def,
          parameters = job,
          containerOverrides={
            'memory': 8192,
            'vcpus': 2,
            'command': ['/scripts/ref-tile-work.py', '--environment', 'Ref::environment', '--end-week', 'Ref::week', '--weeks', '52', '--tile-level', 'Ref::tile_level', '--tile-index', 'Ref::tile_index']
          }
        )
        logger.info('Job %s was submitted and got id %s' % (job_name, submitted['jobId']))
        #clean up the files
        for f in histograms + speed_tiles + [osmlr]:
          os.remove(f)
      else:
        logger.info('No histogram data')

    logger.info('Run complete')
  except (KeyboardInterrupt, SystemExit):
    logger.error('Interrupted or killed')
