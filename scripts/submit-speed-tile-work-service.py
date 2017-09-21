#!/usr/bin/env python
""" push units of work to AWS Batch """

import os
import sys
import time
import boto3
import re
import datetime
import logging
import math

logger = logging.getLogger('make_speeds')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(fmt='%(asctime)s %(levelname)s %(message)s'))
logger.addHandler(handler)

valhalla_tiles = [{'level': 2, 'size': 0.25}, {'level': 1, 'size': 1.0}, {'level': 0, 'size': 4.0}]

def natural_sorted(l):
  exp = re.compile('([0-9]+)')
  return sorted(l, key=lambda s:[ int(c) if c.isdigit() else c for c in re.split(exp, s) ])

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

def get_week(client, env):
  src_bucket = 'datastore-output-' + env
  dest_bucket = 'speedtiles-' + env

  #what source data do we have
  logger.info('Getting time range for source ' + src_bucket)
  years = natural_sorted(get_prefixes_keys(client, src_bucket, [''])[0])
  min_month = natural_sorted(get_prefixes_keys(client, src_bucket, years[:1])[0])[0]
  max_month = natural_sorted(get_prefixes_keys(client, src_bucket, years[-1:])[0])[-1]

  min_day = natural_sorted(get_prefixes_keys(client, src_bucket, [min_month])[0])[0]
  min_date = datetime.datetime.strptime(min_day, '%Y/%m/%d/').date()
  min_date = min_date - datetime.timedelta(days=min_date.weekday())

  max_day = natural_sorted(get_prefixes_keys(client, src_bucket, [max_month])[0])[-1]
  max_date = datetime.datetime.strptime(max_day, '%Y/%m/%d/').date()
  max_date = max_date + datetime.timedelta(days=(6 - max_date.weekday()))
  logger.info('Source data ranges from ' + min_day + ' to ' + max_day)
  logger.info('From the week starting on ' + str(min_date) + ' through the week ending on ' + str(max_date))

  #what dest data did we already create
  logger.info('Getting week for destination ' + dest_bucket)
  
  years = natural_sorted(get_prefixes_keys(client, dest_bucket, [''])[0])
  if len(years):
    week = natural_sorted(get_prefixes_keys(client, dest_bucket, years[-1:])[0])[-1]
    date = datetime.datetime.strptime(week + '1','%Y/%W/%w').date()
    date = date + datetime.timedelta(days=7)
    #this week falls too far in the future
    if date < min_date or date > max_date:
      logger.info('No coverage for the week of ' + str(date))
      return None
    return date.strftime("%Y/%W")
  else:
    return min_date.strftime("%Y/%W")

def get_tiles(tile_level, tile_index):
  tiles = [(tile_level, tile_index)]
  level = filter(lambda x: x['level'] == tile_level, valhalla_tiles)[0]
  next_level = filter(lambda x: x['level'] == tile_level + 1, valhalla_tiles)[0]
  per_row = int(360 / level['size'])
  row = tile_index / per_row
  col = tile_index - row * per_row
  scale = int(level['size'] / next_level['size'])
  row *= scale
  col *= scale
  per_row = int(360 / next_level['size'])
  for y in range(row, row + scale):
    for x in range(col, col + scale):
      tiles.append((next_level['level'],  y*per_row + x))
  return tiles

def submit_jobs(batch_client, env, week, bbox):
  src_bucket = 'datastore-output-' + env
  dest_bucket = 'speedtiles-' + env
  ref_bucket = 'referencetile-' + env
  job_queue = 'speedtiles-' + env
  job_def = 'speedtiles-' + env
  ref_job_queue = 'referencetiles-' + env
  ref_job_def = 'referencetiles-' + env



  logger.info('Creating speed tiles for the week of ' + week)
  if bbox is None:
    bbox=[0, 0, 360, 180]
  else:
    bbox = [ float(v) for v in bbox.split(',') ]
    bbox[0] = int(math.floor((bbox[0] + 90)/4))
    bbox[1] = int(math.floor((bbox[1] + 180)/4))
    bbox[2] = int(math.ceil((bbox[2] + 90)/4))
    bbox[3] = int(math.ceil((bbox[3] + 180)/4))

  #loop over all tiles, level 0 are 4 degrees
  tile_level = 0
  for y in range(bbox[0], bbox[2] + 1):
    for x in range(bbox[1], bbox[3] + 1):
      tile_index = y * 90 + x
      if tile_index > 90 * 45 - 1:
        continue

      #submit the job to make the speed tiles for level 0 and level 1
      job_name = '_'.join([week.replace('/', '-'), str(tile_level), str(tile_index)])
      job = {'src_bucket': src_bucket, 'dest_bucket': dest_bucket, 'tile_level': str(tile_level), 'tile_index': str(tile_index), 'week': week}
      logger.info('Submitting speed tile job ' + job_name)
      logger.info('Job parameters ' + str(job))
      submitted = batch_client.submit_job(
        jobName = job_name,
        jobQueue = job_queue,
        jobDefinition = job_def,
        parameters = job,
        containerOverrides={
          'memory': 8192,
          'vcpus': 2,
          'command': ['/scripts/speed-tile-work.py', '--src-bucket', 'Ref::src_bucket', '--dest-bucket', 'Ref::dest_bucket', '--tile-level', 'Ref::tile_level', '--tile-index', 'Ref::tile_index', '--week', 'Ref::week']
        }
      )

      #submit the corresponding jobs to make the reference tiles using the above speed tiles
      tiles = get_tiles(tile_level, tile_index)
      for t in tiles:
        job_name = '_'.join([week.replace('/', '-'), str(t[0]), str(t[1])])
        job = {'speed_bucket': dest_bucket, 'ref_speed_bucket': ref_bucket, 'tile_level': str(t[0]), 'tile_index': str(t[1]), 'week': week}
        logger.info('Submitting reference tile job ' + job_name)
        logger.info('Job parameters ' + str(job))
        batch_client.submit_job(
          dependsOn = [ submitted['jobId'] ],
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


""" the aws lambda entry point """
env = os.getenv('DATASTORE_ENV', None) # required, 'prod' or 'dev'
week = os.getenv('TARGET_WEEK', None) #optional should be iso8601, ordinal_year/ordinal_week
bbox = os.getenv('TARGET_BBOX', None) #optional should be minx,miny,maxx,maxy

if env != 'prod' and env != 'dev':
  logger.error('DATASTORE_ENV environment variable not set! Exiting.')
  sys.exit(1)

client = boto3.client('s3')
batch_client = boto3.client('batch')

#figure out what time ranges are available
if week is None:
  week = get_week(client, env)

#send the jobs to batch service
if week is not None:
  submit_jobs(batch_client, env, week, bbox)

logger.info('Run complete!')
