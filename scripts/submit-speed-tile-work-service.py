#!/usr/bin/env python
""" push units of work to AWS Batch """

import os
import sys
import time
import boto3
import re
import datetime

def flush(string):
  """ flush output """

  sys.stdout.write(string + os.linesep)
  sys.stdout.flush()

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

def get_week(client, src_bucket, dest_bucket):
  #what source data do we have
  flush('[INFO] Getting time range for source ' + src_bucket)
  years = natural_sorted(get_prefixes_keys(client, src_bucket, [''])[0])
  min_month = natural_sorted(get_prefixes_keys(client, src_bucket, years[:1])[0])[0]
  max_month = natural_sorted(get_prefixes_keys(client, src_bucket, years[-1:])[0])[-1]
  min_day = natural_sorted(get_prefixes_keys(client, src_bucket, [min_month])[0])[0]
  min_date = datetime.datetime.strptime(min_day, '%Y/%m/%d/').date()
  max_day = natural_sorted(get_prefixes_keys(client, src_bucket, [max_month])[0])[-1]
  max_date = min_date = datetime.datetime.strptime(max_day, '%Y/%m/%d/').date()
  flush('[INFO] Source data ranges from ' + min_day + ' to ' + max_day)

  #what dest data did we already create
  flush('[INFO] Getting week for destination ' + dest_bucket)
  max_year = natural_sorted(get_prefixes_keys(client, dest_bucket, [''])[0])[-1]
  if len(max_year):
    week = natural_sorted(get_prefixes_keys(client, dest_bucket, [max_year])[0])[-1]
    parts = [ p for p in week.split('/') if p ]
    week = int(parts[-1]) + 1
    parts[-1] = str(week)
    #this week falls too far in the future
    if week < int(min_date.strftime("%W")) and week > int(max_date.strftime("%W")):
      flush('[INFO] No coverage for the week of ' + '/'.join(parts) + '/')
      return None
    week = '/'.join(parts) + '/'
  else:
    week = int(min_date.strftime("%W"))
    week = years[0] + str(week) + '/'

  #do we have source data available for this target week
  return week

def submit_jobs(batch_client, job_queue, job_def, src_bucket, dest_bucket, week):
  flush('[INFO] Creating speed tiles for the week of ' + week)

  #loop over all tiles, level 0 are 4 degrees
  tile_level = 0
  for tile_id in range(0, (360 * 180) / 4):
    job_name = week + '_' + str(tile_level) + '_' + str(tile_id)
    flush('[INFO] Submitting speed tile job ' + job_name)
    batch_client.submit_job(
      jobName = job_name,
      jobQueue = job_queue,
      jobDefinition = job_def,
      parameters = {
        'src_bucket': src_bucket,
        'dest_bucket': dest_bucket,
        'tile_level': tile_level,
        'tile_index': tile_id,
        'week': week
      },
      containerOverrides={
        'memory': 8192,
        'vcpus': 2,
        'command': [
          '/scripts/speed-tile-work.py',
          '--src-bucket',
          'Ref::src_bucket',
          '--dest-bucket',
          'Ref::dest_bucket',
          '--tile-level',
          'Ref::tile_level',
          '--tile-index',
          'Ref::tile_index',
          '--week',
          'Ref::week',
        ]
      }
    )


""" the aws lambda entry point """

env = os.getenv('DATASTORE_ENV', 'BOGUS') # required, 'prod' or 'dev'

if env == 'BOGUS':
  flush('[ERROR] DATASTORE_ENV environment variable not set! Exiting.')
  sys.exit(1)
else:
  src_bucket = 'datastore-output-' + env
  dest_bucket = 'speed-extracts'# + env
  job_queue = 'speedtiles-' + env
  job_def = 'speedtiles-' + env

client = boto3.client('s3')
batch_client = boto3.client('batch')

#figure out what time ranges are available
week = get_week(client, src_bucket, dest_bucket)

#send the jobs to batch service
submit_jobs(batch_client, job_queue, job_def, week)

flush('[INFO] Run complete!')
