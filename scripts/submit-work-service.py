#!/usr/bin/env python
""" push units of work to AWS Batch """

import os
import sys
import time
import boto3
import logging
import threading
import math
from botocore.exceptions import ClientError

logger = logging.getLogger('make_histograms')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(fmt='%(asctime)s %(levelname)s %(message)s'))
logger.addHandler(handler)

def batch_check_queue(batch_client, job_queue):
  """ check that the job queue is empty before submitting any new work """

  # set default queue status
  queue_status = 'idle'

  logger.info('Checking for existing jobs in the queue.')
  statuses = ['RUNNING', 'RUNNABLE', 'SUBMITTED', 'PENDING', 'STARTING']

  for status in statuses:
    response = batch_client.list_jobs(
      jobQueue=job_queue,
      jobStatus=status,
      maxResults=10
      )

    if len(response['jobSummaryList']) > 0:
      logger.info('Found active jobs in state: ' + status)
      queue_status = 'processing'

  return queue_status

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

def get_time_tiles(client, bucket):
  """ check S3 for new data """

  logger.info('Getting contents of bucket: ' + bucket)
  hours, _ = get_prefixes_keys(client, bucket, [''])
  logger.info('Got %d different hours' % len(hours))
  levels, _ = get_prefixes_keys(client, bucket, hours)
  logger.info('Got %d different levels of hours' % len(levels))
  tiles, _ = get_prefixes_keys(client, bucket, levels)
  logger.info('Got %d different tiles of levels of hours' % len(tiles))
  return tiles

def submit_jobs(tiles, batch_client, job_queue, job_def, reporter_bucket, datastore_bucket):
  """ loop over the tiles and create jobs """

  for tile in tiles:
    # NOTE on resources: these will generally run successfully with
    #   only 128mb specified, but they will fail miserably with only 64mb.
    #   Currently set to 1024mb for safety. Note that the settings here will
    #   override whatever the current setting is in the job definition.
    logger.info('Submitting a new job: ' + tile)
    batch_client.submit_job(
      jobName=tile.replace('/','-'),
      jobQueue=job_queue,
      jobDefinition=job_def,
      parameters={
        's3_reporter_bucket': reporter_bucket,
        's3_datastore_bucket': datastore_bucket,
        's3_reporter_prefix': tile,
      },
      containerOverrides={
        'memory': 4096,
        'vcpus': 1,
        'command': [
          '/scripts/work.py',
          '--s3-reporter-bucket',
          'Ref::s3_reporter_bucket',
          '--s3-datastore-bucket',
          'Ref::s3_datastore_bucket',
          '--s3-reporter-prefix',
          'Ref::s3_reporter_prefix',
        ]
      }
    )

env = os.getenv('DATASTORE_ENV', 'BOGUS') # required, 'prod' or 'dev'
sleep_between_runs = os.getenv('SLEEP_BETWEEN_RUNS', 120) # optional

if env == 'BOGUS':
  logger.error('DATASTORE_ENV environment variable not set! Exiting.')
  sys.exit(1)
else:
  sleep_between_runs = int(sleep_between_runs)
  reporter_bucket = 'reporter-drop-' + env
  datastore_bucket = 'datastore-output-' + env
  job_queue = 'datastore-' + env
  job_def = 'datastore-' + env

s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')
batch_client = boto3.client('batch')

#check if we are still working on stuff
batch_queue_status = batch_check_queue(batch_client, job_queue)
if batch_queue_status == 'processing':
  logger.info('Run complete!')
  logger.info('Sleeping before next run...')
  time.sleep(sleep_between_runs)
else:
  #check if there is new tile data to work on
  logger.info('No jobs in the queue.')
  tiles = get_time_tiles(s3_client, reporter_bucket)
  if not tiles:
    logger.info('Found no tiles! Passing on this run.')
  else:
    #make new jobs
    submit_jobs(tiles, batch_client, job_queue, job_def, reporter_bucket, datastore_bucket)

logger.info('Run complete!')
logger.info('Sleeping before next run...')
time.sleep(sleep_between_runs)
