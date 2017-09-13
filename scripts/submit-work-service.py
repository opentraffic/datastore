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

def s3_clean_work_bucket(s3_resource, work_bucket):
    """ cull any old work data """

    logger.info('Emptying work bucket: ' + work_bucket + '.')

    try:
        s3_resource.Bucket(work_bucket).objects.delete()
    except ClientError as e:
        logger.error('Failed to empty work bucket: ' + work_bucket + '. Aborting!')
        time.sleep(300)
        sys.exit([1])

def s3_get_data(s3_client, reporter_bucket, max_keys):
    """ check S3 for new data """

    logger.info('Getting contents of bucket: ' \
        + reporter_bucket \
        + ', max keys: ' \
        + str(max_keys) \
        + '.')

    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(
        Bucket=reporter_bucket,
        PaginationConfig={'MaxItems': max_keys}
        )

    keys_array = []
    for page in page_iterator:
        if 'Contents' in page:
            for key in page['Contents']:
                keys_array.append(key['Key'])

    keys_array.sort()
    return keys_array

def s3_move_data(keys, work_bucket, reporter_bucket):
    session = boto3.session.Session()
    s3_client = session.client('s3')
    """ move data to working bucket """
    for key in keys:
        logger.info('Moving key: ' + key + ' from ' + reporter_bucket + ' to ' + work_bucket + '.')
        try:
            s3_client.copy_object(
                Bucket=work_bucket,
                Key=key,
                CopySource=reporter_bucket + '/' + key
                )
        except ClientError as e:
            logger.error('Failed to copy key ' + key + ': %s' % e + '.')

        logger.info('Deleting key: ' + key + ' from ' + reporter_bucket + '.')
        try:
            s3_client.delete_object(
                Bucket=reporter_bucket,
                Key=key
                )
        except ClientError as e:
            logger.error('Failed to delete key: ' + key + ': %s' % e + '.')

def build_dictionary(keys_array, bucket_interval):
    """ create a dictionary with a key of type tuple of (time bucket,
    tile_level, tile_index), with the value as a list of
    files: dictionary = {(23, 32, 43): 'tuple as key'}
    dictionary[time_bucket, tile_level, tile_index] = ['/path/1', '/path/2'] """

    dictionary = {}
    for path in keys_array:
        # parse epoch
        epoch = path.split('/')[0]
        epoch_start = int(epoch.split('_')[0])
        epoch_end = int(epoch.split('_')[1])

        # calculate the tile id: shift the tile
        # index 3 bits to the left then add the
        # tile level.
        tile_level = int(path.split('/')[1])
        tile_index = int(path.split('/')[2])

        # create the dictionary, sorting on time_bucket, tile_level, tile_index
        for time_bucket in range(epoch_start/bucket_interval, epoch_end/bucket_interval + 1):
            if (time_bucket, tile_level, tile_index) in dictionary:
                dictionary[(time_bucket, tile_level, tile_index)].append(path)
            else:
                dictionary[(time_bucket, tile_level, tile_index)] = [path]

    return dictionary

def build_jobs(dictionary, batch_client, job_queue, job_def, work_bucket, datastore_bucket):
    """ loop over the dictionary and create jobs """

    logger.info('Building jobs.')

    for key, val in dictionary.items():
        time_bucket = str(key[0])
        tile_level = str(key[1])
        tile_index = str(key[2])
        tile_id = str(((key[2]) << 3) | key[1])
        job_name = str(key[0]) + '_' + str(tile_id)
        # just use the shared prefixes of the files up to the last directory
        files = set([ f[:f.rfind('/') + 1] for f in val ])
        files = ','.join(files)

        # set memory/vcpus for the job based
        #   on how many files we need to process
        if len(val) < 30:
            memory = 512
            vcpus = 1
        elif 30 <= len(val) < 100:
            memory = 1024
            vcpus = 1
        elif 100 <= len(val) < 300:
            memory = 2048
            vcpus = 1
        elif 300 <= len(val) < 500:
            memory = 3072
            vcpus = 2
        else:
            memory = 4096
            vcpus = 2
        
        # create our batch job
        logger.info('Submitting a new job: ' \
            + job_name \
            + ', containing ' \
            + str(len(val)) \
            + ' file(s). Memory: ' \
            + str(memory) \
            + ', vcpus: ' \
            + str(vcpus) \
            + '.')

        # NOTE on resources: these will generally run successfully with
        #   only 128mb specified, but they will fail miserably with only 64mb.
        #   Currently set to 1024mb for safety. Note that the settings here will
        #   override whatever the current setting is in the job definition.
        batch_client.submit_job(
            jobName=job_name,
            jobQueue=job_queue,
            jobDefinition=job_def,
            parameters={
                's3_reporter_bucket': work_bucket,
                's3_datastore_bucket': datastore_bucket,
                's3_reporter_keys': files,
                'time_bucket': time_bucket,
                'tile_id': tile_id,
                'tile_level': tile_level,
                'tile_index': tile_index
            },
            containerOverrides={
                'memory': memory,
                'vcpus': vcpus,
                'command': [
                    '/scripts/work.py',
                    '--s3-reporter-bucket',
                    'Ref::s3_reporter_bucket',
                    '--s3-datastore-bucket',
                    'Ref::s3_datastore_bucket',
                    '--s3-reporter-keys',
                    'Ref::s3_reporter_keys',
                    '--time-bucket',
                    'Ref::time_bucket',
                    '--tile-id',
                    'Ref::tile_id',
                    '--tile-level',
                    'Ref::tile_level',
                    '--tile-index',
                    'Ref::tile_index'
                ]
            }
        )

def split(l, n):
    size = int(math.ceil(len(l)/float(n)))
    cutoff = len(l) % n
    result = []
    pos = 0
    for i in range(0, n):
        end = pos + size if i < cutoff else pos + size - 1
        result.append(l[pos:end])
        pos = end
    return result

env = os.getenv('DATASTORE_ENV', 'BOGUS') # required, 'prod' or 'dev'
sleep_between_runs = os.getenv('SLEEP_BETWEEN_RUNS', 120) # optional
max_keys = os.getenv('MAX_KEYS', 100) # optional
bucket_interval = os.getenv('BUCKET_INTERVAL', 3600) # optional

if env == 'BOGUS':
    logger.error('DATASTORE_ENV environment variable not set! Exiting.')
    sys.exit(1)
else:
    sleep_between_runs = int(sleep_between_runs)
    max_keys = int(max_keys)
    bucket_interval = int(bucket_interval)
    work_bucket = 'reporter-work-' + env
    reporter_bucket = 'reporter-drop-' + env
    datastore_bucket = 'datastore-output-' + env
    job_queue = 'datastore-' + env
    job_def = 'datastore-' + env
    lock_bucket = 'datastore-lambda-lock-' + env

# do work
s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')
batch_client = boto3.client('batch')

batch_queue_status = batch_check_queue(batch_client, job_queue)
if batch_queue_status == 'processing':
    logger.info('Run complete!')
    logger.info('Sleeping before next run...')
    time.sleep(sleep_between_runs)
else:
    logger.info('No jobs in the queue.')

    # get down to work
    s3_clean_work_bucket(s3_resource, work_bucket)

    s3_data = s3_get_data(s3_client, reporter_bucket, max_keys)

    # if the array is empty, abort
    if not s3_data:
        logger.info('Found no keys! Passing on this run.')
        logger.info('Run complete!')
        logger.info('Sleeping before next run...')
        time.sleep(sleep_between_runs)
    else:
        # copy delete data
        chunks = split(s3_data, 10)
        threads = []
        for chunk in chunks:
            threads.append(threading.Thread(target=s3_move_data, args=(chunk, work_bucket, reporter_bucket)))
            threads[-1].start()
        for t in threads:
            t.join()
        dictionary = build_dictionary(s3_data, bucket_interval)
        build_jobs(dictionary, batch_client, job_queue, job_def, work_bucket, datastore_bucket)

        logger.info('Run complete!')
        logger.info('Sleeping before next run...')

        time.sleep(sleep_between_runs)
