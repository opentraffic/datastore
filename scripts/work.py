#!/usr/bin/env python3

import sys
import time
import glob
import argparse
import subprocess
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

def get_time_key(time_bucket, tile_level, tile_index):
    # path key: year/month/day/hour/tile_level/tile_index
    to_time = time.gmtime(time_bucket * 3600)
    time_key = str(to_time[0]) + '/' + str(to_time[1]) + '/' + str(to_time[2]) + '/' + str(to_time[3]) + '/' + str(tile_level) + '/' + str(tile_index)
    return time_key

def upload(time_key, s3_datastore_bucket):
    logger.info('uploading data to bucket: ' + s3_datastore_bucket)
    s3_client = boto3.client('s3')

    uploads = ['.fb']
    for file_extension in uploads:
        key = time_key + file_extension

        data = open(time_key.rsplit('/', 1)[-1] + file_extension, 'rb')
        s3_client.put_object(
            Bucket=s3_datastore_bucket,
            ContentType='binary/octet-stream',
            Body=data,
            Key=key)
        data.close()

def convert(tile_index, time_bucket, tile_id):
    logger.info('running conversion process')
    sys.stdout.flush()

    fb_out_file = str(tile_index) + '.fb'

    # TODO: no idea if the exception handling works
    try:
        output = subprocess.check_output(['datastore-histogram-tile-writer', '-b', str(time_bucket), '-t', str(tile_id), '-f', fb_out_file] + glob.glob('*'), timeout=180, universal_newlines=True, stderr=subprocess.STDOUT)
        for line in output.splitlines():
            logger.info(line)
    except subprocess.CalledProcessError as tilewriter:
        logger.error('Failed running datastore-histogram-tile-writer: ' + str(tilewriter.returncode) + ' ' + str(tilewriter.output))
        sys.exit(tilewriter.returncode)
    logger.info('Finished running conversion')

def get_files(keys, s3_reporter_bucket, s3_datastore_bucket):
    for key in keys:
        object_id = key.rsplit('/', 1)[-1]
        session = boto3.session.Session()
        s3_resource = session.resource('s3')
        logger.info('downloading ' + object_id + ' from s3 bucket: ' + s3_reporter_bucket)
        try:
            if key.endswith('.fb'):
                logger.info('downloading ' + key + ' as ' + object_id + ' from s3 bucket: ' + s3_datastore_bucket)
                s3_resource.Object(s3_datastore_bucket, key).download_file(object_id)
            else:
                logger.info('downloading ' + key + ' as ' + object_id + ' from s3 bucket: ' + s3_reporter_bucket)
                s3_resource.Object(s3_reporter_bucket, key).download_file(object_id)
        except Exception as e:
            logger.error('Failed to download key: %s' % e)

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

def download_data(prefixes_array, s3_reporter_bucket, s3_datastore_bucket, time_key):
    client = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    
    # get the keys
    keys = []
    for prefix in prefixes_array:
        token = None
        first = True
        while first or token:
            if token:
                objects = client.list_objects_v2(Bucket=s3_reporter_bucket, Delimiter='/', Prefix=prefix, ContinuationToken=token)
            else:
                objects = client.list_objects_v2(Bucket=s3_reporter_bucket, Delimiter='/', Prefix=prefix)
            keys.extend([ o['Key'] for o in objects['Contents']])
            token = objects.get('NextContinuationToken')
            first = False
    # add the key for the existing file
    keys.append(time_key + '.fb')

    # download the files
    keys = split(keys, 10)
    threads = []
    for chunk in keys:
        threads.append(threading.Thread(target=get_files, args=(chunk, s3_reporter_bucket, s3_datastore_bucket)))
        threads[-1].start()
    for t in threads:
        t.join()

if __name__ == "__main__":
    # build args
    parser = argparse.ArgumentParser()
    parser.add_argument('--s3-reporter-bucket', type=str, help='Bucket (e.g. reporter-work-prod) in which the data we wish to process is located')
    parser.add_argument('--s3-datastore-bucket', type=str, help='Bucket (e.g. datastore-output-prod) into which we will place transformed data')
    parser.add_argument('--s3-reporter-keys', type=str, help='S3 object keys which we will operate on, found in the s3_reporter_bucket')
    parser.add_argument('--time-bucket', type=int, help='The time bucket')
    parser.add_argument('--tile-id', type=int, help='The tile ID')
    parser.add_argument('--tile-level', type=int, help='The tile level')
    parser.add_argument('--tile-index', type=int, help='The tile index')
    args = parser.parse_args()

    logger.info('reporter input bucket: ' + args.s3_reporter_bucket)
    logger.info('datastore output bucket: ' + args.s3_datastore_bucket)
    logger.info('time bucket: ' + str(args.time_bucket))
    logger.info('tile id: ' + str(args.tile_id))
    logger.info('tile level: ' + str(args.tile_level))
    logger.info('tile index: ' + str(args.tile_index))

    # do work
    time_key = get_time_key(args.time_bucket, args.tile_level, args.tile_index)

    download_data(
        args.s3_reporter_keys.split(','),
        args.s3_reporter_bucket,
        args.s3_datastore_bucket,
        time_key)
    convert(args.tile_index, args.time_bucket, args.tile_id)
    upload(time_key, args.s3_datastore_bucket)

    logger.info('run complete')
