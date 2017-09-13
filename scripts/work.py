#!/usr/bin/env python3

import sys
import time
import glob
import argparse
import subprocess
import boto3
from functools import partial
from multiprocessing.pool import ThreadPool
from botocore.exceptions import ClientError

def get_time_key(time_bucket, tile_level, tile_index):
    # path key: year/month/day/hour/tile_level/tile_index
    to_time = time.gmtime(time_bucket * 3600)
    time_key = str(to_time[0]) + '/' + str(to_time[1]) + '/' + str(to_time[2]) + '/' + str(to_time[3]) + '/' + str(tile_level) + '/' + str(tile_index)
    return time_key

def upload(time_key, s3_datastore_bucket):
    print('[INFO] uploading data to bucket: ' + s3_datastore_bucket)
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
    print('[INFO] running conversion process')
    sys.stdout.flush()

    fb_out_file = str(tile_index) + '.fb'

    # TODO: no idea if the exception handling works
    try:
        output = subprocess.check_output(['datastore-histogram-tile-writer', '-b', str(time_bucket), '-t', str(tile_id), '-f', fb_out_file] + glob.glob('*'), timeout=180, universal_newlines=True, stderr=subprocess.STDOUT)
        for line in output.splitlines():
            print('[INFO]', line)
    except subprocess.CalledProcessError as tilewriter:
        print('[ERROR] Failed running datastore-histogram-tile-writer:', tilewriter.returncode, tilewriter.output)
        sys.exit([tilewriter.returncode])

    print('[INFO] Finished running conversion')

def get_file(key, **kwargs):
    object_id = key.rsplit('/', 1)[-1]
    s3_reporter_bucket = kwargs['rbucket']
    s3_datastore_bucket = kwargs['dbucket']
    session = boto3.session.Session()
    s3_resource = session.resource('s3')
    print('[INFO] downloading ' + object_id + ' from s3 bucket: ' + s3_reporter_bucket)
    try:
        if key.endswith('.fb'):
            s3_resource.Object(s3_datastore_bucket, key).download_file(object_id)
        else:
            s3_resource.Object(s3_reporter_bucket, key).download_file(object_id)
    except Exception as e:
        print('[ERROR] failed to download key: %s' % e)

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
    pool = ThreadPool(processes=10)
    pool.map(partial(get_file, rbucket=s3_reporter_bucket, dbucket=s3_datastore_bucket), keys)

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

    print('[INFO] reporter input bucket: ' + args.s3_reporter_bucket)
    print('[INFO] datastore output bucket: ' + args.s3_datastore_bucket)
    print('[INFO] time bucket: ' + str(args.time_bucket))
    print('[INFO] tile id: ' + str(args.tile_id))
    print('[INFO] tile level: ' + str(args.tile_level))
    print('[INFO] tile index: ' + str(args.tile_index))

    # do work
    time_key = get_time_key(args.time_bucket, args.tile_level, args.tile_index)

    download_data(
        args.s3_reporter_keys.split(','),
        args.s3_reporter_bucket,
        args.s3_datastore_bucket,
        time_key)
    convert(args.tile_index, args.time_bucket, args.tile_id)
    upload(time_key, args.s3_datastore_bucket)

    print('[INFO] run complete')
