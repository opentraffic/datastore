#!/usr/bin/env python3

import os
import sys
import time
import glob
import boto3
import argparse
import subprocess

def cleanup(delete_array):
    # print('[INFO] deleting source objects from bucket ' + args.s3_reporter_bucket)
    pass
    # delete the original keys from the s3_reporter_bucket
    #print('[INFO] deleting source objects from bucket ' + args.s3_reporter_bucket)
    #response = s3_client.delete_objects(
    #    Bucket = args.s3_reporter_bucket,
    #    Delete = { 'Objects': delete_array }
    #    )

def upload(time_bucket, tile_level, tile_index, s3_datastore_bucket):
    print('[INFO] uploading data')
    s3_client = boto3.client('s3')
    to_time = time.gmtime(time_bucket * 3600)

    uploads = ['.fb', '.orc']
    for file_extension in uploads:
        # path key: year/month/day/hour/tile_level/tile_index.fb
        time_key = str(to_time[0]) + '/' + str(to_time[1]) + '/' + str(to_time[2]) + '/' + str(to_time[3]) + '/' + str(tile_level) + '/' + str(tile_index) + file_extension

        data = open(str(tile_index) + file_extension, 'rb')
        response = s3_client.put_object(Bucket = s3_datastore_bucket, ContentType = 'binary/octet-stream', Body = data, Key = time_key)
        data.close()

def convert(tile_index, time_bucket, tile_id):
    print('[INFO] running conversion process')
    sys.stdout.flush()

    fb_out_file = str(tile_index) + '.fb'
    orc_out_file = str(tile_index) + '.orc'

    # TODO: no idea if the exception handling works
    try:
        process = subprocess.check_output(['datastore-histogram-tile-writer', '-b', str(time_bucket), '-t', str(tile_id), '-v', '-f', fb_out_file, '-o', orc_out_file] + glob.glob('*'), timeout=180, universal_newlines=True, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as tilewriter:
        print('[ERROR] Failed running datastore-histogram-tile-writer:', tilewriter.returncode, tilewriter.output)
        sys.exit([tilewriter.returncode])

    print('[INFO] Finished running conversion')

def download(keys_array):
    # this obviously isn't gonna really work... we'll
    # need to maintain the S3 path as a local filesystem
    # path to preserve everything... or preserve them in
    # an object of k,v
    client = boto3.client('s3')
    response = client.list_objects_v2(Bucket=args.s3_reporter_bucket)

    delete_array = []
    s3_resource = boto3.resource('s3')
    for key in keys_array:
        object_id = key.rsplit('/', 1)[-1]
        print('[INFO] downloading ' + object_id + ' from s3')

        s3_resource.Object(args.s3_reporter_bucket, key).download_file(object_id)
        delete_array.append( { 'Key': key } )

    # TODO: untested
    return delete_array

if __name__ == "__main__":
    # build args
    parser = argparse.ArgumentParser()
    parser.add_argument('s3_reporter_bucket', type=str,help='Bucket (e.g. reporter-work-prod) in which the data we wish to process is located')
    parser.add_argument('s3_datastore_bucket', type=str, help='Bucket (e.g. datastore-output-prod) into which we will place transformed data')
    parser.add_argument('s3_reporter_keys', type=str, help='S3 object keys which we will operate on, found in the s3_reporter_bucket')
    parser.add_argument('time_bucket', type=int, help='The time bucket')
    parser.add_argument('tile_id', type=int, help='The tile ID')
    parser.add_argument('tile_level', type=int, help='The tile level')
    parser.add_argument('tile_index', type=int, help='The tile index')
    args = parser.parse_args()

    print('[INFO] reporter intput bucket: ' + args.s3_reporter_bucket)
    print('[INFO] datastore output bucket: ' + args.s3_datastore_bucket)
    print('[INFO] time bucket: ' + str(args.time_bucket))
    print('[INFO] tile id: ' + str(args.tile_id))
    print('[INFO] tile level: ' + str(args.tile_level))
    print('[INFO] tile index: ' + str(args.tile_index))

    # do work
    delete_list = download(args.s3_reporter_keys.split(','))
    convert(args.tile_index, args.time_bucket, args.tile_id)
    upload(args.time_bucket, args.tile_level, args.tile_index, args.s3_datastore_bucket)
    cleanup(delete_list) # TODO: untested

    print('[INFO] run complete')
