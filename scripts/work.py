#!/usr/bin/env python

import os
import boto3
import argparse
import subprocess

def cleanup():
    # delete the original keys from the s3_reporter_bucket
    #print '[INFO] deleting source objects from bucket ' + args.s3_reporter_bucket
    #response = s3_client.delete_objects(
    #    Bucket = args.s3_reporter_bucket,
    #    Delete = { 'Objects': delete_array }
    #    )

def upload():
    s3_client = boto3.client('s3')
    uploads = ['flatbuffer_file', 'orc_file']
    for file in uploads:
        response = s3_client.put_object(Bucket = args.s3_datastore_bucket, Key = file, ContentType = 'binary/octet-stream')

def convert():
    # run our java thingy: the Docker container workdir will have already put us
    #   in the right place to, ummm, do work

    # TODO: error handling?
    #cmd = 'datastore-histogram-tile-writer --time-bucket' + ' ' + str(args.time_bucket) + ' ' + '--tile ' + str(args.tile_id) + ' ' + '-f flatbuffer_file -o orc_file ./*'
    cmd = 'echo WHAT_IS_GOING_ON'

    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    process.wait()
    print '[INFO] Finished running conversion, return code: ' + process.returncode

def download(keys_array):
    # download stuff
    print '[INFO] downloading data from s3'

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
        print '[INFO] Downloading ' + object_id + ' from s3'

        s3_resource.Object(args.s3_reporter_bucket, key).download_file(object_id)
        delete_array.append( { 'Key': key } )

if __name__ == "__main__":
    # build args
    parser = argparse.ArgumentParser()
    parser.add_argument('s3_reporter_bucket', type=str,help='Bucket (e.g. reporter-drop-prod) in which the data we wish to process is located')
    parser.add_argument('s3_datastore_bucket', type=str, help='Bucket (e.g. datastore-output-prod) into which we will place transformed data')
    parser.add_argument('s3_reporter_keys', type=str, help='S3 object keys which we will operate on, found in the s3_reporter_bucket'
    parser.add_argument('time_bucket', type=int, help='The time bucket')
    parser.add_argument('tile_id', type=int, help='The tile ID')
    args = parser.parse_args()

    print '[INFO] reporter intput bucket: ' + args.s3_reporter_bucket
    print '[INFO] datastore output bucket: ' + args.s3_datastore_bucket
    print '[INFO] time bucket: ' + str(args.time_bucket)
    print '[INFO] tile: ' + str(args.tile_id)

    # download stuff
    print '[INFO] downloading data from s3'
    download(args.s3_reporter_keys.split(','))

    # convert stuff
    print '[INFO] running conversion process'
    convert()

    # TODO: upload the result to s3_datastore_bucket
    print '[INFO] uploading resulting files'
    upload()
    
    # TODO: cleanup stuff
    print '[INFO] deleting source objects from bucket ' + args.s3_reporter_bucket
    cleanup()

    print '[INFO] run complete'
