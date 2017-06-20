#!/usr/bin/env python

import os
import boto3
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('s3_reporter_bucket', type=str, help='Bucket (e.g. reporter-drop-prod) in which the data we wish to process is located')
    parser.add_argument('s3_datastore_bucket', type=str, help='Bucket (e.g. datastore-output-prod) into which we will place transformed data')
    parser.add_argument('reporter_s3_keys', type=str, help='S3 object keys which we will operate on, found in the s3_reporter_bucket')
    args = parser.parse_args()

print 'Reporter intput bucket: ' + args.s3_reporter_bucket
print 'Datastore output bucket: ' + args.s3_datastore_bucket

# parse our key list
keys_array = args.reporter_s3_keys.split(',')

# keep a list of all the files we iterate over to delete later in a bulk operation
delete_array = []

# download stuff
s3 = boto3.resource('s3')
for key in keys_array:
    object_id = key.rsplit('/', 1)[-1]
    s3.Object(args.s3_reporter_bucket, key).download_file(object_id)
    delete_array.append( { 'Key': key } ) 	   


# run our java thingy: the Docker container workdir will have already put us
#   in the right place to, ummm, do work

# TODO: program -f flatbuffer_file -o orc_file *

# TODO: upload the result to s3_datastore_bucket
for upload_file in os.listdir('.'):
    client = boto3.client('s3')
    response = client.put_object(
                Bucket = args.s3_datastore_bucket,
                Key = upload_file,
                ContentType = 'binary/octet-stream'
                )

# delete the original keys from the s3_reporter_bucket
print 'Deleting source objects from bucket ' + args.s3_reporter_bucket

client = boto3.client('s3')
response = client.delete_objects(
    Bucket = args.s3_reporter_bucket,
    Delete = { 'Objects': delete_array }
    )
