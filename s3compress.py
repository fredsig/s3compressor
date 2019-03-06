#!/usr/bin/env python

import os
from collections import defaultdict
import boto3
import argparse
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig
import zipfile
import shutil

s3 = boto3.client('s3')
s3_resource = boto3.resource('s3')
archive_bucket = 'my_bucket'
data_dir = '/data/'

config = TransferConfig(
    multipart_threshold = 1048576,
    multipart_chunksize = 1048576,
    max_concurrency = 10,
    num_download_attempts = 10,
    use_threads = True
)

def parse_args():
    parser = argparse.ArgumentParser(description="S3 compressor")
    parser.add_argument('--bucket', help='S3 bucket')
    return parser.parse_args()

def build_dict(bucket):
    object_dict = defaultdict(list)
    try:
        paginator = s3.get_paginator('list_objects')
        pages = paginator.paginate(Bucket=bucket)
    except ClientError as e:
        print("Unable to list objects on bucket %s: %s" % (bucket, e))
    count = 0
    for page in pages:
        for object in page['Contents']:
            sources, year, month, day, remain = object['Key'].split('/', 4)
            yearmonthday = year + month + day
            object_dict[yearmonthday].append(object['Key'])
        count += 1000
        print("Building list of objects: %s" % count)
    return object_dict

def download_file(bucket, yearmonthday, objects):
    directory = data_dir + bucket + '/' + yearmonthday
    if not os.path.exists(directory):
        os.makedirs(directory)
    else:
        print("Directory %s already exists." % directory)
    for key in objects:
        sources, year, month, day, remain = key.split('/', 4)
        file = directory + '/' + remain.replace('/', '_')
        try:
            s3_resource.meta.client.download_file(bucket, key, file, Config=config)
        except ClientError as e:
            print("Unable to download object %s on bucket %s: %s" % (object, bucket, e))
            return False
    return directory

def create_archive(directory):
    z = zipfile.ZipFile(directory + '.zip', 'w', zipfile.ZIP_DEFLATED, allowZip64=True)
    for root, dirs, files in os.walk(directory):
        for filename in files:
            z.write(root + '/' + filename, filename)
    z.printdir()
    z.close()
    return directory + '.zip'

def upload_archive(bucket, zip_file):
    key = bucket + '/' + zip_file.split(data_dir + bucket + '/')[1]
    upload_args = {
        'StorageClass' : 'GLACIER'
    }
    print ("Uploading %s to %s/%s" % (zip_file, archive_bucket, key))
    try:
        s3_resource.meta.client.upload_file(zip_file, archive_bucket, key, ExtraArgs=upload_args)
    except ClientError as e:
        print("Unable to upload zip_file %s on bucket %s: %s" % (key, archive_bucket, e))
        return False
    return True

def delete_archive(bucket, yearmonthday, objects):
    for object in objects:
        print("Deleting %s from %s" % (object, bucket))
        try:
            delete = s3_resource.Object(bucket, object).delete()
        except ClientError as e:
            print("Unable to delete object %s on bucket %s: %s" % (object, bucket, e))
            return False
    path = data_dir + bucket + '/' + yearmonthday
    print("Deleting %s/*" % path)
    shutil.rmtree(path)
    os.remove(path + '.zip')

def main():
    args = parse_args()
    bucket = args.bucket
    object_dict = build_dict(bucket)
    for yearmonthday, objects in object_dict.items():
        print yearmonthday, objects
        directory = download_file(bucket, yearmonthday, objects)
        if directory:
            zip_file = create_archive(directory)
            if upload_archive(bucket, zip_file):
                delete_archive(bucket, yearmonthday, objects)


if __name__ == '__main__':
    main()
