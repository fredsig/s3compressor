#!/usr/bin/env python
# Copyright 2019 Frederico Marques

import os
import boto3
import argparse
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig
import zipfile
import shutil
import threading
from queue import Queue
import time

s3 = boto3.client('s3')
s3_resource = boto3.resource('s3')
data_dir = '/tmp/'

config = TransferConfig(
    multipart_threshold = 1048576,
    multipart_chunksize = 1048576,
    max_concurrency = 10,
    num_download_attempts = 10,
    use_threads = True
)

print_lock = threading.Lock()

def parse_args():
    parser = argparse.ArgumentParser(description="S3 compressor")
    parser.add_argument('--arch_bucket', help='Destination Archive bucket', required=True)
    parser.add_argument('--bucket', help='Source bucket', required=True)
    parser.add_argument('--prefix', default='sources', help='prefix')
    parser.add_argument('--years', help='years. ex: --years 2016,2017', required=True)
    parser.add_argument('--threads', type=int, default=8, help='number of threads')
    return parser.parse_args()

def compressor(bucket, prefix, years):
    for year in years:
        for month in range(1,13):
            for day in range(1,32):
                objects = []
                if day < 10:
                    my_day = '0' + str(day)
                else:
                    my_day = str(day)
                if month < 10:
                    my_month = '0' + str(month)
                else:
                    my_month = str(month)
                my_prefix = prefix + '/' + str(year) + '/' + my_month + '/' + my_day
                try:
                    paginator = s3.get_paginator('list_objects')
                    parameters = {'Bucket': bucket, 'Prefix': my_prefix}
                    pages = paginator.paginate(**parameters)
                except ClientError as e:
                    print("Unable to list objects on bucket %s: %s" % (bucket, e))
                for page in pages:
                    if 'Contents' in page:
                        for object in page['Contents']:
                            objects.append(object['Key'])
                yearmonthday = year + my_month + my_day
                print(yearmonthday)
                print("Total: ", len(objects))
                if objects:
                    directory = get_objects(bucket, yearmonthday, objects)
                    if directory:
                        zip_file = create_archive(directory)
                        if upload_archive(bucket, zip_file):
                            delete_archive(bucket, yearmonthday, objects)
                    else:
                        print("Not deleting files locally or source bucket.")

def get_objects(bucket, yearmonthday, objects):
    directory = data_dir + bucket + '/' + yearmonthday
    if not os.path.exists(directory):
        os.makedirs(directory)
    else:
        print("Directory %s already exists." % directory)
    for key in objects:
        sources, year, month, day, remain = key.split('/', 4)
        file = directory + '/' + remain.replace('/', '_')
        item = {'file' : file, 'key' : key, 'bucket' : bucket}
        download_queue.put(item)
    download_queue.join()
    if not download_queue_dead.empty():
        print("Download failure for one more objects on bucket %s." % download_queue_dead.get())
        return False
    else:
        return directory

def download_object():
    while True:
        item = download_queue.get()
        mydata = threading.local()
        mydata.file = item['file']
        mydata.key = item['key']
        mydata.bucket = item['bucket']
        with print_lock:
            print("Thread : {} - Downloading %s/%s -> %s".format(threading.current_thread().name) % (mydata.bucket, mydata.key, mydata.file))
        try:
            s3_resource.meta.client.download_file(mydata.bucket, mydata.key, mydata.file, Config=config)
        except ClientError as e:
            with print_lock:
                print("Thread : {} - Unable to download object %s on bucket %s: %s".format(threading.current_thread().name) % (mydata.key, mydata.bucket, e))
                download_queue_dead.put(mydata.bucket)
        download_queue.task_done()

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
        item = {'object' : object, 'bucket' : bucket}
        delete_queue.put(item)
    delete_queue.join()
    path = data_dir + bucket + '/' + yearmonthday
    print("Deleting %s/*" % path)
    shutil.rmtree(path)
    os.remove(path + '.zip')

def delete_object():
    while True:
        item = delete_queue.get()
        mydata = threading.local()
        mydata.object = item['object']
        mydata.bucket = item['bucket']
        with print_lock:
            print("Thread : {} - Deleting %s from %s".format(threading.current_thread().name) % (mydata.object, mydata.bucket))
        try:
            delete = s3_resource.Object(bucket, mydata.object).delete()
        except ClientError as e:
            with print_lock:
                print("Thread : {} - Unable to delete object %s on bucket %s: %s".format(threading.current_thread().name) % (mydata.object, mydata.bucket, e))
        delete_queue.task_done()

if __name__ == '__main__':
    args = parse_args()
    archive_bucket = args.arch_bucket
    bucket = args.bucket
    prefix = args.prefix
    years = args.years.split(",")
    threads = args.threads
    download_queue = Queue()
    download_queue_dead = Queue()
    delete_queue = Queue()
    for i in range(threads):
        t = threading.Thread(target=download_object)
        t.daemon = True
        t.start()
    for i in range(threads):
        t = threading.Thread(target=delete_object)
        t.daemon = True
        t.start()
    compressor(bucket, prefix, years)
