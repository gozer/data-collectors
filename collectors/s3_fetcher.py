#!/usr/bin/env python
###############################################################################
# This script downloads an object from AWS S3.
#
# The S3 object is downloaded in parts with multiple threads, each thread
# downloading a different part.  Processing in parts allows large files
# to be downloaded without requiring large amounts of memory, and
# improves performance by downloading multiple parts in parallel.
#
# Usage:
#   s3_fetch -p aws_profile -f output_file -b s3_bucket -s s3_key
#          [-m metadata_file] [-t threads] [-d debug_level]
#
# Parameter descriptions:
#   aws_profile:   name of your AWS profile
#   output_file:   output file name
#   s3_bucket:     name of the S3 bucket where your object exists
#   s3_key:        key (path/name) of the S3 object to be downloaded
#   metadata_file: write S3 object metadata, if it exists, to this file
#   debug_level:   default is INFO
#
# Examples:
#   s3_get.py -p GR_GG_COF_AWS_Techops_Dev_Developer
#     -f local_file.dat -b s3_bucket_name -s s3_key_name
###############################################################################

from __future__ import print_function
import os
import sys
import argparse
import logging
import time as t
import datetime as dt
import base64
import json
import threading
import gc
import traceback
import boto3
import botocore

from botocore.client import Config

class ReadException(Exception):
    pass

def main():
    class Parts(object):
        ''' Download an S3 object in multiple parts, each in a different
        '''
        def __init__(self):
            self.num_s3_parts = int(s3_object['PartsCount'])
            self.max_running = args.threads
            logging.info("S3 object parts: %d" % self.num_s3_parts)
            logging.info("Max running threads: %d" % self.max_running)
            self.parts = ['0-index not used']  # S3 part numbers are 1-relative
            self.total_size_read = 0
            self.total_read_secs = 0
            self.max_read_secs = 0

        def num_parts(self):
            return len(self.parts) - 1

        def num_running(self):
            return len([p for p in self.parts[1:] if p['status'] == 'RUNNING'])

        def num_downloaded(self):
            return len([p for p in self.parts[1:]
                       if p['status'] in ['DOWNLOADED','WRITTEN']])

        def num_written(self):
            return len([p for p in self.parts[1:] if p['status'] == 'WRITTEN'])

        def num_failed(self):
            return len([p for p in self.parts[1:] if p['status'] == 'FAILED'])

        def download_part(self, part_num):
            ''' Download a specific part of the S3 object '''
            try:
                logging.debug("Starting thread %d" % part_num)
                part = s3.get_object(Bucket=args.bucket, Key=args.s3_key,
                                     PartNumber=part_num)
                before_read = current_ts()

                data = part['Body'].read()
                self.parts[part_num]['time'] = current_ts() - before_read
                if len(data) != part['ContentLength']:
                    logging.error("Expected %d bytes, got %d!" %
                                  (len(data), part['ContentLength']))
                    raise ReadException
                self.parts[part_num]['data'] = data
                self.parts[part_num]['status'] = 'DOWNLOADED'
                logging.debug("Ending thread %d, size %d" %
                              (part_num, len(data)))
            except Exception as e:
                self.parts[part_num]['status'] = 'FAILED'
                logging.error("Thread %d failed" % part_num)
                traceback.print_exc()

        def start_thread(self):
            ''' Start a new thread to download a part of the S3 object '''
            part_num = len(self.parts)
            new_thread = threading.Thread(name="Thread"+str(part_num),
                                          target=self.download_part,
                                          args=(part_num,))
            self.parts.append({'thread':new_thread,
                               'time':0,
                               'data':None,
                               'status':'RUNNING'})
            new_thread.start()

        def write_part(self, part_num):
            ''' Write the part to the file.  All parts must be written in
                order.
            '''
            part = self.parts[part_num]

        def collect_thread(self):
            ''' Collect each thread write the file part, in order.
                Garbage collect
                to prevent large amount of memory usage when the file is large.
            '''
            part_num = self.num_written()+1
            if (self.num_parts() >= part_num and
                self.parts[part_num]['status'] == 'DOWNLOADED'
               ):
                part = self.parts[part_num]
                logging.debug("Downloaded part %d in %d seconds" % (
                              part_num, part['time']))
                self.max_read_secs = (
                    part['time'] if part['time'] > self.max_read_secs
                    else self.max_read_secs)
                self.total_read_secs += part['time']
                self.total_size_read += len(part['data'])
                self.write_part(part_num)
                part['status'] = 'WRITTEN'
                part['data'] = None
                gc.collect()  # Clean-up unreferenced data.
                return True
            else:
                return False

            # Loop until any child thread fails or all parts have been written.
            while (self.num_failed() == 0 and
                   self.num_written() < self.num_s3_parts):
                # Start a new thread if...
                #  1) We're running fewer than the max allowed
                #  2) There are still more parts to process
                if (self.num_running() < self.max_running and
                    self.num_parts() < self.num_s3_parts
                   ):
                    self.start_thread()
                    if self.num_written() < self.num_downloaded() - 300:
                        self.collect_thread()
                elif not self.collect_thread():
                    # Sleep if a thread was not started nor collected.
                    t.sleep(5)
                logging.info("parts: total %d, started %d, running %d, "
                             "downloaded %d, written %d" %
                             (self.num_s3_parts, self.num_parts(),
                              self.num_running(), self.num_downloaded(),
                              self.num_written()))

            fout.close()
            if self.num_failed() == 0:
                logging.info("All parts downloaded: total_size_read %d, "
                    "max_read_secs %d, avg_read_secs %d" % (
                    self.total_size_read, self.max_read_secs,
                    int(self.total_read_secs/self.num_s3_parts)))
            else:
                if args.file:
                    os.unlink(args.file)
            return self.num_failed()

    # Get arguments
    aparser = argparse.ArgumentParser()
    aparser.add_argument("-p", "--profile", required=True, help="AWS profile")
    aparser.add_argument("-f", "--file", required=True,
        help="name of file to be created with the S3 object")
    aparser.add_argument("-b", "--bucket", required=True, help="S3 bucket")
    aparser.add_argument("-s", "--s3_key", required=True, help="S3 key")
    aparser.add_argument("-m", "--metadata_file",
        help="name of local file to be created with S3 object metadata")
    aparser.add_argument("-t", "--threads", type=int, default=5,
        help="number of threads for parallel download")
    aparser.add_argument("-d", "--debug_level", default="INFO",
        help="debug level (default is INFO)")
    args = aparser.parse_args()

    # Configure logging
    cformat = "%(threadName)s:%(asctime)s:%(levelname)s:%(message)s"
    cdatefmt = "%Y-%m-%dT%H:%M:%SZ"
    logging.Formatter.converter = t.gmtime
    logging.basicConfig(format=cformat, datefmt=cdatefmt, stream=sys.stdout,
                        level=args.debug_level)
    #logging.getLogger("boto3").setLevel(logging.INFO)
    #logging.getLogger("botocore").setLevel(logging.INFO)

#    if not (os.environ.get('http_proxy') and os.environ.get('https_proxy')):
#        logging.error("http_proxy and https_proxy must be set")
#        sys.exit(1)

    logging.info("profile: %s", args.profile)
    logging.info("file: %s", args.file)
    logging.info("bucket: %s", args.bucket)
    logging.info("s3_key: %s", args.s3_key)
    logging.info("metadata_file: %s", args.metadata_file)
    args.threads = min(10, args.threads)   # max 10 threads
    logging.info("threads: %d", args.threads)

    # Create AWS session for the given profile.
    session = boto3.Session(profile_name=args.profile)
    s3 = session.client('s3', config=Config(signature_version='s3v4',
                                            read_timeout=300))

    # Get object info from S3.
    logging.info("Getting object metadata from s3://%s/%s" %
                 (args.bucket, args.s3_key))
    s3_object = s3.get_object(Bucket=args.bucket, Key=args.s3_key)
    ##                          PartNumber=1)

    # Initiate the process to download the S3 object.
    logging.info("Downloading parts of data from s3://%s/%s..." %
                 (args.bucket, args.s3_key))
    fout = open(args.file, "wb")
    parts = Parts()
    if metadata and args.metadata_file:
        logging.info("Writing metadata to %s" % args.metadata_file)
        with open(args.metadata_file, "w") as mf:
            mf.write(json.dumps(metadata))

if __name__ == '__main__':
    main()
