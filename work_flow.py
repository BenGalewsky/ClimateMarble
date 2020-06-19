
"""
Created on Wed Sep 12 12:41:35 2019

@author: yizhe

"""
import io
import json
import traceback

import sys

import numpy as np
import os
import boto3
from Climate_Marble_basicfusion_MODIS import main_bf_MODIS
from Climate_Marble_basicfusion_CERES import main_bf_CERES
from Climate_Marble_basicfusion_MISR import main_bf_MISR
from argparse import ArgumentParser

# Get the service resource
sqs = boto3.resource('sqs', region_name='us-west-2')
s3_client = boto3.client('s3')

parser = ArgumentParser("Compute Radiance")
parser.add_argument("-q", dest="sqs_queue", required=False, help="SQS Work Queue")
parser.add_argument("-u", dest='user', default='admin')
parser.add_argument("-p", dest='password', default='admin')
parser.add_argument("-f", dest='bf_name', help="Basic Fusion File S3 URL", required=False)
parser.add_argument("--hsds", dest='hsds_endpoint', help="HSDS Endpoint", required=False)


def process_single_file():

    if args.hsds_endpoint:
        import h5pyd as h5py
    else:
        import h5py
    # Run script
    print("Running Climarble script...")
    bucket_name = "climatemarble"
    iyr = 2005
    imon = 5
    try:
        f = None
        if args.hsds_endpoint:
            f = h5py.File(args.bf_name, "r",
                          username=args.user, password=args.password,
                          endpoint=args.hsds_endpoint)
        else:
            f = h5py.File(args.bf_name, "r")

        print(f.fid)

        nc_name = main_bf_MODIS(f, '', SPATIAL_RESOLUTION=0.5, VZA_MAX=18, CATEGORY='VIS')
        nc_name = main_bf_MISR(f, '', SPATIAL_RESOLUTION=0.5, VZA_MAX=18, CAMERA='AN')
        nc_name = main_bf_CERES(f, '', SPATIAL_RESOLUTION=0.5, VZA_MAX=18, MODE='ct')
        print(nc_name)
        s3_client.upload_file(nc_name, bucket_name, 'climarble/{}.{}/'.
                              format(iyr, str(imon).zfill(2)) + nc_name)
        os.remove(nc_name)

    except Exception as ex:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        string_out = io.StringIO()
        traceback.print_tb(exc_traceback, limit=20, file=string_out)

        print(str(ex))
        print(string_out.getvalue())


def process_from_queue():
    import h5pyd as h5py

    print("Ready to index files from "+args.sqs_queue)
    queue = sqs.get_queue_by_name(QueueName=args.sqs_queue)
    dead_letter_queue = sqs.get_queue_by_name(QueueName='climate_marble_dead_letter.fifo')

    done = False
    while not done:
        messages = queue.receive_messages(WaitTimeSeconds=1)
        if not messages:
            break

        for message in messages:
            message_id = message.message_id
            job_record = json.loads(message.body)
            print(job_record)
            message.delete()

            iyr = job_record['year']
            imon = job_record['month']
            bf_name = job_record['terra-file']

            bucket_name = "climatemarble"

            # Run script
            print("Running Climarble script...")
            try:
                f = h5py.File(bf_name, "r",
                              username=args.user, password=args.password, endpoint=job_record['hsds-endpoint'])

                print(f.fid)

                nc_name = main_bf_MODIS(f, '', SPATIAL_RESOLUTION=0.5, VZA_MAX=18, CATEGORY='VIS')
                nc_name = main_bf_MISR(f, '', SPATIAL_RESOLUTION=0.5, VZA_MAX=18, CAMERA='AN')
                nc_name = main_bf_CERES(f, '', SPATIAL_RESOLUTION=0.5, VZA_MAX=18, MODE='ct')
                print(nc_name)
                s3_client.upload_file(nc_name, bucket_name, 'climarble/{}.{}/'.
                                      format(iyr,str(imon).zfill(2)) + nc_name)
                os.remove(nc_name)

            except Exception as ex:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                string_out = io.StringIO()
                traceback.print_tb(exc_traceback, limit=20, file=string_out)

                print(str(ex))
                print(string_out.getvalue())

                dead_letter_record = {
                    "error": str(ex),
                    "traceback": string_out.getvalue(),
                    "job_record": job_record.copy()
                }
                response = dead_letter_queue.send_message(
                    MessageBody=json.dumps(dead_letter_record),
                    MessageGroupId='hsds',
                    MessageDeduplicationId=message_id
                )
    # else:
    #     # Move nc file to my s3
    #     s3_client = boto3.client('s3')
    #     s3_client.upload_file(nc_name, bucket_name, 'climarble/{}.{}/'.format(iyr, str(imon).zfill(2))+nc_name)
    #     os.remove(nc_name)


args = parser.parse_args()

if args.sqs_queue:
    process_from_queue()
else:
    process_single_file()
