
"""
Created on Wed Sep 12 12:41:35 2019

@author: yizhe

"""

import numpy as np
import os
import h5pyd as h5py
import boto3
from Climate_Marble_basicfusion_MODIS import main_bf_MODIS
from Climate_Marble_basicfusion_CERES import main_bf_CERES
from Climate_Marble_basicfusion_MISR import main_bf_MISR
from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument('--month', help='two digit month', type=int, required=True)
parser.add_argument('--year', help="two digit year", type=int, required=True)
parser.add_argument('--user', help='HSDS User', default='admin')
parser.add_argument('--password', help='HSDS Password', default='admin')
parser.add_argument('--endpoint', help='HSDS Endpoint url', required=True)
parser.add_argument('--file', help='HDS5 Domain/File',
                    default="/terra/TERRA_BF_L1B_O9820_20011022161559_F000_V001.h5")

args = parser.parse_args()
iyr = args.year
imon = args.month
bf_name = args.file

bucket_name = "climatemarble"
# bf_name = "/data/TERRA_BF_L1B_O85079_20151216160910_F000_V001.h5"
# Run script
print("Running Climarble script...")
try:
    f = h5py.File(args.file, "r",
                  username=args.user, password=args.password, endpoint=args.endpoint)

    nc_name = main_bf_MODIS(f, '', SPATIAL_RESOLUTION=0.5, VZA_MAX=18, CATEGORY='VIS')
    nc_name = main_bf_MISR(f, '', SPATIAL_RESOLUTION=0.5, VZA_MAX=18, CAMERA='AN')
    nc_name = main_bf_CERES(f, '', SPATIAL_RESOLUTION=0.5, VZA_MAX=18, MODE='ct')
    print(nc_name)
except Exception as ex:
    err_orbit = bf_name.replace('.h5', '.txt')
    print(err_orbit)
    raise ex
else:
    # Move nc file to my s3
    s3_client = boto3.client('s3')
    s3_client.upload_file(nc_name, bucket_name, 'climarble/{}.{}/'.format(iyr, str(imon).zfill(2))+nc_name)
    os.remove(nc_name)
