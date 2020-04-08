
"""
Created on Wed Sep 12 12:41:35 2019

@author: yizhe

"""

import numpy as np
import os
import sys
import boto3
from Climate_Marble_basicfusion_MODIS import main_bf_MODIS
from Climate_Marble_basicfusion_CERES import main_bf_CERES
from Climate_Marble_basicfusion_MISR import main_bf_MISR


iyr = int(sys.argv[1])
imon = int(sys.argv[2])

session = boto3.session.Session(profile_name='nasa_ro')
s3 = session.resource('s3')

s3_local = boto3.client('s3')
bucket_name = 's3yizhe'

bf_path_s3_all = []
for i in s3.Bucket(name='msfc-terrafusion-1.0').objects.filter(Prefix='{}.{}/TERRA_BF_L1B'.format(iyr, str(imon).zfill(2))):
    bf_path_s3_all.append(i.key)


for iii, bf_path_s3 in enumerate(bf_path_s3_all):
    bf_name = bf_path_s3.split('/')[1]

    # Download object to the file
    print ("Downloading the BF file from NASA S3 bucket...{}/{}".format(iii, len(bf_path_s3_all)))
    s3.Bucket('msfc-terrafusion-1.0').download_file(bf_path_s3, bf_name)

    # Run script
    print ("Running Climarble script...")
    try:
        nc_name = main_bf_MODIS(bf_name, '', SPATIAL_RESOLUTION=0.5, VZA_MAX=18, CATEGORY='VIS')
        nc_name = main_bf_MISR(bf_name, '', SPATIAL_RESOLUTION=0.5, VZA_MAX=18, CAMERA='AN')
        nc_name = main_bf_CERES(bf_name, '', SPATIAL_RESOLUTION=0.5, VZA_MAX=18, MODE='ct')
    except:
        err_orbit = bf_name.replace('.h5', '.txt')
        np.savetxt(err_orbit, [1])
        # Move txt file to my s3
        s3_local.upload_file(err_orbit, bucket_name, 'climarble/{}.{}/'.format(iyr, str(imon).zfill(2))+err_orbit)
        # Check nc file existence
        if os.path.isfile(nc_name):
            os.remove(nc_name)
    else:
        # Move nc file to my s3
        s3_local.upload_file(nc_name, bucket_name, 'climarble/{}.{}/'.format(iyr, str(imon).zfill(2))+nc_name)
        os.remove(nc_name)

    # Delete used BF file and move to the next BF data
    print("Delete data files - {}".format(bf_name))
    os.remove(bf_name)
