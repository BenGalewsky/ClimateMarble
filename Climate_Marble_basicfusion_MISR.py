"""
Author@Yizhe

Created on Oct 17, 2019

Sort MISR radiances (from BF product) to the specified lat/lon grids and then store the gridded daily data.

This script is new.
"""

import numpy as np
import os
import sys
import h5pyd as h5py
import s3fs
import xarray as xr
from Climate_Marble_common_functions import get_descending
from skimage.transform import resize
from scipy.stats import binned_statistic_dd


# if __name__ == "__main__":
#     bf_file = sys.argv[1]
#     SPATIAL_RESOLUTION=0.5; VZA_MAX=18; CAMERA='AN'; output_folder=''

def main_bf_MISR(h5f, output_folder, SPATIAL_RESOLUTION=0.5, VZA_MAX=18, CAMERA='AN'):
    """
    (This script is adapted for running on AWS cloud)
    
    The MISR gridded file for each orbit will be generated directly from the basic fusion data files.
    
    Args:
        bf_file (str)                       : BF file path
        output_folder (str)                 : folder storing the gridded results
        SPATIAL_RESOLUTION (float, optional): spatial resolution of the grid (in degree)
        VZA_MAX (int, optional)             : maximum viewing zenith angle considered (in degree)
        CAMERA (str, optional)              : MISR camera
    
    Returns:
        there is no return value for this function
    """

    # =============================================================================
    # 1. Initialization
    #    calculate constant parameters
    #    initialize output arrays and output hdf5 file
    #    check the number of CERES granules 
    # =============================================================================
    output_nc_name = h5f.fid.split('/')[-1].replace('TERRA_BF_L1B', 'CLIMARBLE')
    output_nc_name = output_nc_name.replace('.h5', '.nc')

    # 
    NUM_POINTS = 1 / SPATIAL_RESOLUTION
    NUM_LATS = int(180 / SPATIAL_RESOLUTION)
    NUM_LONS = int(360 / SPATIAL_RESOLUTION)

    LAT_EDGES = np.arange(-90.0, 90.0001, SPATIAL_RESOLUTION)
    LON_EDGES = np.arange(-180.0, 180.0001, SPATIAL_RESOLUTION)

    # 
    orbit_radiance_sum  = np.zeros((NUM_LATS, NUM_LONS, 4))
    orbit_radiance_num  = np.zeros((NUM_LATS, NUM_LONS, 4))
    orbit_nc_out = os.path.join(output_folder, output_nc_name)


    # =============================================================================
    # 2. Main processing
    #    Loop through each CERES granule and sort radiances into the corresponding lat/lon bins
    #    When encounters an asceding granule, script will move to the next granule
    # =============================================================================

    # USE MODIS granules to match first and last time of the descending node
    MISR_blocks = get_descending(h5f, 'MISR.{}'.format(CAMERA))
    if MISR_blocks[0] == 0:
        print(">> IOError( no available MODIS granule in orbit {} )".format(bf_file))
        return

    # LOAD lat/lon here
    lat = h5f['MISR/Geolocation/GeoLatitude'][:]
    lon = h5f['MISR/Geolocation/GeoLongitude'][:]

    # LOAD radiance here
    MISR_bands = ['Blue', 'Green', 'Red', 'NIR']
    rads_all = []
    for iband in MISR_bands:
        rads_all.append(h5f['MISR/{}/Data_Fields/{}_Radiance'.format(CAMERA, iband)][:])

    # SPECIFY data dimension to interpolate SZA/VZA
    rad_shape = (128, 512)
    

    # LOOP through MISR blocks (starts from 0)
    for iblk in MISR_blocks:

        # INTERPOLATE sza and vza (this part can be replaced by a more accurate function)
        raw_sza = h5f['MISR/Solar_Geometry/SolarZenith'][iblk]
        raw_vza = h5f['MISR/{}/Sensor_Geometry/{}Zenith'.format(CAMERA, ''.join(c.lower() if i==1 else c for i,c in enumerate(CAMERA)))][iblk]
        np.place(raw_sza, raw_sza<0, np.nan)
        np.place(raw_vza, raw_vza<0, np.nan)
        blk_sza = resize(raw_sza, rad_shape)
        blk_vza = resize(raw_vza, rad_shape)


        # SELECT lat/lon
        idx_geometry = np.where((blk_sza<89.0) & (blk_vza<VZA_MAX))
        select_lat = lat[iblk][idx_geometry]
        select_lon = lon[iblk][idx_geometry]


        # SELECT spectral radiances here
        # Aggregate 275-m res data to 1.1-km when necessary
        # Separate band by band to allow one (or more) band(s) failure
        for iband, band_name in enumerate(MISR_bands, start=0):
            blk_rad = rads_all[iband][iblk]
            # blk_rad = h5f['MISR/{}/Data_Fields/{}_Radiance'.format(CAMERA, band_name)][iblk]

            if blk_rad.shape == (512, 2048): 
                # 275-m res band
                np.place(blk_rad, blk_rad<0, np.nan)
                fnl_blk_rad = np.nanmean(np.reshape(blk_rad, (blk_rad.shape[0]//4, 4, blk_rad.shape[1]//4,4)), axis=(1,3))
            else:
                fnl_blk_rad = blk_rad


            select_rad = np.nan_to_num(fnl_blk_rad[idx_geometry])
            fnl_idx = np.where((select_rad>0)&(select_rad<1000))[0]

            fnl_lat = select_lat[fnl_idx] * -1
            fnl_lon = select_lon[fnl_idx]
            fnl_rad = select_rad[fnl_idx]

            try:
                rad_sum, binedges, bin_numbers = binned_statistic_dd((fnl_lat, fnl_lon), fnl_rad, bins=[LAT_EDGES, LON_EDGES], statistic='sum')
                rad_cnt, binedges, bin_numbers = binned_statistic_dd((fnl_lat, fnl_lon), fnl_rad, bins=[LAT_EDGES, LON_EDGES], statistic='count')

                orbit_radiance_sum[:, :, iband] += rad_sum
                orbit_radiance_num[:, :, iband] += rad_cnt
            except ValueError:
                continue

    # =============================================================================
    # 3. Save results
    # =============================================================================
    orbit_radiance_num = np.array(orbit_radiance_num, dtype='int16')

    coords_lats = np.linspace(90-SPATIAL_RESOLUTION/2, -90+SPATIAL_RESOLUTION/2, NUM_LATS)
    coords_lons = np.linspace(-180+SPATIAL_RESOLUTION/2, 180-SPATIAL_RESOLUTION/2, NUM_LONS)

    xr_rad_sum = xr.DataArray(orbit_radiance_sum, coords=[('latitude', coords_lats), ('longitude', coords_lons), ('misr_channel', range(4))])
    xr_rad_num = xr.DataArray(orbit_radiance_num, coords=[('latitude', coords_lats), ('longitude', coords_lons), ('misr_channel', range(4))])
    xr_rad_sum.encoding['_FillValue'] = 0
    xr_rad_num.encoding['_FillValue'] = 0
    xr_rad_sum.name = 'MISR spec rad sum'
    xr_rad_num.name = 'MISR spec rad num'
    xr_rad_sum.to_netcdf(orbit_nc_out, 'a')
    xr_rad_num.to_netcdf(orbit_nc_out, 'a')
    return orbit_nc_out


if __name__ == "__main__":
    bf_file = sys.argv[1]
    main_bf_MISR(bf_file, '')





