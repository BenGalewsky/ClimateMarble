"""
Author@Yizhe

Created on Oct 15, 2019

Sort CERES radiances (from BF product) to the specified lat/lon grids and then store the gridded daily data.

Comparing to the previous code that works on the SSF dataset, this version is simplier:

(1) no need for finding granules
(2) no need for converting lat/lon from CERES convention to MISR/MODIS convention
"""

import numpy as np
import os
import sys
import h5pyd as h5py
import s3fs
import xarray as xr
from Climate_Marble_common_functions import latslons_to_idxs, get_descending



def main_bf_CERES(h5f, output_folder, SPATIAL_RESOLUTION=0.5, VZA_MAX=18, MODE='ct'):
    """
    (This script is adapted for running on AWS cloud)
    
    The CERES gridded file for each orbit will be generated directly from the basic fusion data files.

    Args:
        bf_file (str): BF file path
        output_folder (str): folder storing the gridded results
        SPATIAL_RESOLUTION (float, optional): spatial resolution of the grid (in degree)
        VZA_MAX (int, optional): maximum viewing zenith angle considered (in degree)
        MODE (str, optional): category of CERES scan mode ('ct', 'all')
    
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
    
    # 
    orbit_sw_sum  = np.zeros((NUM_LATS, NUM_LONS))
    orbit_sw_num  = np.zeros((NUM_LATS, NUM_LONS), dtype='int16')
    orbit_lw_sum  = np.zeros((NUM_LATS, NUM_LONS))
    orbit_nc_out = os.path.join(output_folder, output_nc_name)


    # =============================================================================
    # 2. Main processing
    #    Loop through each CERES granule and sort radiances into the corresponding lat/lon bins
    #    When encounters an asceding granule, script will move to the next granule
    # =============================================================================

    # USE MODIS granules to match first and last time of the descending node
    t0, t1 = get_descending(h5f, 'CERES')
    if t0 == 0:
        print(">> IOError, no available MODIS granule in orbit {}".format(h5f.fid))
        return

    # GET CERES granules
    CERES_granules = [item[0] for item in h5f['CERES'].items()]
    if len(CERES_granules) == 0:
        print(">> IOError, no available CERES granule in orbit {}".format(h5f.fid))
        return

    # LOOP through each CERES granule
    for igranule in CERES_granules:
        # USE time of FOV to select CERES samples
        ssf_time = h5f['CERES/{}/FM1/Time_and_Position/Time_of_observation'.format(igranule)][:]
        idx_0 = np.where((ssf_time>=t0)&(ssf_time<=t1))[0]
        if len(idx_0) == 0:
            continue
        else:
            # USE sw_flx, vza, sza, and mode_flg to select required CERES samples   
            # (edited on Oct. 15, 2019)
            # these citeria may not be enough, as there are extremely large values in LW radiances in all modes (but not in cross-track mode).
            # as a result, for all-modes, two additional criteria '0<lw<1000' were added.
            ssf_sw   = h5f['CERES/{}/FM1/Radiances/SW_Radiance'.format(igranule)][:]
            ssf_mode = h5f['CERES/{}/FM1/Radiances/Radiance_Mode_Flags'.format(igranule)][:]
            ssf_lw   = h5f['CERES/{}/FM1/Radiances/LW_Radiance'.format(igranule)][:]
            ssf_sza  = h5f['CERES/{}/FM1/Viewing_Angles/Solar_Zenith'.format(igranule)][:]
            ssf_vza  = h5f['CERES/{}/FM1/Viewing_Angles/Viewing_Zenith'.format(igranule)][:]

            if MODE == 'ct':
                idx_1 = np.where((ssf_sw>0)&(ssf_sw<1000)&(ssf_vza<VZA_MAX)&(ssf_sza<=89.0)&(ssf_mode==0))[0] # cross-track mode only
            else:
                idx_1 = np.where((ssf_sw>0)&(ssf_sw<1000)&(ssf_vza<VZA_MAX)&(ssf_sza<=89.0)&(ssf_lw<1000)&(ssf_lw>0))[0] # all modes (check longwave radiances as well (Arp. 23, 2019))

            ## INTERSECT idx_0 and idx_1, and use these indexes to select requried samples
            ## (edited on July 24, 2019)
            ## sol should be "TOA Incoming Solar Radiation" but mistakely used "CERES solar zenith at surface" in the processing.
            idx = np.intersect1d(idx_0, idx_1)
            lats = h5f['CERES/{}/FM1/Time_and_Position/Latitude'.format(igranule)][:][idx]
            lons = h5f['CERES/{}/FM1/Time_and_Position/Longitude'.format(igranule)][:][idx]
            sw = ssf_sw[idx]
            lw = ssf_lw[idx]

            # Calculate lat/lon indexes of all sample. 
            # (2018.05.29) Explicitly convert these indexes to integer
            lats_idx, lons_idx = latslons_to_idxs(lats, lons, NUM_POINTS)

            # BIN data
            for i, j, isw, ilw in zip(lats_idx, lons_idx, sw, lw):
                orbit_sw_sum[i, j] += isw
                orbit_lw_sum[i, j] += ilw
                orbit_sw_num[i, j] += 1

    # =============================================================================
    # 3. Save results
    # =============================================================================
    coords_lats = np.linspace(90-SPATIAL_RESOLUTION/2, -90+SPATIAL_RESOLUTION/2, NUM_LATS)
    coords_lons = np.linspace(-180+SPATIAL_RESOLUTION/2, 180-SPATIAL_RESOLUTION/2, NUM_LONS)

    xr_sw_sum = xr.DataArray(orbit_sw_sum, coords=[('latitude', coords_lats), ('longitude', coords_lons)])
    xr_lw_sum = xr.DataArray(orbit_lw_sum, coords=[('latitude', coords_lats), ('longitude', coords_lons)])
    xr_sw_num = xr.DataArray(orbit_sw_num, coords=[('latitude', coords_lats), ('longitude', coords_lons)])
    xr_sw_sum.encoding['_FillValue'] = 0
    xr_lw_sum.encoding['_FillValue'] = 0
    xr_sw_num.encoding['_FillValue'] = 0
    xr_sw_sum.name = 'CERES SW rad sum'
    xr_lw_sum.name = 'CERES LW rad sum'
    xr_sw_num.name = 'CERES SW rad num'
    xr_sw_sum.to_netcdf(orbit_nc_out, 'a')
    xr_lw_sum.to_netcdf(orbit_nc_out, 'a')
    xr_sw_num.to_netcdf(orbit_nc_out, 'a')
    return orbit_nc_out


if __name__ == "__main__":
    bf_file = sys.argv[1]
    main_bf_CERES(bf_file, '')





