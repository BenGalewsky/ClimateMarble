"""
Created on Wed Sep 11 10:41:35 2019

@author: yizhe

Functions support generating Climate Marble from basicfusion data.

Updates:
Oct 10 2019 - add function <fetch_bf_files_s3(iyr, imon)>
"""

import h5py
import numpy as np
import os
import datetime
import julian


###
def save_data_hdf5(filename, data_path, data):
    """save data into a hdf5 file
    
    Create the hdf5 file if the file does not exist.
    
    Arguments:
        filename {string} -- [output hdf5 file path]
        data_path {string} -- [data path in the hdf5 file]
        data {array} -- [data that needs to be saved]
    """
    with h5py.File(filename, 'a') as h5f:
        h5f.create_dataset(data_path, data=data, compression='gzip')
    return


###
def fetch_bf_files_condo(iyr, imon, iday):
    """fetch basic fusion files for a given ymd.
    
    given the basic fusion folder is "/terradata/basicfusion", return the paths of basic fusion files for a given time.
    
    Arguments:
        iyr {int} -- year
        imon {int} -- month
        iday {int} -- day
    """
    # BF_folder = "/terradata/basicfusion"
    BF_folder = "/projects/rcaas/terrafusion/yizhe"
    BF_folder_sub = os.path.join(BF_folder, '{}.{}'.format(iyr, str(imon).zfill(2)))
    string_ymd = '{}{}{}'.format(iyr, str(imon).zfill(2), str(iday).zfill(2))
    BF_files = np.array([os.path.join(BF_folder_sub, ifile) for ifile in os.listdir(BF_folder_sub) if string_ymd in ifile])
    return BF_files


###
def latslons_to_idxs(lats, lons, num):
    """
    An updated key function that determines the lat/lon indexes for a given resolution map based on the input lats and lons.
    
    Args:
        lats (TYPE): Description
        lons (TYPE): Description
        num (float32): number of indexes within 1 degree
    
    Returns:
        lat/lon index arrays: Description
    """

    lats_int = lats.astype('int32')
    lons_int = lons.astype('int32')
    lats_dec = lats - lats_int
    lons_dec = lons - lons_int

    # Latitude
    lats_idx = (90-lats_int) * num - (lats_dec*num).astype('int32')
    lats_idx[lats>=0] -= 1

    # Longitude
    lons_idx = (180+lons_int) * num + (lons_dec*num).astype('int32')
    lons_idx[lons<0] -= 1
    np.place(lons_idx, lons_idx==360*num, 0)
    
    # Add on Oct.15, 2018
    lats_idx = lats_idx.astype('int32')
    lons_idx = lons_idx.astype('int32')
    return lats_idx, lons_idx


###
def ymd_to_doy(iyr, imon, iday):
    """convert year, month, day to day-of-year.
    
    [description]
    
    Arguments:
        iyr {int} -- year
        imon {int} -- month
        iday {int} -- day
    
    Returns:
        doy -- day of year
    """
    doy = (datetime.datetime(iyr, imon, iday)-datetime.datetime(iyr, 1, 1)).days+1
    return doy


def get_descending(h5f, instrument):
    """
    Given a BF instance, return the starting and ending time of the descending node.
    Note that both times are stick to the MODIS 5-min granules.

    For CERES, return the julian time of the first/last descending MODIS granule in the bf file;
    For MODIS, return the first/last descending MODIS granule in the bf file;
    For MISR, return the first/last block of the descending MODIS granule in the bf file.
    
    Args:
        h5f (hdf5 instance): instance of a basic fusion file
        instrument (str)   : instrument from 'CERES', 'MISR', and 'MODIS'
    
    Returns:
        out_array (array): return MODIS granules, CERES start/end julian times, 
                                     or MISR start/end blocks depending on the instrument
    """

    MODIS_granules = [item[0] for item in h5f['MODIS'].items()]
    if len(MODIS_granules) == 0:
        print(">> IOError( no available MODIS granule in orbit {} )".format(bf_file))
        out_array = np.array([0, 0])

    else:
        descending_granules = []
        for igranule in MODIS_granules:     
            try:
                lats = h5f['MODIS/{}/_1KM/Geolocation/Latitude'.format(igranule)][:]
                lons = h5f['MODIS/{}/_1KM/Geolocation/Longitude'.format(igranule)][:]
            except KeyError:
                print(">> KeyError( cannot access lat/lon in {} )".format(igranule))
                continue

            # Process descending granules only (neutral granules are omitted)
            # May be improved by using a better criteria (?)
            cnt = 0
            for i in range(1, len(lats)):
                if all(lats[i]<lats[i-1]) == False:
                    cnt += 1
            if cnt >= 1000:
                # print(">> CriteriaError( this is not a descending granule {} )".format(igranule))
                continue
            else:
                descending_granules.append(igranule)

        descending_julian_bound = np.array([granuletime_to_jd(descending_granules[0]), granuletime_to_jd(descending_granules[-1], offset_mins=5)])

        if instrument.startswith('MODIS'):
            out_array = descending_granules
        elif instrument.startswith('CERES'):
            out_array = descending_julian_bound
        elif instrument.startswith('MISR'):
            # get MISR camera
            icam = instrument.split('.')[1]
            # get MISR BlockCenterTime
            try:
                bct = h5f['MISR/AN/BlockCenterTime'][:]
            except:
                print(">> IOError( cannot access MISR data )")
                out_array = np.array([0, 0])
            else:
                misr_block_julian = []
                for ibct in bct: 
                    yr, mon, day = str(ibct).split('-') # ibct: "b'2012-06-03T07:34:23.000532Z"
                    yr = int(yr[2:])                    # 2012
                    if yr == 0:
                        misr_block_julian.append(0)
                    else:
                        hr, mn, sec_decimal = day[3:].split(':') # "07:34:23.000532Z'"
                        sec = int(float(sec_decimal[:-2]))       # 23.000532
                        millisec = int(1000*(float(sec_decimal[:-2]) - sec)) # .000532 * 1000

                        dt = datetime.datetime(yr, int(mon), int(day[:2]), int(hr), int(mn), sec, millisec)
                        misr_block_julian.append(julian.to_jd(dt, fmt='jd'))
                
                misr_block_julian = np.array(misr_block_julian)
                misr_descending_blocks = np.where((misr_block_julian>=descending_julian_bound[0])&(misr_block_julian<=descending_julian_bound[1]))[0]

                out_array = misr_descending_blocks
    return out_array


def granuletime_to_jd(mod_granule_string, offset_mins=0):
    """
    Convert a MODIS granule string (e.g., 'granule_2012155_0700') to julian date (e.g., 2456081.7916666665)
    
    Args:
        mod_granule_string (str): MODIS granule string
        offset_mins (int, optional): offset minutes

    Returns:
        jd (int): julian date
    """
    yr = int(mod_granule_string.split('_')[1][:4])
    doy = int(mod_granule_string.split('_')[1][-3:])
    hr = int(mod_granule_string.split('_')[2][:2])
    mn = int(mod_granule_string.split('_')[2][-2:])

    dt = datetime.datetime(yr, 1, 1, hr, mn) + datetime.timedelta(days=doy-1, minutes=offset_mins)
    jd = julian.to_jd(dt, fmt='jd')    
    return jd


# if __name__ == '__main__':
#     fetch_bf_files(2000, 2, 25)