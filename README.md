# ClimateMarble

Author @ Yizhe & Larry

Produce CERES, MISR, and MODIS gridded radiance and insolation based on Terra 
Basicfusion product on NASA s3. Results are stored at orbital-level and can be 
further processed to generate daily, monthly, and other temporal-average data 
for generating Climate Marbles.

This analysis has been modified to work with the NASA Data Carousel prototype.

## Usage
The script has a number of command line arguments to enable it to be used within
a batch workflow or as a standalone tool for working with a single file. You
also have the option of reading a file using s3fs or with HSDS.

If you  supply a `--hsds` command line argument, the script will attempt to read
data from the HSDS via h5pyd library. If you omit this parameter, the script
will read the file with h5py using the s3fs library.

To run the script as part of the Data Carousel, you provide the SQS work queue 
and omit the filename. In this case, the script opens up the queue and accepts
files to process via messages on the queue.

