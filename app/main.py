from typing import Optional
import os
import time
import tempfile

import xarray as xr
import zonal_variograms.main as zv

from .cloud import Processor

# dictionary, mapping the variable path prefixes to the variable name(s)
# in the HYRAS netCDF files
VARIABLES = {
    'Humidity': 'hurs', 
    'Precipitation': 'pr', 
    'RadiationGlobal': 'rsds', 
    'TemperatureMax': 'tasmax', 
    'TemperatureMean': 'tas', 
    'TemperatureMin': 'tasmin'
}

# instantiate a Processor object. That is populated with settings from an .env file, 
# the envrionment variables, holds some defaults or can be overwritten by kwargs.
# The Processor object is used to access the cloud storage buckets, and track the 
# currently processed, finished or errored files, to orchestrate the processing
# between different containers, either in the google cloud or locally.
# processor = Processor()

def main(prefix: str = None, max_iterations: Optional[int] = None, timeout: Optional[int] = None):
    # check if there is an environment variable for the prefix
    if prefix is None:
        prefix = os.getenv('PREFIX')
    
    # if prefix is None, recursively call main() with the first prefix in the VARIABLES dict
    if prefix is None:
        prefixes = list(VARIABLES.keys())
        for prefix in prefixes:
            main(prefix, int(max_iterations / len(prefixes)), int(timeout / len(prefixes)))
    
    # now there is a prefix for sure, so get the variable name from the VARIABLES dict
    variable = VARIABLES[prefix]

    # check if there is an environment variable for the max_iterations
    if max_iterations is None:
        max_iterations = os.getenv('MAX_ITERATIONS')
    if max_iterations is not None:
        max_iterations = int(max_iterations)

    # instaintiate a new Processor object
    processor = Processor()

    # set the current iteration to 0
    iteration = 0
    start = time.time()

    # loop until the maximum number of iterations is reached
    while iteration != max_iterations:
        # check the timeout
        if timeout is not None and time.time() - start > timeout:
            # we have timed out, break the loop
            break
        
        # get the next file to process, and write it to a temporary file
        with tempfile.NamedTemporaryFile(suffix='.nc') as tmp:
            # get the next file name to process - use the context manager to retrieve the filename
            # as that will make Processor to track progress in logfiles, shared across all workers
            with processor.next_unprocessed_file(prefix=prefix) as file_name:
                # if the file_name is None, we have processed all files for this prefix
                if file_name is None:
                    break

                # otherwise, download to the temporary file
                processor.download(file_name, target=tmp)

                # open the temporary file as an xarray dataaset - this is special hyras handling
                da = xr.open_dataset(tmp.name, decode_coords=True, mask_and_scale=True)[variable]

                # for hyras we need to set the spatial reference system and the coordinates manually
                # because the provider does somehow define the stuff differently, and GIS applications
                # cannot open hyras properly
                da.rio.set_spatial_dims(x_dim='x', y_dim='y', inplace=True)
                da.rio.write_crs('epsg:3034', inplace=True)

                # now turn back into a dataset
                ds = xr.Dataset({variable: da})

                # make the clip for all EZG, we can chunk this by hand using use_oid here
                raise NotImplementedError('Not sure how to supply the EZG here') 


if __name__ == '__main__':
    import click

    @click.command()
    @click.option('--prefix', default=None, help='The prefix of the files to process.')
    @click.option('--max_iterations', default=None, type=int, help='The maximum number of iterations.')
    def cli(prefix, max_iterations):
        main(prefix, max_iterations)
