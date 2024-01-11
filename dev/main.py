from typing import Any, Optional
import os
from io import BytesIO
import time
import tempfile
from pathlib import Path
from tqdm import tqdm

from pydantic import Field
import rioxarray as rio
import xarray as xr
import geopandas as gpd
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
# as we add some more functions only needed for the hyras processing, we will inherit 
# from processor and add the functions there
class HyrasProcessor(Processor):
    catchments: str
    catchment_name: str = ''

    def model_post_init(self, __context: Any) -> None:
        # we add the filename of the catchment geopackage to the processor logfiles 
        # as a first level prefix. That way, we can process differenct catchments simultaneously
        if self.catchment_name is None or self.catchment_name == '':
            self.catchment_name = Path(self.catchments).stem

        # change the log file names
        self.progress_log = f'{self.catchment_name}/{self.progress_log}'
        self.finished_log = f'{self.catchment_name}/{self.finished_log}'
        self.errored_log = f'{self.catchment_name}/{self.errored_log}'
        
        return super().model_post_init(__context)
    

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
    processor = HyrasProcessor()

    # The HyrasProcessor extends the Processor class with a catchments property, which is the path
    # to the catchments geopackage. We can use this to clip the data to the catchments.
    # Load the layer here, before we jump into the loop.

    # the file is in the target at the specified current location
    blob = processor.target.blob(processor.catchments)

    # load the file into a memory buffer
    buffer = BytesIO()
    blob.download_to_file(buffer)
    buffer.seek(0)

    # load using geopandas
    catchments: gpd.GeoDataFrame = gpd.read_file(buffer)
    catchments.to_crs(3034, inplace=True)

    # set the current iteration to 0
    iteration = 0
    start = time.time()

    # define a callback to handle excetions during the call
    errors = []
    def error_callback(e: Exception, it: int, fname: str):
        raise e
        msg = f"[{fname}],{it},{e.__class__.__name__}],{str(e)}"
        print(msg)
        errors.append(msg)
        return None

    # loop until the maximum number of iterations is reached
    while iteration != max_iterations:
        # check the timeout
        if timeout is not None and time.time() - start > timeout:
            # we have timed out, break the loop
            break

        # increase the iteration counter
        iteration += 1        
        
        # get the next file to process, and write it to a temporary file
        with tempfile.NamedTemporaryFile(suffix='.nc') as tmp:
            # get the next file name to process - use the context manager to retrieve the filename
            # as that will make Processor to track progress in logfiles, shared across all workers
            with processor.unprocessed_file(prefix=prefix) as file_name:
                # if the file_name is None, we have processed all files for this prefix
                if file_name is None:
                    break
                print(f'Downloading {file_name}...', end='', flush=True)

                # otherwise, download to the temporary file
                processor.download(file_name, target=tmp)
                print('done.', flush=True)

                # open the temporary file as an xarray dataaset - this is special hyras handling
                da = xr.open_dataset(tmp.name, decode_coords=True, mask_and_scale=True)[variable]

                # for hyras we need to set the spatial reference system and the coordinates manually
                # because the provider does somehow define the stuff differently, and GIS applications
                # cannot open hyras properly
                da.rio.set_spatial_dims(x_dim='x', y_dim='y', inplace=True)
                da.rio.write_crs('epsg:3034', inplace=True)

                # now turn back into a dataset
                ds = xr.Dataset({variable: da})
                print(ds)

                # make the clip for all EZG, we can chunk this by hand using use_oid here
                clips = zv.clip_features_from_dataset(ds, catchments, oid='oid', n_jobs=None)
                return clips
            
                # now zip the clips to the catchment id and upload the clip at the specifiec location
                for catchment_id, clip in tqdm(zip(catchments.id.tolist(), clips), total=len(clips)):
                    if clip is not None:
                        # create the path for the output file
                        output_path = Path(processor.catchment_name) / catchment_id / Path(file_name).name

                        # write the clip to a temporary file
                        with tempfile.NamedTemporaryFile(suffix='.nc') as tmp:
                            clip.to_netcdf(tmp.name)

                            # upload the file
                            processor.upload(str(output_path), tmp.name)
                    else:
                        output_path = Path(processor.catchment_name) / catchment_id / f"{Path(file_name).stem}.error"
                        buf = BytesIO()
                        buf.write(f"Error during processing of {file_name}.".encode('utf-8'))
                        buf.seek(0)
                        processor.upload(str(output_path), buf)
    # while loop finished
    if len(errors) > 0:
        print('\n'.join(errors))
        buf = BytesIO()
        buf.write('\n'.join(errors).encode('utf-8'))
        buf.seek(0)
        processor.upload(str(Path(processor.catchment_name) / 'errors.csv'), buf)
    print('done.')
    print(f"Took {round(time.time() - start)} seconds.")


if __name__ == '__main__':
    import click

    @click.command()
    @click.option('--prefix', default=None, help='The prefix of the files to process.')
    @click.option('--max_iterations', default=None, type=int, help='The maximum number of iterations.')
    def cli(prefix, max_iterations):
        main(prefix, max_iterations)
