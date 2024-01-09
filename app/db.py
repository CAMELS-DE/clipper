from typing import Any, Optional, List, Generator
from pathlib import Path
import tempfile
import io
from contextlib import contextmanager

from tqdm import tqdm
import duckdb
import geopandas as gpd
import pandas as pd
import rioxarray as rio
import xarray as xr

from .cloud import Processor

LOAD_SQL = """create or replace table tmp as 
select time, lon, lat, {variable} from '{fname}' 
where {variable} is not null 
and lon between (select st_xmin(geom) from catchments) and (select st_xmax(geom) from catchments)
and lat between (select st_ymin(geom) from catchments) and (select st_ymax(geom) from catchments);
"""


class HyrasDBProcessor(Processor):
    catchments: str
    catchment_name: str = ''

    def model_post_init(self, __context: Any) -> None:
        # we add the filename of the catchment geopackage to the processor logfiles 
        # as a first level prefix. That way, we can process differenct catchments simultaneously
        if self.catchment_name is None or self.catchment_name == '':
            self.catchment_name = Path(self.catchments).stem

        # change the log file names
        self.progress_log = f'db/{self.catchment_name}/{self.progress_log}'
        self.finished_log = f'db/{self.catchment_name}/{self.finished_log}'
        self.errored_log = f'db/{self.catchment_name}/{self.errored_log}'
        
        return super().model_post_init(__context)
    
    def next_year(self) -> List[str]:
        for year in range(1950, 2024):
            # get all files for the currently tested year
            blobs = list(self.source.list_blobs(match_glob=f"*/*_hyras_*{year}*.nc"))

            # if there are none, continue to next year
            if len(blobs) == 0:
                continue
            
            # check if any of the files is currently being processed
            progress = self.progress_list._content
            if any([blob in progress for blob in blobs]):
                continue

            # check if any of the files has already been processed
            finished = self.finished_list._content
            if any([blob in finished for blob in blobs]):
                continue

            # check if any of the files has already errored
            errored = self.errored_list._content
            if any([blob in errored for blob in blobs]):
                continue

            # if we are still here, yield the list of blobs
            yield [blob.name for blob in blobs] 

class HyrasDB:
    def __init__(self, processor: Optional[HyrasDBProcessor] = None) -> None:
        if processor is None:
            processor = HyrasDBProcessor()
        self.proc = processor

        self._catchments = None

    def _load_catchments(self) -> None:
        # the file is in the target at the specified current location
        blob = self.proc.target.blob(self.proc.catchments)

        # load the file into a memory buffer
        buffer = io.BytesIO()
        blob.download_to_file(buffer)
        buffer.seek(0)

        # load using geopandas
        self._catchments: gpd.GeoDataFrame = gpd.read_file(buffer)
    
    @property
    def catchments(self) -> gpd.GeoDataFrame:
        if self._catchments is None:
            self._load_catchments()
        return self._catchments
    