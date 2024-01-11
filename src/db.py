from typing import Any, Union, Optional
from typing_extensions import Literal
from pathlib import Path
from io import IOBase, BytesIO
from functools import cached_property

from dotenv import load_dotenv
from pydantic import Field, computed_field
from pydantic_settings import BaseSettings
from google.cloud.storage import Client, Bucket
from google.oauth2 import service_account
import geopandas as gpd
import httpx

VAR_MAP = {
    'pr': 'preciptation',
    'hurs': 'humidity',
    'rsds': 'radiation_global',
    'tas': 'air_temperature_mean',
    'tasmax': 'air_temperature_max',
    'tasmin': 'air_temperature_min'
}

load_dotenv()


class DuckProcessor(BaseSettings):
    project_id: str = 'camels-de'
    gkey: str = Field(default='', repr=False, frozen=True, alias='google_application_credentials')
    target_bucket: str
    notebook_bucket: str
    catchments_path: str
    result_prefix: str = Field(init_var=False, default='')
    base_url: str = "https://opendata.dwd.de/climate_environment/CDC/grids_germany/daily/hyras_de"

    @computed_field
    @cached_property
    def client(self) -> Client:
        # build credentials from the service account
        if self.gkey != '':
            credentials = service_account.Credentials.from_service_account_file(self.gkey)
        else:
            credentials = None

        # initialize the client
        return Client(self. project_id, credentials=credentials)

    def model_post_init(self, __context: Any) -> None:
        # derive the prefix on GCE from the name of the catchments file
        self.result_prefix = Path(self.catchments_path).stem
        return super().model_post_init(__context)

    @computed_field
    @property
    def target(self) -> Bucket:
        return self.client.bucket(self.target_bucket)
    
    @computed_field
    @property
    def notebooks(self) -> Bucket:
        return self.client.bucket(self.notebook_bucket)
    
    def load_catchments(self) -> gpd.GeoDataFrame:
        # the catchments_path is possibly a local path
        path = Path(self.catchments_path)
        if path.exists():
            return gpd.read_file(path)
    
        # get the catchments blob from the target bucket
        blob = self.target.blob(self.catchments_path)

        # load to buffer
        buffer = BytesIO()
        blob.download_to_file(buffer)
        buffer.seek(0)

        # return the GeoDataFrame
        return gpd.read_file(buffer)

    def upload(self, blob_name: str, source: Union[IOBase, str], if_exists: Union[Literal['raise'], Literal['ignore']] = 'raise') -> None:
        # get the blob object
        blob = self.target.blob(blob_name)

        # check if the blob already exists
        if blob.exists() and if_exists == 'raise':
            raise FileExistsError(f'Blob {blob_name} already exists in bucket {self.target.path}.')
        
        # otherwise upload the blob
        if isinstance(source, str):
            blob.upload_from_filename(source)
        else:
            blob.upload_from_file(source)

    def upload_notebook(self, notebook_name: str, source: Union[IOBase, str]) -> None:
        # get the blob object
        blob = self.notebooks.blob(notebook_name)

        # upload the blob
        if isinstance(source, str):
            blob.upload_from_filename(source)
        else:
            blob.upload_from_file(source)
        

    def download_hyras(self, variable: str, year: int, path: Optional[IOBase] = None) -> Union[IOBase, str]:
        # build the filename 
        filename = f"{variable}_hyras_{1 if variable in ('pr') else 5}_{year}_v5-0_de.nc"
        # build the specific url
        url = f"{self.base_url}/{VAR_MAP[variable]}/{filename}"

        # download
        response = httpx.get(url)
        if response.status_code != 200:
            raise FileNotFoundError(f'File {filename} not found. (Tested: {url})')

        # handle return value
        if path is None:
            buffer = BytesIO()
            buffer.write(response.content)
            buffer.seek(0)
            return buffer
        else:
            path.write(response.content)

    def exists(self, blob_name) -> bool:
        return self.target.blob(blob_name).exists()
    
    def processing_state(self, ):
        pass
