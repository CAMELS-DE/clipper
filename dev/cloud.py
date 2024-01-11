from typing import Union, List, Optional
from typing_extensions import Literal
import json
from io import BytesIO, IOBase
from functools import cached_property
from contextlib import contextmanager
from pathlib import Path

from dotenv import load_dotenv
from pydantic import Field, computed_field, BaseModel
from pydantic_settings import BaseSettings
from google.cloud.storage import Client, Bucket
from google.oauth2 import service_account

load_dotenv()


class LogHandler(BaseModel):
    processor: 'Processor'
    log_file: str

    @property
    def _content(self):
        if self.processor.target.blob(self.log_file).exists():
            return self.processor.target.blob(self.log_file).download_as_string().decode('utf-8').split('\n')
        else:
            return []
        
    def add(self, item: str):
        new_content = self._content + [item]
        self.processor.target.blob(self.log_file).upload_from_string('\n'.join(new_content))
    
    def remove(self, item: str):
        new_content = [line for line in self._content if line != item]
        self.processor.target.blob(self.log_file).upload_from_string('\n'.join(new_content))
    
    def tolist(self) -> List[str]:
        return self._content
    
    def __contains__(self, item: str) -> bool:
        return item in self._content
    
    def __iter__(self):
        return iter(self._content)
    
    def __len__(self) -> int:
        return len(self._content)
    
    def __add__(self, other: str):
        self.add(other)

    def __sub__(self, other: str):
        self.remove(other)


class Processor(BaseSettings):
    project_id: str = 'camels-de'
    gkey: str = Field(repr=False, frozen=True, alias='google_application_credentials')
    source_bucket: str
    target_bucket: str
    progress_log: str = 'progress.log'
    finished_log: str = 'finished.log'
    errored_log: str = 'errored.log'

    @computed_field
    @cached_property
    def client(self) -> Client:
        # build credentials from the service account
        credentials = service_account.Credentials.from_service_account_file(self.gkey)

        # initialize the client
        return Client(self. project_id, credentials=credentials)

    @computed_field
    @property
    def source(self) -> Bucket:
        return self.client.bucket(self.source_bucket)
    
    @computed_field
    @property
    def target(self) -> Bucket:
        return self.client.bucket(self.target_bucket)

    @property
    def progress_list(self) -> LogHandler:
        return LogHandler(processor=self, log_file=self.progress_log)
    
    @property
    def finished_list(self) -> LogHandler:
        return LogHandler(processor=self, log_file=self.finished_log)
    
    @property
    def errored_list(self) -> LogHandler:
        return LogHandler(processor=self, log_file=self.errored_log)
    
    def next_file(self, prefix: Optional[str] = None) -> str:
        # loop through all files of given prefix
        for blob in self.source.list_blobs(prefix=prefix):
            # check if the file is currently being processed
            if blob.name in self.progress_list:
                continue

            # check if the file has already been processed
            if blob.name in self.finished_list:
                continue

            # check if the file has already errored
            if blob.name in self.errored_list:
                continue

            # otherwie return the file name and break the loop
            return blob.name

    def download(self, blob_name: str, target: Optional[IOBase] = None) -> IOBase:
        # get the blob object
        blob = self.source.blob(blob_name)

        # check if the blob exists
        if not blob.exists():
            raise FileNotFoundError(f'Blob {blob_name} does not exist in bucket {self.source.path}.')
        
        # if no target is given, create a new BytesIO object
        if target is None:
            target = BytesIO()
        
        # download the blob as bytes or text, depending on the target type
        blob.download_to_file(target)

        return target
    
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

    @contextmanager
    def unprocessed_file(self, file_name: Optional[str] = None, prefix: Optional[str] = None):
        # get the next file to process
        if file_name is None:
            file_name = self.next_file(prefix=prefix)

        # add the file name to the progress list
        self.progress_list + file_name

        # flag for errored files
        did_error = False

        # yield the file name to the caller
        try:
            yield file_name
        except Exception as e:
            # if an error occured, add the file name to the errored list
            self.errored_list + file_name

            # set the flag to True
            did_error = True

            raise e
        finally:
            # remove the file name from the progress list
            self.progress_list - file_name

            # add the file name to the finished list if no error occured
            if not did_error:
                self.finished_list + file_name

