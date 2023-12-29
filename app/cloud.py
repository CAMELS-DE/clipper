from typing import Union, List, Optional
import json
from functools import cached_property
from contextlib import contextmanager

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
    gsa: Union[str, dict] = Field(repr=False, frozen=True, alias='google_service_account')
    source_bucket: str
    target_bucket: str
    progress_log: str = 'progress.log'
    finished_log: str = 'finished.log'
    errored_log: str = 'errored.log'

    @computed_field
    @cached_property
    def client(self) -> Client:
        # decode the google service account string
        service_dict = json.loads(self.gsa) if isinstance(self.gsa, str) else self.gsa

        # build credentials from the service account
        credentials = service_account.Credentials.from_service_account_info(info=service_dict)

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


# define a function to process the next, single file
@contextmanager
def next_unprocessed_file(prefix: Optional[str] = None, **kwargs):
    # init a processor object
    processor = Processor(**kwargs)

    # get the next file to process
    file_name = processor.next_file(prefix=prefix)

    # add the file name to the progress list
    processor.progress_list + file_name

    # flag for errored files
    did_error = False

    # yield the file name to the caller
    try:
        yield file_name
    except Exception:
        # if an error occured, add the file name to the errored list
        processor.errored_list + file_name

        # set the flag to True
        did_error = True
    finally:
        # remove the file name from the progress list
        processor.progress_list - file_name

        # add the file name to the finished list if no error occured
        if not did_error:
            processor.finished_list + file_name

