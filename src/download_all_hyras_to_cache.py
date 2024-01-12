"""
This file will download all hyras into the specified cache.
Currently the cache is hard-coded for building the container with all hyras data
already included.

"""
from dotenv import load_dotenv
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, TimeoutError

from db import DuckProcessor


load_dotenv()


def download_one(variable: str):
    processor = DuckProcessor(
        local_hyras_cache='./hyras_cache'
    )
    
    # go for each year
    for year in tqdm(range(1950, 2024)):
        try:
            processor.download_hyras(variable=variable, year=year)
        except Exception as e:
            print(str(e), flush=True)
            continue



def download():
    variables = ('hurs', 'rsds', 'pr', 'tas', 'tasmax', 'tasmin')

    # go multithreading for faster download
    with ThreadPoolExecutor() as executor:
        try:
            list(executor.map(download_one, variables, timeout=60*50))
        except TimeoutError:
            print("The full hyras dataset could not be downloaded within 50 Minutes")


if __name__ == '__main__':
    download()
