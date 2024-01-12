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


def download_one(variable: str, start: int = 1951, end: int = 2024):
    processor = DuckProcessor(
        local_hyras_cache='./hyras_cache'
    )
    
    # go for each year
    for year in tqdm(range(start, end)):
        try:
            processor.download_hyras(variable=variable, year=year)
        except Exception as e:
            print(str(e), flush=True)
            continue



def download():
    # 'pr takes way longer than the others, so we start it many times
    variables = ('hurs', 'rsds', 'tas', 'tasmax', 'tasmin', 'pr', 'pr', 'pr', 'pr', 'pr', 'pr', 'pr')
    starts = (1951, 1951, 1951, 1951, 1951, 1951, 1961, 1971, 1981, 1991, 2001, 2011)
    ends =   (2024, 2024, 2024, 2024, 2024, 1961, 1971, 1981, 1991, 2001, 2011, 2024)

    # go multithreading for faster download
    with ThreadPoolExecutor() as executor:
        try:
            list(executor.map(download_one, variables, starts, ends, timeout=60*50))
        except TimeoutError:
            print("The full hyras dataset could not be downloaded within 50 Minutes")


if __name__ == '__main__':
    download()
