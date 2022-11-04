import pandas as pd
import requests
import logging
import time, sys, os
from functools import wraps
import shutil
import contextlib
from typing import Tuple
import xlrd

# retry decorator with adjustable nbr of retries and retry attempt passed to wrapped function as keyword argument 'attempt'
def retry(retries: int, **fkwargs):
    def _decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(retries):
                try:
                    return func(*args, attempt = attempt, **kwargs)
                except Exception as e:
                    logging.warning(f"Error: {fkwargs['error']}, Attempt: {attempt}, Python Error: {e}")
                    time.sleep(attempt*attempt)
            logging.error("3 attempts to download bpost xls to df failed. Exiting...")
            sys.exit(1)
        return wrapper
    return _decorator

class basicClient():

    def __init__(self) -> None:
        None

    @retry(retries = 3, error = "Error downloading xls file from bpost to df")
    def download_xls_to_df(self, url: str, file_name: str, **kwargs) -> Tuple[str, pd.DataFrame]:
        """ 
            ARGUMENTS
                url: url of the xls file, eg. www.example.com/file.xls
                file_name: nam for storing file, eg. local_file

            RETURNS
                Tuple[local_file, df] where local_file = ./tmp_data/timestamp_file_name.xls and 
                df is the first worksheet read into a dataframe
        
        """
        logging.info(f"Attempt number: {kwargs['attempt']}")
        download_path = "./tmp_data/"
        with contextlib.suppress(Exception):
            shutil.rmtree(download_path)
        timestamp = round(time.time(), 4)
        if not os.path.exists(download_path):
            os.mkdir(download_path)
        temp_location = f"{download_path}{timestamp}_{file_name}.xls"
        response = requests.get(url)
        with open(temp_location, 'wb') as f:
            f.write(response.content)
        workbook = xlrd.open_workbook(temp_location, ignore_workbook_corruption=True)
        df = pd.read_excel(workbook)
        return temp_location, df

