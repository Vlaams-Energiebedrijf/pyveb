import pandas as pd
import os
import logging
import sys
import pyodbc
from time import time
from io import BytesIO
import boto3
import random
from time import sleep

"""
    https://github.com/mkleehammer/pyodbc/wiki/Data-Types

"""

class lynxClient():

    CONN_RETRIES = 3
    BACKOFF_IN_SECONDS = 5

    def __init__(self):
        """
            At runtime, environment variables are injected via entrypoint.sh.
            For local development, we fetch environment variables from local enviroment. Make sure they are set up. 
            Lynx only has 1 environment, hence no need to pass ENV
        """
        try: 
            server_raw = os.environ['LYNX_SERVER']
            port = os.environ['LYNX_PORT']
            database = os.environ['LYNX_DATABASE']
            username = os.environ['LYNX_USERNAME']
            password = os.environ['LYNX_PASSWORD']
            server = f'{server_raw},{port}'
            logging.info("Fetched lynx credentials from environment variables")
        except Exception as e:
            logging.error("Issue fetching lynx credentials from environment variables. Exiting...")
            logging.error(e)
            sys.exit(1)

        # connect to lynx SQL server 
        try: 
            connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password
            self.conn = self._connect(connection_string)
            logging.info("Succesfully set up connection with Lynx SQL Server")
        except Exception as e:
            logging.error("Unable to establish connection with Lynx SQL Server. Exiting")
            logging.error(e)
            sys.exit(1)
        return

    def _connect(self,connection_string:str):
        f"""
            Connect to Lynx via pyodbc. Automatically retries {self.CONN_RETRIES} times with exponential backoff of {self.BACKOFF_IN_SECONDS}. 
            Returns Connection or runtime error
        """
        # https://github.com/mkleehammer/pyodbc/wiki/Unicode
        # conn.setdecoding(pyodbc.SQL_CHAR, encoding='latin1', to=str)
        # conn.setencoding(str, encoding='latin1')
        nbr_of_retries = 0
        while True:
            try:
                return pyodbc.connect(connection_string)
            except pyodbc.Error as ex:
                if nbr_of_retries == int(self.CONN_RETRIES)-1:
                    raise RuntimeError(f'Not able to establish connection with Lynx server after {self.CONN_RETRIES}') from ex
                sleep_duration = (int(self.BACKOFF_IN_SECONDS) * 2 ** nbr_of_retries + random.uniform(0, 1))
                sleep(sleep_duration)
                nbr_of_retries += 1
                try:
                    sqlstate = ex.args[1]
                    sqlstate = sqlstate.split(".")
                    logging.warning('Issue connecting to Lynx server, trying again')
                    logging.warning(sqlstate[-3])
                except Exception:
                    logging.warning('Issue connecting to Lynx server and error response cannot be parsed. Trying again')
    
    def query_to_list(self, query:str):
        """
            fetchall() where all rows will be stored in memory
            returns 
                data list
                columns list
                dtypes list
        """
        cursor = self.conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        # https://stackoverflow.com/questions/5504340/python-mysqldb-connection-close-vs-cursor-close
        columns = [column[0] for column in cursor.description]
        dtypes = [column[1] for column in cursor.description]
        cursor.close()
        return rows, columns, dtypes

    def query_fetch_single_value(self, query: str) -> str:
        """
            grab a scalar, eg select max(x) from y
        """
        cursor = self.conn.cursor()
        cursor.execute(query)
        val = cursor.fetchval()
        cursor.close()
        return val

    def query_to_df(self, query:str) -> pd.DataFrame:
        """
            fetchall() where all rows will be stored in memory
            returns pandas dataframe
        """
        cursor = self.conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [column[0] for column in cursor.description]
        cursor.close()
        df = pd.DataFrame.from_records(rows, columns=columns)
        return df

    def get_max_cursors(self) -> str:
        cursor = self.conn.cursor()
        nbr_cursors = cursor.getinfo(pyodbc.SQL_MAX_CONCURRENT_ACTIVITIES)
        return nbr_cursors

    def cursor(self):
        cursor = self.conn.cursor()
        return cursor


    def stream_to_s3_parquet(self, query:str, batch_size:int, s3_bucket:str, s3_prefix:str, s3_filename:str) -> None:
        """
            Streams the results of a sql query to parquet files on s3 with 'batch_size' nbr of rows per file. 
            Output files have the following key:
                {s3_bucket}{s3_prefix}{timestamp}_{filename}.parquet        
        """
        for x in self._stream_results(query, batch_size):
            col_names = [column[0] for column in x[0].cursor_description]
            df = pd.DataFrame.from_records(x, columns=col_names)
            self._df_to_parquet_s3(df, s3_bucket, s3_prefix, s3_filename)
        return

                
    def _stream_results(self, query:str, batch_size: int):
        cursor = self.conn.cursor()
        iter = cursor.execute(query)
        while True:
            rows = iter.fetchmany(batch_size)
            if not rows:
                break
            yield rows
            # for row in rows:
            #     yield row

    def _df_to_parquet_s3(self, df:pd.DataFrame, s3_bucket: str, s3_prefix: str, file_name:str):
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, index=False, allow_truncated_timestamps=True)
        s3 = boto3.resource('s3')
        timestamp = round(time(), 4)
        s3_key = f"{s3_prefix}{timestamp}_{file_name}.parquet"
        s3.Object(s3_bucket, s3_key).put(Body=parquet_buffer.getvalue())
        logging.info(f'Stored {s3_key} on s3 {s3_bucket}')
        del df
        return
    




        