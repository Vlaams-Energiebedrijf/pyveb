import pandas as pd
import os
import logging
import sys
import pyodbc
from time import time
from io import BytesIO
import boto3
from time import sleep
from .custom_decorators import retry

class lynxClient():
    """
        Class which creates a connection to SQL server based on connection details in ENV variables.
        Connection and cursor objects are created on initialization with retry patterns. 

        The connection object allows querying and several methods are available which all have retry mechanism built-in. Each method 
        generates a cursor object which is closed after correct execution. Each method is automatically retried 3 times. 

        It is advised to call 'close_connection' if you no longer need the class instance. 
    """

    def __init__(self) -> str:
        self._connection_instance = None
        self._connection_instance = self._connect()
        logging.info('successfully created connection')

    def _create_conn_string(self) -> str:
        """
            Function to create a connection string for SQL server
            Environment variables are injected in the container environment via entrypoint.sh at runtime
            For local development, we fetch environment variables from local enviroment. Make sure they are set up. 
            Lynx only has 1 environment, hence no need to pass ENV.
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
        connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password
        return connection_string

    @retry(retries=3, error="Error creating connection to SQL server")
    def _connect(self, **kwargs) -> pyodbc.Connection:
        return pyodbc.connect(self._create_conn_string())

    @retry(retries=3, error="Error creating cursor")
    def _create_cursor(self, **kwargs) -> pyodbc.Cursor:
        return self._connection_instance.cursor()

    @retry(retries=3, error="Error executing query to list")
    def query_to_list(self, query:str, **kwargs) :
        """
            fetchall() where all rows will be stored in memory
            returns 
                data list
                columns list
                dtypes list
        """
        cursor = self._create_cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        # https://stackoverflow.com/questions/5504340/python-mysqldb-connection-close-vs-cursor-close
        columns = [column[0] for column in cursor.description]
        dtypes = [column[1] for column in cursor.description]
        cursor.close()
        return rows, columns, dtypes

    @retry(retries=3, error="Error executing query_fetch_single_value")
    def query_fetch_single_value(self, query: str, **kwargs) -> str:
        """
            grab a scalar, eg select max(x) from y
        """
        cursor = self._create_cursor()
        cursor.execute(query)
        val = cursor.fetchval()
        cursor.close()
        return str(val)

    @retry(retries=3, error="Error executing query_to_df")
    def query_to_df(self, query:str, **kwargs) -> pd.DataFrame:
        """
            fetchall() where all rows will be stored in memory
            returns pandas dataframe
        """
        cursor = self._create_cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [column[0] for column in cursor.description]
        cursor.close()
        df = pd.DataFrame.from_records(rows, columns=columns)
        logging.info(df)
        return df

    @retry(retries=3, error="Error executing get_max_cursors ")
    def _get_max_cursors(self, **kwargs) -> str:
        cursor = self._create_cursor()
        nbr_cursors = cursor.getinfo(pyodbc.SQL_MAX_CONCURRENT_ACTIVITIES)
        return nbr_cursors

    @retry(retries=3, error="Error executing stream_to_s3_parquet")
    def stream_to_s3_parquet(self, query:str, batch_size:int, s3_bucket:str, s3_prefix:str, s3_filename:str, **kwargs) -> None:
        """
            Streams the results of a sql query to parquet files on s3 with 'batch_size' nbr of rows per file. 
            Output files have the following key:
                {s3_bucket}{s3_prefix}{timestamp}_{filename}.parquet        
        """
        if kwargs['attempt'] > 1:       # ensure idempotency in case of retry
            s3 = boto3.resource('s3')
            bucket = s3.Bucket(s3_bucket)
            bucket.objects.filter(Prefix=s3_prefix).delete()
        for x in self._stream_results(query, batch_size):
            col_names = [column[0] for column in x[0].cursor_description]
            df = pd.DataFrame.from_records(x, columns=col_names)
            self._df_to_parquet_s3(df, s3_bucket, s3_prefix, s3_filename)
        return
         
    def _stream_results(self, query:str, batch_size: int) -> None:
        cursor = self._create_cursor()
        cursor_iterator = cursor.execute(query)
        while True:
            rows = cursor_iterator.fetchmany(batch_size)
            if not rows:
                break
            yield rows
            # for row in rows:
            #     yield row

    def _df_to_parquet_s3(self, df:pd.DataFrame, s3_bucket: str, s3_prefix: str, file_name:str) -> None:
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, index=False, allow_truncated_timestamps=True)
        s3 = boto3.resource('s3')
        timestamp = round(time(), 4)
        s3_key = f"{s3_prefix}{timestamp}_{file_name}.parquet"
        s3.Object(s3_bucket, s3_key).put(Body=parquet_buffer.getvalue())
        logging.info(f'Stored {s3_key} on s3 {s3_bucket}')
        del df
        return
    
    def close_connection(self) -> None:
        self._connection_instance.close()
        logging.info("Closed connection to SQL server")
        return



        