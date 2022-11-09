
import os, sys
import pandas as pd
import pyodbc
import boto3
import logging
from time import time
from io import BytesIO

class sqlTerraClient():

    DRIVER = '{ODBC Driver 17 for SQL Server}'
    PORT = 1433

    def __init__(self, env):
        """
            When deploying, environment variables are injected depending on ENV via entrypoint.sh.
            For local development, we fetch environemnt variables from local enviroment where we have variables for local, dev and prd, hence the except statement

            ! since we don't have a local redshift cluster we load into redshift dev 
        """
        try:
            logging.info('Trying to fetch sql credentials from environment variables')
            server_raw=os.environ['TERRA_HOST']
            db=os.environ['TERRA_DB']
            username=os.environ['TERRA_USERNAME']
            password=os.environ['TERRA_PASSWORD'] 
        except Exception:
            logging.info('Trying to fetch ENV specific sql credentials from environment variables')
            server_raw=os.environ[f'TERRA_HOST_{env.upper()}']
            db=os.environ[f'TERRA_DB_{env.upper()}']
            username=os.environ[f'TERRA_USERNAME_{env.upper()}']
            password=os.environ[f'TERRA_PASSWORD_{env.upper()}'] 
        server = f'{server_raw},{self.PORT}'
        connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + db + ';UID=' + username + ';PWD=' + password
        try:
            logging.info('Trying to create a connection to DB')
            self.conn = pyodbc.connect(connection_string)
            logging.info('Succesfully creaeted DB connection')
        except Exception as e:
            logging.error(f'Issue creating connection: {e} .Exiting...')
            sys.exit(1)
        return 

    def query_to_df(self, query:str) -> pd.DataFrame:
        """
            fetchall() where all rows will be stored in memory
            returns pandas dataframe
        """
        logging.info('Executing query on DB')
        cursor = self.conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [column[0] for column in cursor.description]
        cursor.close()
        df = pd.DataFrame.from_records(rows, columns=columns)
        logging.info('Succesfully queried DB and returned as a pandas DF')
        return df

    def stream_to_s3_parquet(self, query:str, batch_size:int, s3_bucket:str, s3_prefix:str, s3_filename:str) -> None:
        """
            Streams the results of a sql query to parquet files on s3 with 'batch_size' nbr of rows per file. 
            Output files have the following key:
                {s3_bucket}{s3_prefix}{timestamp}_{filename}.parquet        
        """
        logging.info(f'Streaming result of query {query} to s3 {s3_bucket}/{s3_prefix} with batch size {batch_size}')
        for x in self._stream_results(query, batch_size):
            col_names = [column[0] for column in x[0].cursor_description]
            df = pd.DataFrame.from_records(x, columns=col_names)
            self._df_to_parquet_s3(df, s3_bucket, s3_prefix, s3_filename)
        logging.info('Completed streaming of query to parquet files on s3')
        return
                
    def _stream_results(self, query:str, batch_size: int):
        cursor = self.conn.cursor()
        iterator = cursor.execute(query)
        while True:
            rows = iterator.fetchmany(batch_size)
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
        return
