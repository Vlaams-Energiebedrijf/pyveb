import os, sys
import pandas as pd
import pyodbc
import boto3
import logging
from time import time, sleep
from io import BytesIO

class sqlTerraClient():

    DRIVER = '{ODBC Driver 17 for SQL Server}'
    PORT = 1433

    def __init__(self, env, **kwargs):
        """
            Pass additional pyodbc arguments, eg auto_commit=true, as kwargs

            When running on AWS Batch, environment variables are injected via entrypoint.sh - TRY statement)
            When developing locally, environment variables are injected via the shell env - EXCEPT statement
            
            You need to set up the following env variables in .zshr or similar: 

                export TERRA_HOST_LOCAL='vo-ter-dev-rds-01.cspft74uddih.eu-west-1.rds.amazonaws.com'
                export TERRA_USERNAME_LOCAL='TerraAdmin'
                export TERRA_PASSWORD_LOCAL='
                export TERRA_DB_LOCAL='Terra-dev'
                export TERRA_HOST_DEV='vo-ter-dev-rds-01.cspft74uddih.eu-west-1.rds.amazonaws.com'
                export TERRA_USERNAME_DEV='TerraAdmin'
                export TERRA_PASSWORD_DEV=
                export TERRA_DB_DEV='Terra-dev'
                export TERRA_HOST_PRD='vo-ter-prd-rds-01.cspft74uddih.eu-west-1.rds.amazonaws.com'
                export TERRA_USERNAME_PRD='TerraAdmin'
                export TERRA_PASSWORD_PRD=
                export TERRA_DB_PRD='Terra'
                export TERRA_HOST_STG='vo-ter-stg-rds-01.cspft74uddih.eu-west-1.rds.amazonaws.com'
                export TERRA_USERNAME_STG='TerraAdmin'
                export TERRA_PASSWORD_STG=
                export TERRA_DB_STG='Terra-stg'
        """
        try:
            logging.info('Trying to fetch sql credentials from environment variables')
            self.server_raw=os.environ['TERRA_HOST']
            self.db=os.environ['TERRA_DB']
            self.username=os.environ['TERRA_USERNAME']
            password=os.environ['TERRA_PASSWORD'] 
        except Exception:
            logging.info('Trying to fetch ENV specific sql credentials from environment variables')
            self.server_raw=os.environ[f'TERRA_HOST_{env.upper()}']
            self.db=os.environ[f'TERRA_DB_{env.upper()}']
            self.username=os.environ[f'TERRA_USERNAME_{env.upper()}']
            password=os.environ[f'TERRA_PASSWORD_{env.upper()}'] 
        self.server = f'{self.server_raw},{self.PORT}'
        connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + self.server + ';DATABASE=' + self.db + ';UID=' + self.username + ';PWD=' + password
        
        retries = 3
        retry_delay = 1  # seconds

        for attempt in range(retries + 1):
            try:
                logging.info('Trying to create a connection to DB')
                self.conn = pyodbc.connect(connection_string, **kwargs)
                logging.info('Succesfully creaeted DB connection')
                break  # If successful, exit the loop
            except pyodbc.Error as e:
                print(f"Error connecting to the database: {e}")
                if attempt < retries:
                    sleep(retry_delay * (2 ** attempt))
                else:
                    print("Max retries reached. Exiting.")
                    sys.exit(1)
        return 
    
    def call_stored_procedure(self, proc_statement:str): 
        # sourcery skip: extract-method
        """ 
            ! ensure you instantiate a sqlTerraClient passing auto_commit=True kwarg
            eg. proc_statement = '
        
        """
        retries = 3
        retry_delay = 1  # seconds

        for attempt in range(retries + 1):
            try:
                logging.info('Trying to create a connection to DB')
                cursor = self.conn.cursor()
                logging.info('Succesfully creaeted DB connection')
                break  # If successful, exit the loop
            except pyodbc.Error as e:
                logging.warning(f"Error connecting to the database: {e}")
                if attempt < retries:
                    sleep(retry_delay * (2 ** attempt))
                else:
                    logging.error("Max retries reached. Exiting.")
                    sys.exit(1)

        try:
            cursor.execute(proc_statement)
            print(f'Stored procedure {proc_statement} executed correctly')
            cursor.close()
        except pyodbc.Error as e:
            print("Error calling the stored procedure:", e)
            raise e

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
