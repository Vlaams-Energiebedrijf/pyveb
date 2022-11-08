import os, urllib
import pandas as pd
import pyodbc

class sqlTerraClient():

    DRIVER = '{ODBC Driver 17 for SQL Server}'

    def __init__(self, env):
        """
            When deploying, environment variables are injected depending on ENV via entrypoint.sh.
            For local development, we fetch environemnt variables from local enviroment where we have variables for local, dev and prd, hence the except statement

            ! since we don't have a local redshift cluster we load into redshift dev 
        """
        try:
            server=os.environ['TERRA_HOST']
            db=os.environ['TERRA_DB']
            username=os.environ['TERRA_USERNAME']
            password=os.environ['TERRA_PASSWORD'] 
        except Exception:
            server=os.environ[f'TERRA_HOST_{env.upper()}']
            db=os.environ[f'TERRA_DB_{env.upper()}']
            username=os.environ[f'TERRA_USERNAME_{env.upper()}']
            password=os.environ[f'TERRA_PASSWORD_{env.upper()}'] 
        conn_string = self._make_connection_string(server, db, username, password)
        self.conn = pyodbc.connect(conn_string)
        return 

    def _make_connection_string(self, server: str, db: str, username: str, password: str) -> str:
        connection_string = ';'.join([
            f'DRIVER={self.DRIVER}',
            f'SERVER=tcp:{server}',
            f'DATABASE={db}',
            f'UID={username}',
            f'PWD={password}'
        ])
        params = urllib.parse.quote_plus(connection_string)
        return f"mssql+pyodbc:///?odbc_connect={params}"

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

    # def query_to_list(self, query:str):
    #     """
    #         fetchall() where all rows will be stored in memory
    #         returns 
    #             data list
    #             columns list
    #             dtypes list
    #     """
    #     cursor = self.conn.cursor()
    #     cursor.execute(query)
    #     rows = cursor.fetchall()
    #     # https://stackoverflow.com/questions/5504340/python-mysqldb-connection-close-vs-cursor-close
    #     columns = [column[0] for column in cursor.description]
    #     dtypes = [column[1] for column in cursor.description]
    #     cursor.close()
    #     return rows, columns, dtypes

    # def query_fetch_single_value(self, query: str) -> str:
    #     """
    #         grab a scalar, eg select max(x) from y
    #     """
    #     cursor = self.conn.cursor()
    #     cursor.execute(query)
    #     val = cursor.fetchval()
    #     cursor.close()
    #     return val

    
    # def get_max_cursors(self) -> str:
    #     cursor = self.conn.cursor()
    #     nbr_cursors = cursor.getinfo(pyodbc.SQL_MAX_CONCURRENT_ACTIVITIES)
    #     return nbr_cursors

    # def cursor(self):
    #     cursor = self.conn.cursor()
    #     return cursor


    # def stream_to_s3_parquet(self, query:str, batch_size:int, s3_bucket:str, s3_prefix:str, s3_filename:str) -> None:
    #     """
    #         Streams the results of a sql query to parquet files on s3 with 'batch_size' nbr of rows per file. 
    #         Output files have the following key:
    #             {s3_bucket}{s3_prefix}{timestamp}_{filename}.parquet        
    #     """
    #     for x in self._stream_results(query, batch_size):
    #         col_names = [column[0] for column in x[0].cursor_description]
    #         df = pd.DataFrame.from_records(x, columns=col_names)
    #         self._df_to_parquet_s3(df, s3_bucket, s3_prefix, s3_filename)
    #     return

                
    # def _stream_results(self, query:str, batch_size: int):
    #     cursor = self.conn.cursor()
    #     iter = cursor.execute(query)
    #     while True:
    #         rows = iter.fetchmany(batch_size)
    #         if not rows:
    #             break
    #         yield rows
    #         # for row in rows:
    #         #     yield row

    # def _df_to_parquet_s3(self, df:pd.DataFrame, s3_bucket: str, s3_prefix: str, file_name:str):
    #     parquet_buffer = BytesIO()
    #     df.to_parquet(parquet_buffer, index=False, allow_truncated_timestamps=True)
    #     s3 = boto3.resource('s3')
    #     timestamp = round(time(), 4)
    #     s3_key = f"{s3_prefix}{timestamp}_{file_name}.parquet"
    #     s3.Object(s3_bucket, s3_key).put(Body=parquet_buffer.getvalue())
    #     logging.info(f'Stored {s3_key} on s3 {s3_bucket}')
    #     del df
    #     return
    




        