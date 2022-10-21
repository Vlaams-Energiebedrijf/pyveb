
import boto3
import pandas as pd
import datetime
import logging
from io import BytesIO
from time import time
import psutil
import sys
import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame as SparkDataFrame
import pyspark.sql.functions as F
import numpy as np


class tfClient():
    def __init__(self,s3_client, s3_bucket, s3_target_prefix, partition_start, **kwargs):
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_target_prefix
        self.partition_date = partition_start
        self.s3_client = s3_client
        self.spark = self._create_spark_session()
        return

    def _create_spark_session(self, loglevel = "ERROR"):
         # https://stackoverflow.com/questions/50891509/apache-spark-codegen-stage-grows-beyond-64-kb 
        nbr_cores = self._get_nbr_cores()
        spark = SparkSession.builder.master(f"local[{nbr_cores}]") \
                    .appName(f'Spark_{self.s3_prefix}') \
                    .config('spark.sql.codegen.wholeStage', 'false') \
                    .config("spark.sql.session.timeZone", "UTC") \
                    .getOrCreate()
        spark.sparkContext.setLogLevel(loglevel)
        return spark

    def _get_nbr_cores(self):
       return psutil.cpu_count(logical = False)

    def pandas_read_parquet(self, file, **kwargs) -> pd.DataFrame:
        return pd.read_parquet(file, **kwargs)

    def pandas_read_csv(self, file,  **kwargs) -> pd.DataFrame:
        return pd.read_csv(file, **kwargs)

    ## in case we have null dates, they cannot be 9999-01-01 since this compromises our upsert operation
    def pandas_format_timestamps(self, df:pd.DataFrame) -> pd.DataFrame:
        datetime_df = df.select_dtypes(include="datetime")
        datetime_cols = datetime_df.columns
        # replace cogenius NULL with datetimelike string in order to convert via .dt accessor
        if datetime_cols:
            df[datetime_cols] = df[datetime_cols].replace({None: '1111-01-01 00:00:00.000'})
            df[datetime_cols] = df[datetime_cols].apply(pd.to_datetime, format='%Y-%m-%d %H:%M:%S', errors='coerce')
        return df

    # ensure consistency with parser.meta_cols, parser.spark_meta_cols
    def pandas_add_metadata(self, file, df:pd.DataFrame) -> pd.DataFrame:
        df['META_file_name'] = file
        df['META_partition_date'] = self.partition_date
        df['META_partition_date'] = df['META_partition_date'].apply(pd.to_datetime, format='%Y-%m-%d', errors='coerce')
        df['META_processing_date_utc'] = datetime.datetime.now(datetime.timezone.utc)
        df['META_processing_date_utc'] = df['META_processing_date_utc'].apply(pd.to_datetime, format='%Y-%m-%d %H:%M:%S', errors='coerce')
        return df

    def pandas_write_to_parquet(self, file, df: pd.DataFrame) -> None:
        file_name = file.split('/')[-1]
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, index=False, allow_truncated_timestamps=True)
        s3 = boto3.resource('s3')
        # we keep raw file name but overwrite extension if not parquet already
        if '.csv' in file_name:
            file_name = file_name.replace('.csv', '.parquet')
        if '.xlsx' in file_name:
            file_name = file_name.replace('.xslx', '.parquet')
        if '.xls' in file_name:
            file_name = file_name.replace('.xls', '.parquet')
        if '.parquet' in file_name:
            file_name = file_name
        s3_key = f"{self.s3_prefix}{file_name}"
        s3.Object(self.s3_bucket, s3_key).put(Body=parquet_buffer.getvalue())
        logging.info(f'Stored {s3_key} on s3 {self.s3_bucket}')
        del df
        return

    def pandas_convert_objects_to_string(self, df: pd.DataFrame) -> pd.DataFrame:
        columns = df.columns
        for col in columns:
            if (dict(df.dtypes)[col]) in ['object']:
                df[col] = df[col].map(str)
        return df

    def pandas_to_spark_dataframe(self, df:pd.DataFrame, schema) -> SparkDataFrame:
        return self.spark.createDataFrame(df, schema = schema)

    def spark_apply_schema(self, df:SparkDataFrame , schema) -> SparkDataFrame:
        return self.spark.createDataFrame(df.collect(), schema = schema)

    def spark_empty_and_nan_to_null(self, df: SparkDataFrame) -> SparkDataFrame:
        """
            This function might return columns containing only nulls. This will mess with the column datatypes. Hence, make sure
            you apply spark_apply_schema function after so the null columns are stored in parquet with the correct datatype as expected by redshift
        """
        columns = df.columns
        for column in columns:
            if dict(df.dtypes)[column] in ['int', 'double']:
                df = df.withColumn(column,F.when(F.isnan(F.col(column)),None).otherwise(F.col(column)))
            df = df.withColumn(column, F.when(F.col(column) == '', None).otherwise(F.col(column)))
            df = df.withColumn(column, F.when(F.col(column) == ' ', None).otherwise(F.col(column)))
            df = df.withColumn(column, F.when(F.col(column) == '  ', None).otherwise(F.col(column)))
            if dict(df.dtypes)[column] not in ['int', 'double']:
                df = df.withColumn(column, F.when(F.col(column) == 'NaN', None).otherwise(F.col(column)))
                df = df.withColumn(column, F.when(F.col(column) == 'NaT', None).otherwise(F.col(column)))
        return df

    def spark_write_to_parquet(self, spark_df, max_records_per_file = 10000):
        local_path = './data'
        try:
            shutil.rmtree(local_path)
        except Exception:
            None
        try:
            spark_df.write.option('maxRecordsPerFile', max_records_per_file).mode('overwrite').parquet(local_path)
            for file in os.listdir(local_path):
                local_file = f'{local_path}/{file}'
                if '.crc' not in file and 'SUCCESS' not in file:
                    self.s3_client.upload_local_file(local_file, self.s3_prefix)
                os.remove(local_file)
            try:
                os.rmdir(local_path)
            except Exception:
                None
            logging.info(f'Succesfully wrote {self.s3_prefix}')
        except Exception as e:
            logging.error(f'Issue creating parquet file: {self.s3_prefix}. Exiting...')
            logging.error(e)
            sys.exit(1)
        return 

    def pandas_convert_int_null_back_to_int(self, df: pd.DataFrame, columns: list) -> pd.DataFrame:
        for col in columns:
            df[col] = df[col].astype('Int64')
            # convert to None because otherwise we can't load into pyspark IntegerType
            df[col] = df[col].replace({np.nan: None})
        return df



