
### WORK IN PROGRESS





import boto3
import pandas as pd
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
import datetime





class sparkClient():

    # file_name = file.split('/')[-1]

    def __init__(self, s3_client, s3_bucket, s3_target_prefix, partition_start, **kwargs):
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_target_prefix
        self.partition_date = partition_start
        self.s3_client = s3_client
        self.spark = self._create_spark_session()
        return None

    def _create_spark_session(self, loglevel = "ERROR"):
         # https://stackoverflow.com/questions/50891509/apache-spark-codegen-stage-grows-beyond-64-kb 
        nbr_cores = self._get_nbr_cores()
        spark = SparkSession.builder.master(f"local[{nbr_cores}]") \
                    .appName(f'Spark_{self.s3_prefix}') \
                    .config('spark.sql.codegen.wholeStage', 'false') \
                    .config("spark.sql.session.timeZone", "UTC") \
                    .getOrCreate()
        # .config('spark.sql.parquet.filterPushdown', 'false') \
        spark.sparkContext.setLogLevel(loglevel)
        return spark

    def _get_nbr_cores(self) -> int:
       return psutil.cpu_count(logical = False)

    def read_parquet_to_pyspark(self, file:str, delete_file=True) -> SparkDataFrame:
        """
            ARGUMENTS
                file: ./data/filename.parquet
            RETURNS
                spark_df

            ADDITIONAL INFO
                Download file locally with s3_client.download_s3_to_local_file()
                Downloading straight from S3 is complex via pyspark, hence we dowload locally first. 
                Additionally, we delete the local file afterwards if delete_file = True
        """
        spark_df = self.spark.read.parquet(file)

        if delete_file and spark_df:
            os.remove(file)
        return spark_df

    def add_metadata(self, df:SparkDataFrame, file_name: str, partition_date:str) -> SparkDataFrame:
        df = df.withColumn('META_file_name', F.lit(str(file_name))) \
                .withColumn('META_partition_date', F.lit(datetime.strptime(partition_date, "%Y-%m-%d"))) \
                .withColumn('META_processing_date_utc', F.lit(datetime.datetime.utcnow()))
        return df

    def convert_int_with_null_back_to_int(self, df: SparkDataFrame, cols: list) -> SparkDataFrame:
        """
            When reading from SQL, columns which are INT but contain NULLS get converted to parquet float columns.
            We create a pyspark schema based on the original SQL schema, hence INT column gets translated into StructField('col', IntegerType(), True).
            In order to apply this pyspark schema, we need to convert the relevant int columns back to int.
        """
        map_to_int = F.udf(lambda x: self._map_float_to_int(x))
        for col in cols:
            df = df.withColumn(col, (map_to_int(df[col])).cast("string"))
        return df


    def _map_float_to_int(self, num):
        if num is None:
            return None
        return str(int(num))


    def spark_apply_schema(self, df:SparkDataFrame , schema) -> SparkDataFrame:
        spark_df = self.spark.createDataFrame(df.collect(), schema = schema)
        return spark_df