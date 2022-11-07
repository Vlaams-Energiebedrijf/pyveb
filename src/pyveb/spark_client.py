
import contextlib
import pandas as pd
import shutil, os
import logging
from time import time
import psutil
import sys
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame as SparkDataFrame
import pyspark.sql.functions as F
import datetime
from functools import reduce
from pyspark.sql.functions import udf
from typing import List, Dict
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType, DoubleType, DecimalType, ArrayType, BinaryType, LongType
import boto3
from requests import session

class sparkClient():
    """
        !!! env is currently passed via **kwargs but this is a mandatory field! Should be refactored 
        
        In case we want to read straight from s3 into spark assumed_role kwarg needs to be provided
    
    """

    def __init__(self, s3_client, s3_bucket, s3_target_prefix, partition_start, **kwargs):
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_target_prefix
        self.partition_date = partition_start
        self.s3_client = s3_client
        self.env = kwargs['env']
        # self.assumed_role = kwargs['assumed_role']
        self.spark = self._create_spark_session()
        return None

    def _get_temp_batch_credentials(self ):
        # we cannot fetch sts temp credentials via an assumed role, hence, we get the current credentials of the assumed role
        # https://stackoverflow.com/questions/36287720/boto3-get-credentials-dynamically
        session = boto3.Session()
        credentials = session.get_credentials()
        credentials = credentials.get_frozen_credentials()
        access_key = credentials[0]
        secret_key = credentials[1]
        session_token = credentials[2]
        return access_key, secret_key, session_token

    def _create_spark_session(self):
         # https://stackoverflow.com/questions/50891509/apache-spark-codegen-stage-grows-beyond-64-kb 
        nbr_cores = self._get_nbr_cores()
        try:
            if self.env == 'local':
                logging.info('Building local spark session')
                spark = SparkSession.builder.master(f"local[{nbr_cores}]") \
                            .appName(f'Spark_{self.s3_prefix}') \
                            .config('spark.sql.codegen.wholeStage', 'false') \
                            .config("spark.sql.session.timeZone", "UTC") \
                            .config("fs.s3a.aws.credentials.provider","com.amazonaws.auth.profile.ProfileCredentialsProvider")\
                            .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0')\
                            .getOrCreate()
            else:
                access_key, secret_key, session_token = self._get_temp_batch_credentials()
                # https://stackoverflow.com/questions/54223242/aws-access-s3-from-spark-using-iam-role?noredirect=1&lq=1
                logging.info('Building cloud spark session')
                spark = SparkSession.builder.master(f"local[{nbr_cores}]") \
                            .appName(f'Spark_{self.s3_prefix}') \
                            .config('spark.sql.codegen.wholeStage', 'false') \
                            .config("spark.sql.session.timeZone", "UTC") \
                            .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0')\
                            .config('fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider')\
                            .config('fs.s3a.access.key', access_key)\
                            .config('fs.s3a.secret.key', secret_key)\
                            .config('fs.s3a.session.token', session_token)\
                            .getOrCreate()  
            #  config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')\
            # .config("fs.s3a.aws.credentials.provider","com.amazonaws.auth.InstanceProfileCredentialsProvider")\
            # "fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
            # .config('spark.sql.parquet.filterPushdown', 'false') \
            spark.sparkContext.setLogLevel("ERROR")
            logging.info(f"Spark Session Spark_{self.s3_prefix} created")
        except Exception as e:
            logging.error("Issue creating Spark Session. Exiting...")
            logging.error(e)
            sys.exit(1)
        return spark

    def _get_nbr_cores(self) -> int:
       return psutil.cpu_count(logical = False)

    def read_single_csv_file(self, file:str, schema: Dict[str, StructField] = None, header: str = "true", delimiter: str = ";") -> SparkDataFrame:
        file = file.replace('s3://', 's3a://')
        try: 
            df = self.spark.read.format("csv").option("header",header).option("delimiter", delimiter).load(file)
            logging.info(f"Read {self.s3_prefix} in spark DF")
            if schema:
                new_df = self.enforce_schema(df, schema)
                logging.info("Enforced schema")
            else:
                new_df = df
        except Exception as e:
            logging.error("Issue reading parquet Exiting...")
            logging.error(e)
            sys.exit(1)
        return new_df

    def read_single_parquet_file(self, file: str, schema: Dict[str, StructField] = None) -> SparkDataFrame:
        file = file.replace('s3://', 's3a://')
        try: 
            df = self.spark.read.format("parquet").load(file)
            logging.info(f"Read {self.s3_prefix} in spark DF")
            if schema:
                new_df = self.enforce_schema(df, schema)
                logging.info("Enforced schema")
            else:
                new_df = df
        except Exception as e:
            logging.error("Issue reading parquet Exiting...")
            logging.error(e)
            sys.exit(1)
        return new_df

    def read_multiple_csv_files(self, files: List[str], schema: Dict[str, StructField] = None, header: str = "true", delimiter: str = ";") -> SparkDataFrame:
        try:
            list_of_dfs = []
            for file in files:
                file = file.replace('s3://', 's3a://')
                df = self.spark.read.format("csv").option("header",header).option("delimiter", delimiter).load(file)
                if schema:
                    new_df = self.enforce_schema(df, schema)
                    list_of_dfs.append(new_df)
                else:
                    list_of_dfs.append(df)
            united_df = reduce(self._unite_dfs, list_of_dfs)
            logging.info("Succesfully read all files in a spark DF")
        except Exception as e:
            logging.error("Issue reading parquet files and unioning them in spark DF Exiting...")
            logging.error(e)
            sys.exit(1)
        return united_df
    
    def read_multiple_parquet_files(self, files: List[str], schema: Dict[str, StructField] = None) -> SparkDataFrame:
        try:
            list_of_dfs = []
            for file in files:
                file = file.replace('s3://', 's3a://')
                df = self.spark.read.format("parquet").load(file)
                if schema:
                    new_df = self.enforce_schema(df, schema)
                    list_of_dfs.append(new_df)
                else:
                    list_of_dfs.append(df)
            united_df = reduce(self._unite_dfs, list_of_dfs)
            logging.info("Succesfully read all files in a spark DF")
        except Exception as e:
            logging.error("Issue reading parquet files and unioning them in spark DF Exiting...")
            logging.error(e)
            sys.exit(1)
        return united_df

    def enforce_schema(self, df: SparkDataFrame, schema: Dict[str, StructField]) -> SparkDataFrame:    
        try:
            new_df = df.select([F.col(c).cast(schema[c]).alias(c) for c in df.columns])
            logging.info("Succesfully applied schema")
        except Exception as e:
            logging.error("Issue applying schema. Exiting...")
            logging.error(e)
            sys.exit(1)
        return new_df

    def _unite_dfs(self, df1: SparkDataFrame, df2: SparkDataFrame) -> SparkDataFrame:
        return df1.unionByName(df2)

    def add_metadata(self, df:SparkDataFrame) -> SparkDataFrame:
        try: 
            df = df.withColumn('META_file_name', F.input_file_name()) \
                        .withColumn('META_partition_date', F.lit(datetime.datetime.strptime(self.partition_date, "%Y-%m-%d"))) \
                        .withColumn('META_processing_date_utc', F.lit(datetime.datetime.now(datetime.timezone.utc)))
            logging.info("Succesfully added metadata")
        except Exception as e:
            logging.error("Issue adding metadata. Exiting...")
            logging.error(e)
            sys.exit(1)
        return df

    @staticmethod
    @udf
    def udf_unicode(x):
        if x is None: 
            res = None
        else:
            try:
                res = x.encode("ascii","ignore")
            except AttributeError:
                res = x
        return res

    def convert_version(self, df: SparkDataFrame) -> SparkDataFrame: 
        if 'version' in df.columns:
            try: 
                new_df = df.select(*[self.udf_unicode(column).alias('version') if column == 'version' else column for column in df.columns])
                logging.info("Succesfully converted lynx version column to ascii.")
            except Exception as e:
                logging.error("Issue converting lynx version column.Exiting...")
                logging.error(e)
                sys.exit(1)
        else: new_df = df
        return new_df

    def reindex_cols(self, df: SparkDataFrame, columns_order: List[str]) -> SparkDataFrame:
        try:
            new_df = df.select(*columns_order)
            logging.info("Succesfully reindexed columns")
        except Exception as e:
            logging.error("Issue reindexing columns. Exiting...")
            logging.error(e)
            sys.exit(1)
        return new_df

    def apply_schema(self, df:SparkDataFrame , schema) -> SparkDataFrame:
        return self.spark.createDataFrame(df.collect(), schema = schema)

    @staticmethod
    @udf
    def udf_float_to_int(x):
        if x is None:
            res = None
        else:
            try:
                res = int(x)
            except AttributeError:
                res = x
        return res

    def convert_float_to_int_int(self, df: SparkDataFrame, cols: list) -> SparkDataFrame:
        """
            When reading from SQL, columns which are INT but contain only NULLS get converted to parquet float columns.
            We create a pyspark schema based on the original SQL schema, hence INT column gets translated into StructField('col', IntegerType(), True).
            In order to apply this pyspark schema, we need to convert the relevant int columns back to int.
        """
        try: 
            new_df = df.select(*[self.udf_float_to_int(column).cast(IntegerType()).alias(column) if column in cols else column for column in df.columns])
            logging.info("Succesfully converted float columns back to int")
        except Exception as e:
            logging.error("Issue converting float columns to int. Exiting...")
            logging.error(e)
            sys.exit(1)
        return new_df

    def write_to_parquet(self, spark_df: SparkDataFrame, max_records_per_file = 100000):
        local_path = './data'
        with contextlib.suppress(Exception):
            shutil.rmtree(local_path)
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

    def spark_df_to_pandas_df(self, df: SparkDataFrame) -> pd.DataFrame:
        return df.toPandas()

