import contextlib
import pandas as pd
import shutil, os
import logging
from datetime import datetime, timezone
from time import time
from functools import reduce
import psutil
import sys
import json
from typing import List, Dict
from operator import itemgetter
import uuid

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame as SparkDataFrame
import pyspark.sql.functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType, DoubleType, DecimalType, ArrayType, BinaryType, LongType
import boto3

from . import s3_client

class sparkClient():
    """
        env: local, dev or prd
    """

    ### create session ###########################################################################################################################
    ##############################################################################################################################################
    def __init__(self, env:str, **kwargs):
        self.env = env
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
        """
           Create a spark session on a local cluster (ie. single node) using nbr_of_cores cores.

           In case spark runs on AWS batch, we need to use temporary AWS credentials in order to read from 
           S3. In case of local dev we can use default AWS profile. 
        """
         # https://stackoverflow.com/questions/50891509/apache-spark-codegen-stage-grows-beyond-64-kb 
        nbr_cores = self._get_nbr_cores()

        try:
            if self.env == 'local':
                logging.info('Building spark session w ProfileCredentialsProvider for S3 access')
                spark = SparkSession.builder.master(f"local[{nbr_cores}]") \
                            .appName(f'Spark_{str(uuid.uuid4())}') \
                            .config('spark.sql.codegen.wholeStage', 'false') \
                            .config("spark.sql.session.timeZone", "UTC") \
                            .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0')\
                            .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "LEGACY") \
                            .config("fs.s3a.aws.credentials.provider","com.amazonaws.auth.profile.ProfileCredentialsProvider")\
                            .getOrCreate()

            else:
                access_key, secret_key, session_token = self._get_temp_batch_credentials()
                # https://stackoverflow.com/questions/54223242/aws-access-s3-from-spark-using-iam-role?noredirect=1&lq=1
                logging.info('Building spark session w TemporaryAWSCredentialsProvider for S3 access')
                spark = SparkSession.builder.master(f"local[{nbr_cores}]") \
                            .appName(f'Spark_{str(uuid.uuid4())}') \
                            .config('spark.sql.codegen.wholeStage', 'false') \
                            .config("spark.sql.session.timeZone", "UTC") \
                            .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0')\
                            .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "LEGACY") \
                            .config('fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider')\
                            .config('fs.s3a.access.key', access_key)\
                            .config('fs.s3a.secret.key', secret_key)\
                            .config('fs.s3a.session.token', session_token)\
                            .getOrCreate()  

            spark.sparkContext.setLogLevel("ERROR")
            logging.info("Spark Session created")
        except Exception as e:
            logging.error("Issue creating Spark Session. Exiting...")
            logging.error(e)
            sys.exit(1)
        return spark

    @staticmethod
    def _get_nbr_cores() -> int:
       return psutil.cpu_count(logical = False)

    ### read into spark df  ######################################################################################################################
    ##############################################################################################################################################
    def read_single_csv_file(self, file:str, schema: Dict[str, StructField] = None, header: str = "true", delimiter: str = ";") -> SparkDataFrame:
        """
            file: complete s3 URI including s3://
        """
        file = file.replace('s3://', 's3a://')
        try: 
            df = self.spark.read.format("csv").option("header",header).option("delimiter", delimiter).load(file)
            logging.info(f"Read {file} in spark DF")
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
        """ 
            file: complete s3 URI including s3://
        """
        file = file.replace('s3://', 's3a://')
        try: 
            df = self.spark.read.format("parquet").load(file)
            logging.info(f"Read {file} in spark DF")
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
        """
            files: list of complete s3 URI including s3://
        """   
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
        """
            files: list of complete s3 URI including s3://
        """   
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

    def pandas_df_to_spark_df(self, pandas_df: pd.DataFrame, schema=None) -> SparkDataFrame:
        if schema:
            df_spark = self.spark.createDataFrame(pandas_df, schema=schema)
        else:
            df_spark = self.spark.createDataFrame(pandas_df)
        return df_spark

    #### write spark df  #########################################################################################################################
    ##############################################################################################################################################
    def write_to_parquet(self, spark_df: SparkDataFrame, s3_bucket: str, s3_prefix: str, max_records_per_file = 100000) -> None:
        s3 = s3_client.s3Client(s3_bucket)
        local_path = './data'
        with contextlib.suppress(Exception):
            shutil.rmtree(local_path)
        try:
            spark_df.write.option('maxRecordsPerFile', max_records_per_file).mode('overwrite').parquet(local_path)
            for file in os.listdir(local_path):
                local_file = f'{local_path}/{file}'
                if '.crc' not in file and 'SUCCESS' not in file:
                    s3.upload_local_file(local_file, s3_prefix )
                os.remove(local_file)
            try:
                os.rmdir(local_path)
            except Exception:
                None
            logging.info(f'Succesfully wrote {s3_prefix}')
        except Exception as e:
            logging.error(f'Issue creating parquet file: {s3_prefix}. Exiting...')
            logging.error(e)
            sys.exit(1)
        return 

    @staticmethod
    def spark_df_to_pandas_df(df: SparkDataFrame) -> pd.DataFrame:
        return df.toPandas()
    
    def apply_schema(self, df: SparkDataFrame, schema: StructType) -> SparkDataFrame:
        """
            Method that first reoders the columns to align with the Spark Schema and applies it afterwards
        """
        try:
            df = df.select(StructType.fieldNames(schema))
            df = self.spark.createDataFrame(df.collect(), schema = schema)
            logging.info("Succesfully applied schema")
        except Exception as e:
            logging.error("Issue applying schema. Exiting...")
            logging.error(e)
            sys.exit(1)
        return df


    #### UDF & their methods #####################################################################################################################
    ##############################################################################################################################################
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

    @staticmethod
    @udf
    def udf_string_to_int(x):
        if x is None or x =='':
            res = None
        else:
            try:
                # https://stackoverflow.com/questions/1841565/valueerror-invalid-literal-for-int-with-base-10 
                res = int(x)
            except AttributeError:
                res = x
        return res

    @staticmethod
    @udf
    def udf_string_to_timestamp(x):
        if x is None or x =='':
            res = None
        else:
            x = x.split('.')[0]
            date_format = '%Y-%m-%d %H:%M:%S'
            try:
                # https://stackoverflow.com/questions/1841565/valueerror-invalid-literal-for-int-with-base-10 
                res = datetime.strptime(x, date_format)
            except AttributeError:
                res = x
        return str(res)

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

    @staticmethod
    @udf
    def udf_filter_max_from_array(x:str, sort_field:str) -> any:
        data = json.loads(x)
        if not data:
            return json.dumps(data)
        max_element = max(data, key=itemgetter(sort_field))
        return json.dumps(max_element)

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

    def convert_string_to_int_int(self, df: SparkDataFrame, cols: list) -> SparkDataFrame:
        """
            Convert list of string cols to IntegerType() by explicit casting
        """
        try: 
            new_df = df.select(*[self.udf_string_to_int(column).cast(IntegerType()).alias(column) if column in cols else column for column in df.columns])
            logging.info("Succesfully converted str columns to int")
        except Exception as e:
            logging.error("Issue converting str columns to int. Exiting...")
            logging.error(e)
            sys.exit(1)
        return new_df

    def convert_string_to_timestamp(self, df: SparkDataFrame, cols: list) -> SparkDataFrame:
        """
            Convert list of string cols to TimestampType() by explicit casting
        """
        try: 
            print('cols')
            print(cols)
            new_df = df.select(*[self.udf_string_to_timestamp(column).cast(TimestampType()).alias(column) if column in cols else column for column in df.columns])
            logging.info("Succesfully converted string columns to timestamp")
        except Exception as e:
            logging.error("Issue converting str columns to timestamp. Exiting...")
            logging.error(e)
            sys.exit(1)
        return new_df

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

    def filter_max_from_array(self, df: SparkDataFrame, spark_array: str, sort_field: str ) -> SparkDataFrame:
        """
            Method to filter an array and only keep the array_element for which max(array_element.sort_field)
        """ 
        temp_col_name = str(uuid.uuid4())
        df = df.withColumn(temp_col_name, self.udf_filter_max_from_array(F.to_json(F.col(spark_array)), F.lit(sort_field)))
        df = df.drop(spark_array)
        df_json_schema = self.spark.read.json(df.rdd.map(lambda row: row[temp_col_name])).schema
        df = df.withColumn(spark_array, F.from_json(F.col(temp_col_name), df_json_schema))
        df = df.drop(temp_col_name)
        return df

    #### static dataframe manipulation methods ###################################################################################################
    ##############################################################################################################################################
    @staticmethod
    def add_metadata(df:SparkDataFrame, file_name = None, partition_date = datetime(2020,1,1)) -> SparkDataFrame:
        """
            partition_date: airflow execution date passed as a datetime.datetime object
        """
        if not file_name: file_name = F.input_file_name()
        # partition_date = str(partition_date.date())
        try: 
            df = df.withColumn('META_file_name', F.lit(file_name)) \
                        .withColumn('META_partition_date', partition_date) \
                        .withColumn('META_processing_date_utc', F.lit(datetime.now(timezone.utc)))
            logging.info("Succesfully added metadata")
        except Exception as e:
            logging.error("Issue adding metadata. Exiting...")
            logging.error(e)
            sys.exit(1)
        return df

    @staticmethod
    def enforce_schema(df: SparkDataFrame, schema: Dict[str, StructField]) -> SparkDataFrame:    
        try:
            new_df = df.select([F.col(c).cast(schema[c]).alias(c) for c in df.columns])
            logging.info("Succesfully applied schema")
        except Exception as e:
            logging.error("Issue applying schema. Exiting...")
            logging.error(e)
            sys.exit(1)
        return new_df

    @staticmethod
    def _unite_dfs(df1: SparkDataFrame, df2: SparkDataFrame) -> SparkDataFrame:
        return df1.unionByName(df2)

    @staticmethod
    def reindex_cols(df: SparkDataFrame, columns_order: List[str]) -> SparkDataFrame:
        try:
            new_df = df.select(*columns_order)
            logging.info("Succesfully reindexed columns")
        except Exception as e:
            logging.error("Issue reindexing columns. Exiting...")
            logging.error(e)
            sys.exit(1)
        return new_df

    @staticmethod
    def clean_old_dates(self, df:SparkDataFrame, cols_to_clean:list) -> SparkDataFrame:
        """
            Spark 3.0 has difficulties working with dates older than 1900-01-01.

            org.apache.spark.SparkUpgradeException: You may get a different result due to the upgrading of Spark 3.0: reading dates before 1582-10-15 or timestamps before 1900-01-01T00:00:00Z from Parquet files can be ambiguous, as the files may be written by Spark 2.x or legacy versions of Hive, which uses a legacy hybrid calendar that is different from Spark 3.0+'s Proleptic Gregorian calendar. See more details in SPARK-31404. You can set spark.sql.legacy.parquet.datetimeRebaseModeInRead to 'LEGACY' to rebase the datetime values w.r.t. the calendar difference during reading. Or set spark.sql.legacy.parquet.datetimeRebaseModeInRead to 'CORRECTED' to read the datetime values as it is.
        
            Adding datetimeRebaseModeInRead still results in 'mktime argument out of range' errors. Hence, we'll manually clean these dates.
        """
        new_df = df.select([
            F.when(F.col(column) < "1900-01-01 00:00:00", F.lit("1900-01-01 00:00:00"))\
            .otherwise(F.col(column))\
            .cast(TimestampType())\
            .alias(column) if column in cols_to_clean else column for column in df.columns
            ]
        )
        return new_df

    @staticmethod
    def nan_to_null(df: SparkDataFrame) -> SparkDataFrame:
        """
            In case you have string columns with NaN strings ( eg after reading pd.read_csv) which you want 
            to load as NULL in redshift, apply this function. 
        """
        for col in df.columns:
            df = df.withColumn(col, F.when(F.col(col) == 'NaN', None).otherwise(F.col(col)))
        return df
    
    @staticmethod
    def flatten_struct(df: SparkDataFrame, flatten_col: StructType) -> SparkDataFrame:
        """     
            Method to flatten a StructType
        """
        cols = df.columns
        cols.remove(flatten_col)
        df = df.select(*cols, f'{flatten_col}.*')
        return df

    @staticmethod
    def filter_empty_from_array(df: SparkDataFrame, spark_array: str, filter_field: str) -> SparkDataFrame:
        """
            Method to filter elements from an array when array_element.filter_field is null
        """
        temp_col_name = str(uuid.uuid4())
        # https://stackoverflow.com/questions/73293720/pyspark-filter-an-array-of-structs-based-on-one-value-in-the-struct
        df = df.withColumn(temp_col_name, F.expr(f"filter({spark_array}, x -> x.{filter_field} is not null)")) 
        df = df.drop(spark_array)
        df = df.withColumnRenamed(temp_col_name, spark_array)
        return df

    @staticmethod
    def explode_column(df: SparkDataFrame, explode_col: str) -> SparkDataFrame:
        """    
            Explodes a column which contain a list of items. The col type can either be a string '['a', 'b']' or an array.

            Given df 
            +------+-------------+------+
            |  col1|         col2|  col3|
            +------+-------------+------+
            |    z1| [a1, b2, c3]|   foo|
            +------+-------------+------+

            explode_columns(df, col2) will result in 

            +-----+-----+-----+
            | col1| col2| col3|
            +-----+-----+-----+
            |   z1|   a1|  foo|
            |   z1|   b2|  foo|
            |   z1|   c3|  foo|
            +-----+-----+-----+
        """
        col_type = dict(df.dtypes)[explode_col]
        temp_col_name = str(uuid.uuid4())
        if col_type == 'string':
            # https://stackoverflow.com/questions/57066797/pyspark-dataframe-split-column-with-multiple-values-into-rows
            df = df.withColumn(temp_col_name, F.explode( F.split( F.regexp_extract( F.regexp_replace(F.col(explode_col), "\s", ""), "^\[(.*)\]$", 1), ",") ) )
            df = df.drop(explode_col)
            df = df.withColumnRenamed(temp_col_name, explode_col)
        if 'array' in col_type:
            df = df.withColumn(temp_col_name, F.explode(explode_col))
            df = df.drop(explode_col)
            df = df.withColumnRenamed(temp_col_name, explode_col)
        return df

    @staticmethod
    def drop_cols(df: SparkDataFrame, list_cols: List[str]) -> SparkDataFrame:
        """
            Method that drops al columns in the list_cols 1 by 1.
        """
        for c in list_cols:
            df = df.drop(c)
        return df


            



