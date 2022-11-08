import psycopg2
import os, sys
import logging
import pandas as pd
import json
from io import BytesIO
import boto3
from time import time

class rsClient():
    def __init__(self, env, rs_iam_role):
        """
            When deploying, environment variables are injected depending on ENV via entrypoint.sh.
            For local development, we fetch environemnt variables from local enviroment where we have variables for local, dev and prd, hence the except statement.

            ! since we don't have a local redshift cluster we load into redshift dev 
        """
        try:
            dbname=os.environ['REDSHIFT_DB']
            user = os.environ['REDSHIFT_UNAME']
            password = os.environ['REDSHIFT_PASSWORD']
            host = os.environ['REDSHIFT_HOST']
            port = os.environ['REDSHIFT_PORT']
        except Exception: 
            dbname=os.environ[f'REDSHIFT_DB_{env.upper()}']
            user = os.environ[f'REDSHIFT_UNAME_{env.upper()}']
            password = os.environ[f'REDSHIFT_PASSWORD_{env.upper()}']
            host = os.environ[f'REDSHIFT_HOST_{env.upper()}']
            port = os.environ[f'REDSHIFT_PORT_{env.upper()}']
        try:
            logging.info('Establishing connection to redshift via psycopg2')
            self.conn = psycopg2.connect(dbname=dbname,user = user,password = password,host = host, port = port)
            logging.info('Redshift Connection succesfully established')
        except psycopg2.DatabaseError as error:
            logging.error('Issue connecting to redshift. Exiting with status 1')
            logging.error(error)
            sys.exit(1)
        self.rs_iam_role = rs_iam_role

    def _enable_autocommit(self):
        self.conn.set_session(autocommit=True)
        return

    def _disable_autocommit(self):
        self.conn.set_session(autocommit=False)
        return

    def _query(self, sql):
        with self.conn.cursor() as cur:
            cur.execute(sql)
            self.conn.commit()
        return 

    def _create_stage_like_target(self,rs_target, rs_stage):
        self._query(f"""DROP TABLE IF EXISTS {rs_stage}""")
        self._query(f"""CREATE TABLE {rs_stage} (LIKE {rs_target})""")
        self._enable_autocommit() 
        self._query(f"""ALTER TABLE {rs_stage} DROP COLUMN meta_loading_date_utc""")
        self._disable_autocommit()

    def _copy_parquet_into_stage(self, files, rs_stage):
        for file in files:
            try:
                dml = f"""
                    COPY {rs_stage}
                    FROM '{file}'
                    IAM_ROLE '{self.rs_iam_role}'
                    FORMAT AS PARQUET
                """
                self._query(dml)
            except Exception as e:
                logging.error(f'Issue copying {file} into staging table. Exiting')
                logging.error(f'message: {e}', exc_info=True)
                sys.exit(1)
        logging.info('Succesfully loaded all files in temp staging table')
        return

    def _upsert(self, rs_target, rs_stage, upsert_keys):
        where_condition_target = ''
        counter = 0
        for col in upsert_keys:
            if counter == 0:
                line_target = f'{rs_target}.{col} = {rs_stage}.{col}'
            else:
                line_target = f'AND {rs_target}.{col} = {rs_stage}.{col}'
            where_condition_target += str(line_target)
            counter =+ 1
        try:
            self._query(f"""
                    begin transaction;

                    DELETE FROM {rs_target} 
                    USING {rs_stage} 
                    WHERE {where_condition_target} AND
                        GREATEST({rs_stage}.datecreated, {rs_stage}.datemodified) >= GREATEST({rs_target}.datecreated, {rs_target}.datemodified)
                    ;
                    
                    INSERT INTO {rs_target}
                    SELECT *
                    FROM {rs_stage};

                    DROP TABLE {rs_stage};
                    end transaction;
                """)
            logging.info(f'UPSERT succesfull for {rs_stage}')
        except Exception as e:
                logging.error('Issue UPSERTING stage into target. Exiting...')
                logging.error(f'message: {e}', exc_info=True)
                sys.exit(1)
    

    def _upsert_non_lynx(self, rs_target, rs_stage, upsert_keys):
        where_condition_target = ''
        counter = 0
        for col in upsert_keys:
            if counter == 0:
                line_target = f'{rs_target}.{col} = {rs_stage}.{col}'
            else:
                line_target = f'AND {rs_target}.{col} = {rs_stage}.{col}'
            where_condition_target += str(line_target)
            counter =+ 1
        try:
            self._query(f"""
                    begin transaction;

                    DELETE FROM {rs_target} 
                    USING {rs_stage} 
                    WHERE {where_condition_target} 
                    ;
                    
                    INSERT INTO {rs_target}
                    SELECT *
                    FROM {rs_stage};

                    DROP TABLE {rs_stage};
                    end transaction;
                """)
            logging.info(f'UPSERT based on version succesfull for {rs_stage}')
        except Exception as e:
                logging.error('Issue UPSERTING stage into target. Exiting...')
                logging.error(f'message: {e}', exc_info=True)
                sys.exit(1)


    def _upsert_version(self, rs_target, rs_stage, upsert_keys):
        where_condition_target = ''
        counter = 0
        for col in upsert_keys:
            if counter == 0:
                line_target = f'{rs_target}.{col} = {rs_stage}.{col}'
            else:
                line_target = f'AND {rs_target}.{col} = {rs_stage}.{col}'
            where_condition_target += str(line_target)
            counter =+ 1
        try:
            self._query(f"""
                    begin transaction;

                    DELETE FROM {rs_target} 
                    USING {rs_stage} 
                    WHERE {where_condition_target} AND
                        {rs_stage}.version > {rs_target}.version
                    ;
                    
                    INSERT INTO {rs_target}
                    SELECT *
                    FROM {rs_stage};

                    DROP TABLE {rs_stage};
                    end transaction;
                """)
            logging.info(f'UPSERT based on version succesfull for {rs_stage}')
        except Exception as e:
                logging.error('Issue UPSERTING stage into target. Exiting...')
                logging.error(f'message: {e}', exc_info=True)
                sys.exit(1)
    

    def _full_refresh(self, rs_target, rs_stage):
        # delete is slow but it is not possible to do a truncate within an atomic transaction
        try:
            self._query(f"""
                    begin transaction;

                    DELETE FROM {rs_target} 
                    ;
                    
                    INSERT INTO {rs_target}
                    SELECT *
                    FROM {rs_stage};

                    DROP TABLE {rs_stage};
                    end transaction;
                """)
            logging.info(f'FULL REFRESH succesfull for {rs_target}')
        except Exception as e:
                logging.error('Issue REFRESHING target. Exiting...')
                logging.error(f'message: {e}', exc_info=True)
                sys.exit(1)
    
    # TO DO 
    def _append(self):
        None

    def upsert(self, files, rs_target_schema, rs_target_table, upsert_keys, lynx=True, version=False):
        """
            ARGUMENTS
                files: list of parquet files (eg. ['s3://bucket/folder/sub/file.parquet', ...])
                rs_target_schema: redshift target schema (eg. 'ingest)
                rs_target_table: redshift target table (eg. 'cogenius_xxx')
                upsert_keys: list of fields to match records between the source (ie. stage) and target, in order to identify which records already exist
                lynx: boolean if true, datecreated/datemodifed or version will be taken into account (depending on version argument). If false, only upsert keys are taken into account
                            
            RETURNS
                None

            ADDITIONAL INFO
                List of upsert keys can be considered a composite key. If the composite key already exists within the target table, the associated record
                will be deleted and replaced by the new record with the same composite key. 
                Since and older partition can be reprocessed, an additional check is performed on datemodified. Only if the datemodified within the target is 
                < than the datemodified within the partition we're upserting, the record will be deleted from the target
        """
        rs_target = f'{rs_target_schema}.{rs_target_table}'
        rs_stage = f'{rs_target}_TEMP_STAGE'
        self._create_stage_like_target(rs_target, rs_stage)
        self._copy_parquet_into_stage(files, rs_stage)
        if lynx: 
            self._upsert(rs_target, rs_stage, upsert_keys)
        else:
            self._upsert_non_lynx(rs_target, rs_stage, upsert_keys)
        return


    # REFACTOR - add to def upsert() with version boolean (lynx=true, version=true)
    def upsert_version(self, files, rs_target_schema, rs_target_table, upsert_keys):
        """
            ARGUMENTS
                files: list of parquet files (eg. ['s3://bucket/folder/sub/file.parquet', ...])
                rs_target_schema: redshift target schema (eg. 'ingest)
                rs_target_table: redshift target table (eg. 'cogenius_xxx')
                upsert_keys: list of fields to match records between the source (ie. stage) and target, in order to identify which records already exist
                            
            RETURNS
                None

            ADDITIONAL INFO
                List of upsert keys can be considered a composite key. If the composite key already exists within the target table, the associated record
                will be deleted and replaced by the new record with the same composite key. 
                Since and older partition can be reprocessed, an additional check is performed on version. Only if the version within the target is 
                < than the version within the partition we're upserting, the record will be deleted from the target
        """
        rs_target = f'{rs_target_schema}.{rs_target_table}'
        rs_stage = f'{rs_target}_TEMP_STAGE'
        self._create_stage_like_target(rs_target, rs_stage)
        self._copy_parquet_into_stage(files, rs_stage)
        self._upsert_version(rs_target, rs_stage, upsert_keys)
        return

    def full_refresh(self,files, rs_target_schema, rs_target_table):
        """
            ARGUMENTS
                files: list of parquet files (eg. ['s3://bucket/folder/sub/file.parquet', ...])
                rs_target_schema: redshift target schema (eg. 'ingest)
                rs_target_table: redshift target table (eg. 'cogenius_xxx')
                            
            RETURNS
                None

            ADDITIONAL INFO
                The target table will be truncated (ie we're using delete since truncate commits automatically)
                and stage will be copied into target
        """
        rs_target = f'{rs_target_schema}.{rs_target_table}'
        rs_stage = f'{rs_target}_TEMP_STAGE'
        self._create_stage_like_target(rs_target, rs_stage)
        self._copy_parquet_into_stage(files, rs_stage)
        self._full_refresh(rs_target, rs_stage)
        return

     # Not tested yet
    def load_copy_csv(self, s3_bucket, s3_prefix, rs_target, iam_role, delimiter, columns=None, timeformat='YYYY-MM-DDTHH:MI:SS'):
        """
            s3_bucket: bucket
            s3_prefix: path/path/path/
            rs_target: target_schema.target_table
            env: dev or prd
            columns: ['col1', 'col2', ...]

            ColumnA in your data file will map to col1 in rs, columnB in your file will map to col2 in rs.
            Columns >= len(columns in file). You can use this to create a RS table with X cols, a file with X-1 cols and have 1 col in redshift 
            defaulting to a value, for example a loading date (ie. loading date TIMESTAMP default sysdate)
        """
        path = 's3://'+s3_bucket+'/'+s3_prefix
        if columns == None:
            dml = f"""
                COPY {rs_target}
                FROM '{path}'
                CVS
                IAM_ROLE '{iam_role}'
                delimiter '{delimiter}'
                timeformat '{timeformat}'
                EMPTYASNULL
            """
        else: 
            dml = f"""
                COPY {rs_target} ('{",".join(columns)}')
                FROM '{path}'
                CSV
                IAM_ROLE '{iam_role}'
                delimiter '{delimiter}'
                timeformat '{timeformat}'
                EMPTYASNULL
            """
        self._query(dml)
        return

    def rs_to_df(self, dml):
        return pd.read_sql_query(dml, self.conn)

    
    def rs_fetch_single_val(self, query):
        result = None
        with self.conn.cursor() as cur:
            cur.execute(query)
            result = cur.fetchone()
            self.conn.commit()
        return result[0]

    

    def rs_column_to_api_query_param_list(self, rs_source_schema:str, rs_source_table:str, rs_source_column:str, api_query_params:str):
        """
            ARGUMENTS:
                rs_source_schema: eg. ingest
                rs_source_table: eg. table
                rs_source_column: eg column1
                api_query_params: eg objectId in case API endpoint is url/objectId?value1 where value1 is extracted from redshift ingest.table.column1
            RETURNS
                list of stringified json [{'objectId': column1[value1]}, {'objectId': column1[value2]}]

            ADDITIONAL INFO:
                For several APIs we need a redshift column as input. For each cell value we'll fetch a data object from an API via query parameters.
                Hence, we create a list of stringified dictionaries which can be readily passed into a fetch function containing the relevant cell value and
                query parameter.
        """
        dml = f"""
            SELECT "{rs_source_column}" from {rs_source_schema}.{rs_source_table} where "{rs_source_column}" is not null
        """
        df = self.rs_to_df(dml)
        # sometimes we have floats in redshift that should be integers, hence we need to cast to integers.
        float_col = df.select_dtypes(include=['float64']) 
        for col in float_col.columns.values:
            df[col] = df[col].astype('int64')
        input_list= df[rs_source_column].tolist()
        api_queries = [json.dumps({api_query_params: x}) for x in input_list]
        return api_queries

    def rs_column_to_list(self, rs_source_schema:str, rs_source_table:str, rs_source_column:str):
        dml = f"""
            SELECT "{rs_source_column}" from {rs_source_schema}.{rs_source_table} where "{rs_source_column}" is not null
        """
        df = self.rs_to_df(dml)
        # sometimes we have floats in redshift that should be integers, hence we need to cast to integers.
        float_col = df.select_dtypes(include=['float64']) 
        for col in float_col.columns.values:
            df[col] = df[col].astype('int64')
        path_params= df[rs_source_column].tolist()
        return path_params

    def rs_query_to_list(self, query: str, rs_source_column: str):
        df = self.rs_to_df(query)
          # sometimes we have floats in redshift that should be integers, hence we need to cast to integers.
        float_col = df.select_dtypes(include=['float64']) 
        for col in float_col.columns.values:
            df[col] = df[col].astype('int64')
        path_params= df[rs_source_column].tolist()
        return path_params

    def stream_to_s3_parquet(self, query:str, batch_size:int, s3_bucket:str, s3_prefix:str, s3_filename:str) -> None:
        """
            Streams the results of a sql query to parquet files on s3 with 'batch_size' nbr of rows per file. 
            Output files have the following key:
                {s3_bucket}{s3_prefix}{timestamp}_{filename}.parquet        
        """
        for rows, cols in self._stream_results(query, batch_size):
            get_data = [x for x in rows]
            col_names = [y[0] for y in cols]
            df = pd.DataFrame(get_data)
            df.columns = col_names
            self._df_to_parquet_s3(df, s3_bucket, s3_prefix, s3_filename)
        return
      
    def _stream_results(self, query:str, batch_size: int):
        cursor = self.conn.cursor()
        cursor.itersize = batch_size
        cursor.execute(query)
        while True:
            rows = cursor.fetchmany(batch_size)
            cols = cursor.description
            if not rows:
                break
            yield rows, cols
            

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
