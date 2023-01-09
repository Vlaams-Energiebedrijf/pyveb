import boto3
from urllib.parse import unquote
import os, sys, time
import logging
from io import BytesIO
import pandas as pd

class s3Client():

    def __init__(self, s3_bucket:str) -> None:
        self.bucket_name = s3_bucket
        self.bucket = self._create_bucket_resource()
        self.client = self._create_client()

    def _create_client(self):
        logging.info('Creating boto3 s3 client')
        s3_client = boto3.client("s3")
        logging.info('Succesfully created s3 client')
        return s3_client

    def _create_bucket_resource(self):
        logging.info('Creating boto3 s3 bucket resource')
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self.bucket_name)
        logging.info('Succesfully created bucket resource')
        return bucket

    def list_files(self, s3_prefix:str, file_type:str, max_files=1000000000000, list_empty_files=True) -> list:
        """
            ARGUMENTS
                s3_prefix: folder/subfolder/
                file_type: eg. csv, xlsx, parquet
                max_files: optional, unless max_files is specified, all files are returned. 
                list_empty_files: if False, files with size 0 are not listed

            RETURNS 
                List of files from s3_prefix, filtering by type. Files are fetched per chunks of 1000. 
            
            ADDITIONAL INFO 
            Files are automatically unquoted and returned as full path ( ie. s3:://bucket/s3_prefix/filename.extension)
            
            List_empty_files kwarg is set to True by default to enable backwards compatibility. 
        """
        kwargs = {
            "Bucket": self.bucket_name,
            "Delimiter": "|",
            "EncodingType": "url",
            "MaxKeys": 1000,
            "Prefix": s3_prefix,
            "FetchOwner": False
        }
        truncated = True
        files = []
        counter = 0
        while truncated and counter < max_files:
            counter += 1000
            response = self.client.list_objects_v2(**kwargs)
            truncated = response["IsTruncated"]
            if "Contents" in response:
                if list_empty_files:
                    files.extend(record["Key"] for record in response["Contents"])
                else:
                    files.extend(record["Key"] for record in response["Contents"] if record["Size"] > 0)
                if truncated:
                    kwargs["ContinuationToken"] = response["NextContinuationToken"]
        files = files[:max_files]
        files = [x for x in files if file_type in x]
        files = [unquote(x) for x in files]
        files = ['s3://'+self.bucket_name+'/'+x for x in files]
        return files

    def list_files_bigger_than(self, s3_prefix:str, file_size_bytes:int = 10000000, file_type:str = 'parquet') -> list:
        """
            ARGUMENTS
                s3_prefix: folder/subfolder/
                file_size_bytes: optional, eg. 10000000 (note: 10 000 000 = 10mb)
                file_type: optional, eg. csv, xlsx, parquet
            RETURNS 
                List of files from filtered by type and strictly bigger than file_size_bytes
        """
        big_files = []  
        for file in self.bucket.objects.filter(Prefix=s3_prefix):
            if file.size > file_size_bytes and f'.{file_type}' in file.key:
                file_name = f's3://{self.bucket_name}/{file.key}'  
                big_files.append(file_name)
        return big_files

    def _delete_prefix(self, s3_prefix:str ) -> None:
        """
            Delete all objects with a certain prefix
        """
        logging.info(f'Starting delete operation for prefix {s3_prefix}')
        self.bucket.objects.filter(Prefix=s3_prefix).delete()
        logging.info(f'Succesfully delete all objects for prefix {s3_prefix}')
        return
    
    def _prefix_exist_and_not_empty(self, s3_prefix:str) -> bool:
        '''
            S3_prefix should exist. 
            S3_prefix should not be empty.

            Returns boolean
        '''
        if not s3_prefix.endswith('/'):
            s3_prefix += '/'
        resp = self.client.list_objects(Bucket=self.bucket_name, Prefix=s3_prefix, Delimiter='/', MaxKeys=1)
        return 'Contents' in resp, 'CommonPrefixes' in resp

    def delete_prefix_if_exist(self, s3_prefix:str) -> None:
        """
            ARGUMENTS
                s3_prefix: folder/subfolder/

            RETURNS 
                None

            ADDITIONAL INFO
                Checks whether the prefix contains at least 1 file or folder. If not, prefix is considered non-existent.
                If yes, prefix ( ie all folders, subfolders and files) will be deleted. 
        """
        try: 
            data_exist, subdir_exist = self._prefix_exist_and_not_empty(s3_prefix)
            if data_exist or subdir_exist:
                self._delete_prefix(s3_prefix)
                logging.warning(f'Found and deleted prefix {s3_prefix}.')
            else:
                logging.warning(f'Partition {s3_prefix} does not exist and will be created')
        except Exception as e:
            logging.error('Issue checking if prefixs exists already and/or deleting. Exiting...')
            logging.error(e)
            sys.exit(1)
        return

    def upload_local_file(self, local_file:str, s3_prefix: str) -> None:
        """
            ARGUMENTS
                local_file: ./data/file.ext
                s3_prefix: target_folder/target_subfolder

            RETURNS
                None

            ADDITIONAL INFO
                Use this function to upload a single file from local to s3. The local filename will be retained. 
        """
        target_file_name = local_file.split('/')[-1]
        target_file = f'{s3_prefix}{target_file_name}'
        try: 
            logging.info('Uploading file ...')
            self.client.upload_file(local_file, self.bucket_name, target_file)
            logging.info(f'Uploaded {local_file} to {target_file}')
        except Exception as e: 
            logging.error(f'Issue uploading {local_file} to {target_file}')
            logging.error(e)
            sys.exit(1)
        return

    def df_to_parquet(self, df: pd.DataFrame, s3_prefix:str, s3_file_name:str, add_timestamp:bool = True ) -> None:
        """
            ARGUMENTS
                df: pandas dataframe
                s3_prefix: target_folder/target_subfolder
                s3_file_name: name of the file without extension.  
                add_timestamp: if True, s3_file_name will be prefxied with a timestamp

            RETURNS
                None

            ADDITIONAL INFO
                DF will be read into memory and stored in S3 as parquet under key s3_prefix/1562388.0020_s3_file_name.parquet
        """
        logging.info('Starting upload')
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, index=False, allow_truncated_timestamps=True)
        if add_timestamp:
            timestamp = round(time.time(), 4)
            s3_key = f'{s3_prefix}{timestamp}_{s3_file_name}.parquet'
        else:
            s3_key = f'{s3_prefix}{s3_file_name}.parquet'
        parquet_buffer.seek(0)
        self.bucket.upload_fileobj(parquet_buffer, s3_key)
        logging.info(f'Uploaded dataframe to {s3_key}')
        return

    def df_to_csv_s3(self, df: pd.DataFrame, s3_prefix:str, s3_file_name:str, delimiter=",") -> None:
        """
            ARGUMENTS
                df: pandas dataframe
                s3_prefix: target_folder/sub_folder
                s3_file_name: name of the file without extension. A timestamp will be added within the function. Eg, file_name = pipeline_name
                delimiter: df will be converted to a csv file with the provided delimiter
            
            RETURNS
                None

            ADDITIONAL INFO
                Dataframe will be read into memory and stored in S3 as parquet under key s3_prefix/1562388.0020_s3_file_name.csv
        """
        timestamp = round(time.time(), 4)
        csv_buffer = BytesIO()
        df.to_csv(csv_buffer, index=False, sep=delimiter)
        s3_key = f'{s3_prefix}{timestamp}_{s3_file_name}.csv'
        csv_buffer.seek(0)
        self.bucket.upload_fileobj(csv_buffer, s3_key)
        return 

    def download_s3_to_local_file(self, file:str, file_extension = 'parquet', download_path='./data') -> str:
        if not download_path.endswith('/'):
            download_path = f'{download_path}/'
        timestamp = round(time.time(), 4)
        local_file = f'{download_path}/{timestamp}.{file_extension}'
        if not os.path.exists(download_path):
            os.mkdir(download_path)
        file = file.split('//')[1].split('/',1)[1]
        try:
            self.bucket.download_file(file, local_file)
        except Exception as e:
            logging.error(f'Issue downloading {file}. Exiting...')
            logging.error(e)
            sys.exit(1)
        return local_file

    def download_s3_to_memory(self, file:str):
        """
            !! only tested for csv files 

            Returns: 
                filestream object with pointer at position 0
        """
        try: 
            file = file.split('//')[1].split('/',1)[1]
            obj = self.bucket.Object(file)
            file_stream = BytesIO()
            obj.download_fileobj(file_stream)
            # https://stackoverflow.com/questions/61690731/pandas-read-csv-from-bytesio
            file_stream.seek(0)
        except Exception as e:
            logging.error(f'Issue downloading {file} to memory stream. Exiting...')
            logging.error(e)
            sys.exit(1)
        return file_stream



class externalS3Client():

    def __init__(self, aws_access_key_id_name:str, aws_secret_access_key_name:str , ext_bucket:str):
        """ 
            ARGS
                aws_access_key_id_name: environment variable holding aws_access_key_id
                aws_secret_access_key_name: environment variable holding aws_secret_access_key_name
                ext_bucket: external bucket name without 's3://" eg, ext_bucket_name
        
        """
        self.bucket = ext_bucket
        try:
            aws_access_key_id=os.environ[aws_access_key_id_name]
            aws_secret_access_key = os.environ[aws_secret_access_key_name]
        except Exception: 
            logging.error('AWS credentials for external account not found. Exiting...')
            sys.exit(1)

        session_ext = boto3.Session(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key
        )
        self.s3 = session_ext.resource('s3')

    def put_object(self, s3_prefix:str, local_file:str, ext_file_name:str):
        """ 
            puts an object inside the external bucket
            inside the specified directory, with the specified filename
        """
        data = open(local_file, 'rb')
        try:
            self.s3.Bucket(self.bucket).put_object(Key=f"{s3_prefix}/{ext_file_name}", Body=data)
            logging.info(f"stored in external s3 bucket: {self.bucket}")
        except Exception as e:
            logging.error(f"Could not store file: {ext_file_name} in external s3 bucket")
            logging.error(e)
            sys.exit(1)

