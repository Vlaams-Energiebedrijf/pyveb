
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

    def list_files(self, s3_prefix:str, file_type:str, max_files=1000000000000) -> list:
        """
            ARGUMENTS
                s3_prefix: folder/subfolder/
                file_type: eg. csv, xlsx, parquet
                max_files: optional, unless max_files is specified, all files are returned. 

            RETURNS 
                List of files from s3_prefix, filtering by type. Files are fetched per chunks of 1000. 
            
            ADDITIONAL INFO 
            Files are automatically unquoted and returned as full path ( ie. s3:://bucket/s3_prefix/filename.extension)
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
                for record in response["Contents"]:
                    filename = record["Key"]
                    files.append(filename)
                if truncated:
                    kwargs["ContinuationToken"] = response["NextContinuationToken"]
        files = files[:max_files]
        files = [x for x in files if file_type in x]
        files = [unquote(x) for x in files]
        files = ['s3://'+self.bucket_name+'/'+x for x in files]
        return files

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
            s3_prefix = s3_prefix+'/' 
        resp = self.client.list_objects(Bucket=self.bucket_name, Prefix=s3_prefix, Delimiter='/', MaxKeys=1)
        return 'Contents' in resp

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
            folder_exists = self._prefix_exist_and_not_empty(s3_prefix)
            if folder_exists:
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
            logging.info(f'Uploading file ...')
            self.client.upload_file(local_file, self.bucket_name, target_file)
            logging.info(f'Uploaded {local_file} to {target_file}')
        except Exception as e: 
            logging.error(f'Issue uploading {local_file} to {target_file}')
            logging.error(e)
            sys.exit(1)
        return

    def df_to_parquet(self, df: pd.DataFrame, s3_prefix:str, s3_file_name ) -> None:
        """
            ARGUMENTS
                df: pandas dataframe
                s3_prefix: target_folder/target_subfolder
                s3_file_name: name of the file without extension. A timestamp will be added within the function. 

            RETURNS
                None

            ADDITIONAL INFO
                DF will be read into memory and stored in S3 as parquet under key s3_prefix/1562388.0020_s3_filename.parquet
        """
        timestamp = round(time.time(), 4)
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, index=False, allow_truncated_timestamps=True)
        s3_key = f'{s3_prefix}{timestamp}_{s3_file_name}.parquet'
        parquet_buffer.seek(0)
        self.bucket.upload_fileobj(parquet_buffer, s3_key)
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