from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File 
from office365.runtime.client_request_exception import ClientRequestException

import os, io, logging, difflib, sys
from datetime import datetime
import time
from dataclasses import dataclass
from typing import List

from pyveb.s3_client import s3Client
from pyveb.custom_decorators import retry

@dataclass
class sharepointFile:
    name: str
    last_modified_date: datetime
    creation_date: datetime
    url: str
    uri: str
    version: str
    relative_url: str

class sharepointClient():

    def __init__(self, site_url ) -> None:
        """
            Initiate a new sharepoint connection to site_url. In general the site_url is the toplevel 'sitepage' one level below the 'forms' or 'document libraries' you're interested in.

            eg. 
            
            IF you want to connect to 'Facturatie' folder which is a 'forms' element with URL : https://vlaamsenergiebedrijf.sharepoint.com/leveringen/Marktwerking/Facturatie/Forms/Alle%20documenten.aspx
            THEN the toplevel sitepage is https://vlaamsenergiebedrijf.sharepoint.com/leveringen/Marktwerking/SitePages/Index.aspx 
            In this case, the site_url you need to specify is 'https://vlaamsenergiebedrijf.sharepoint.com/leveringen/Marktwerking'

            LOCAL:  Ensure you've setup SHAREPOINT_USER & SHAREPOINT_PASSWORD env variables ( for actual credentials see AWS arn:aws:secretsmanager:eu-west-1:308089413519:secret:office365/data@veb.be-hsgqh6 )
            DEV/PRD: ensure SHAREPOINT_USER & SHAREPOINT_PASSWORD  are injected into container via entrypoint.sh 
         """
        try:
            user=os.environ['SHAREPOINT_USER']
            password = os.environ['SHAREPOINT_PASSWORD']
            logging.info('Found sharepoint credentials environment variables')
        except KeyError as e:
            logging.error('Couldnot find the required environment variables: SHAREPOINT_USER & SHAREPOINT_PASSWORD. Ensure they;re setup locally or injected into docker container via entrypoint.sh ')
        self.user_credentials = UserCredential(user, password)
        try:
            self.ctx = ClientContext(site_url).with_credentials(self.user_credentials)
            logging.info(f'Successfully established connection to sharepoint: {site_url}')
        except ClientRequestException as e:
            logging.error(f'Issue establishing connection to sharepoint: {site_url}. Exiting...')
            sys.exit(1) 
        return

    @staticmethod
    def parse_sharepoint_file_object(obj) -> sharepointFile : 
        name = obj['Name']
        last_modified_date = obj['TimeLastModified']
        creation_date=obj['TimeCreated']
        url = obj['LinkingUri']
        uri = obj['LinkingUrl']
        version = str(obj['MajorVersion'])+'.'+str(obj['MinorVersion'])
        relative_url = obj['ServerRelativeUrl']
        sp = sharepointFile(name, last_modified_date, creation_date, url, uri, version, relative_url)
        return sp

    def list_files(self, folder_prefix: str) -> List[sharepointFile]:
        """
        List all files in a folder/subfolder/subfolder. The toplevel folder needs to be a Sharepoint 'FORMS' element

        e.g.
        IF folder URL is https://vlaamsenergiebedrijf.sharepoint.com/leveringen/Marktwerking/Facturatie/Forms/Alle%20documenten.aspx 
        THEN folder_prefix = 'Facturatie'

        IF folder URL is https://vlaamsenergiebedrijf.sharepoint.com/leveringen/Marktwerking/Facturatie/Forms/Alle%20documenten.aspx?id=%2Fleveringen%2FMarktwerking%2FFacturatie%2FB%26O%20Facturatie&viewid=d82816a0%2D29b3%2D433d%2Db6de%2D9585c8984bd9
        THEN folder_prefix = 'Facturatie/B&O Facturatie

        ARGUMENTS: 
            folder_prefix: eg. Facturatie/B&O Facturatie

            - prefix can contain spaces
            - if prefix only contains subfolders and no files None will be returned

        RETURNS

            list of files as namedTuple(sharepointFile, [name, last_modified, creation_date, url, uri, version])
        """
        libraryRoot = self.ctx.web.get_folder_by_server_relative_url(folder_prefix)
        self.ctx.load(libraryRoot).execute_query()
        files = libraryRoot.files
        self.ctx.load(files).execute_query()
        parsed_files = [self.parse_sharepoint_file_object(f.properties) for f in files]
        return parsed_files
    
    def match_filename(self, list_files: List[sharepointFile], file_name: str) -> sharepointFile:
        files_found = [f.name for f in list_files]
        logging.warning(f'All files in folder: {files_found}')
        best_match = difflib.get_close_matches(file_name, files_found,1)
        logging.warning(f'Closest match found: {best_match[0]} for orginal file name {file_name}')
        best_file = [f for f in list_files if f.name == best_match[0]]
        return best_file[0]

    @retry(retries=3, error="Error download sharepoint file to s3")
    def download_to_s3(self, sharepoint_folder_prefix:str, sharepoint_file_name:str, s3_prefix:str, s3_bucket: str ='veb-data-pipelines', **kwargs) -> None:
        """
            Downloads a sharepoint file to the provided s3 prefix and bucket. 

            ARGUMENTS:
                -sharepoint_folder_prefix:  
                    e.g.
                        IF folder URL is https://vlaamsenergiebedrijf.sharepoint.com/leveringen/Marktwerking/Facturatie/Forms/Alle%20documenten.aspx 
                        THEN folder_prefix = 'Facturatie'

                        IF folder URL is https://vlaamsenergiebedrijf.sharepoint.com/leveringen/Marktwerking/Facturatie/Forms/Alle%20documenten.aspx?id=%2Fleveringen%2FMarktwerking%2FFacturatie%2FB%26O%20Facturatie&viewid=d82816a0%2D29b3%2D433d%2Db6de%2D9585c8984bd9
                        THEN folder_prefix = 'Facturatie/B&O Facturatie

                - s3_prefix: folder/subfolder/
                - s3_bucket: default veb-data-pipelines
        """
        files = self.list_files(sharepoint_folder_prefix)
        file = self.match_filename(files, sharepoint_file_name)
        current_file = File.open_binary(self.ctx, file.relative_url)
        bytes_file_obj = io.BytesIO()
        bytes_file_obj.write(current_file.content)
        bytes_file_obj.seek(0)
        s3 = s3Client(s3_bucket)
        file_name = file.name.replace(' ', '_')
        if s3_prefix.endswith('/'):
            key = f'{s3_prefix}{file_name}'
        else:
            key = f'{s3_prefix}/{file_name}'
        s3.client.put_object(Body=bytes_file_obj, Bucket=s3_bucket, Key=key)
        logging.info(f'Wrote {file.name} to {s3_bucket}/{s3_prefix}/{file_name}')
        return
        
    def upload_to_sharepoint(self, file, sharepoint_folder_prefix:str, file_name:str, file_extension:str, file_suffix_type:str =None, **kwargs) -> str:
        """
            Uploads a bytesobject or a local file to sharepoint. 

            ARGUMENTS 
                -sharepoint_folder_prefix:  
                    e.g.
                        IF folder URL is https://vlaamsenergiebedrijf.sharepoint.com/leveringen/Marktwerking/Facturatie/Forms/Alle%20documenten.aspx 
                        THEN folder_prefix = 'Facturatie'

                        IF folder URL is https://vlaamsenergiebedrijf.sharepoint.com/leveringen/Marktwerking/Facturatie/Forms/Alle%20documenten.aspx?id=%2Fleveringen%2FMarktwerking%2FFacturatie%2FB%26O%20Facturatie&viewid=d82816a0%2D29b3%2D433d%2Db6de%2D9585c8984bd9
                        THEN folder_prefix = 'Facturatie/B&O Facturatie

                        IF folder URL is https://vlaamsenergiebedrijf.sharepoint.com/data/Gedeelde%20%20documenten/Forms/AllItems.aspx?id=%2Fdata%2FGedeelde%20%20documenten%2Fwerkmap&viewid=e1053b37%2D7bf0%2D4709%2Db252%2D845a5a3773c4
                        THEN folder_prefix = 'Gedeelde  documenten/werkmap'

                - file_name: name of the object/file on sharepoint, eg. terra_extract
                - file_extension: file extension, eg. .xlsx
                - file_suffix_type: optional. Will add a dynamic suffix to the file name. eg current_date will add 2022_03_07 to the file name

                ==> complete file_name will be: terra_extract_2020-03-07.xlsx

            RETURNS
                url of the file 

        """
        valid_suffixes = ['current_date', 'unix_timestamp', None]
        if file_suffix_type not in valid_suffixes:
            raise ValueError(f'Invalid file suffix provided in config. Accepted values are: {valid_suffixes}')
        if file_suffix_type == 'current_date':
            today = datetime.now()
            formatted_date = today.strftime('%Y-%m-%d')
            target_suffix = formatted_date
        if file_suffix_type == 'unix_timestamp':
            target_suffix = int(time.time())
        if file_suffix_type:
            target_name = f'{file_name}_{target_suffix}.{file_extension}'
        else: 
            target_name = f'{file_name}.{file_extension}'
        target_folder = self.ctx.web.get_folder_by_server_relative_url(sharepoint_folder_prefix)
        target_file = target_folder.upload_file(target_name, file).execute_query()
        logging.info(f'Uploaded file to {target_file.serverRelativeUrl}')
        return target_file.serverRelativeUrl
        