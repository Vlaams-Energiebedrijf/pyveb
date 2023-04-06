from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File 
import os
import io
from io import StringIO
import json
import pandas as pd
import boto3
import logging
from datetime import datetime
from dataclasses import dataclass
from typing import List, Dict
from office365.runtime.client_request_exception import ClientRequestException
from office365.sharepoint.client_context import ClientContext
import tempfile


@dataclass
class sharepointFile:
    name: str
    last_modified_date: datetime
    creation_date: datetime
    url: str
    uri: str
    version: str
    ServerRelativeUrl: str

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

class sharepointClient():

    VEB_URL = 'https://vlaamsenergiebedrijf.sharepoint.com/leveringen/Marktwerking'

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
        self.ctx = ClientContext(site_url).with_credentials(self.user_credentials)
        logging.info(f'Successfully established connection to sharepoint: {site_url}')
        return

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
        parsed_files = [parse_sharepoint_file_object(f.properties) for f in files]
        return parsed_files
    
    def download_file(self, file):
        File.download()
        with tempfile.TemporaryDirectory() as local_path:
            file_name = os.path.basename(file)
            with open(os.path.join(local_path, file_name), 'wb') as local_file:
                file = File.from_url(file).with_credentials(self.user_credentials).download(local_file).execute_query()
            print("'{0}' file has been downloaded into {1}".format(file.serverRelativeUrl, local_file.name))

    def download_rel_url(self, url):
        download_path = os.path.join(tempfile.mkdtemp(), os.path.basename(url))
        with open(download_path, "wb") as local_file:
            _ = self.ctx.web.get_file_by_server_relative_url(url).download(local_file).execute_query()
            #file = ctx.web.get_file_by_server_relative_url(file_url).download(local_file).execute_query()
        print("[Ok] file has been downloaded into: {0}".format(download_path))

    def download(self, file):
        current_file = File.open_binary(self.ctx, file.serverRelativeUrl)

if __name__=='__main__':

    site_url = 'https://vlaamsenergiebedrijf.sharepoint.com/leveringen/Marktwerking'
    folder_prefix = 'Facturatie/B&O Facturatie'
    sp = sharepointClient(site_url)
    files = sp.list_files(folder_prefix)
    for f in files: print(f)
    # logging.warning('downloading file')
    # def remove_prefix(text, prefix):
    #     return text[text.startswith(prefix) and len(prefix):]
    
    # url = remove_prefix(files[0].uri, f'{site_url}/')
    # print(url.split('?')[0])
    # sp.download_rel_url(url)
    sp.download(f.rel_url)

    # https://vlaamsenergiebedrijf.sharepoint.com/leveringen/Marktwerking/Facturatie/B&O%20Facturatie/Allocatie%20EL%202022%20V1.xlsb
 