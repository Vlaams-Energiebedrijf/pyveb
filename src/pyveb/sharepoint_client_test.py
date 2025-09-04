import sys
import os

# Add the folder containing your local pyveb package to sys.path
local_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, local_path)

from pyveb.sharepoint_client import sharepointClient
from pyveb.s3_client import s3Client

if __name__ == "__main__":

    url = "https://vlaamsenergiebedrijf.sharepoint.com/sites/Levering"
    aws_secret = "office365/sharepoint_file_access"

    sp = sharepointClient(url, aws_secret)


    # folder = "Shared Documents"
    folder = "Facturatie/B&O Facturatie"

    files = sp.list_files(folder)


    for fi in files:
        print(fi)


    ###3


    url = 'https://vlaamsenergiebedrijf.sharepoint.com/sites/ITTeam'
    
    s3 = s3Client('veb-data-pipelines')

    sp_2 = sharepointClient(url, aws_secret)

    processed_files = s3.list_files('development/export_eportal/full_refresh/raw/export_eportal/year=2023/month=12/day=21', 'csv')

    # there should only be a single file 
    file_bytes = s3.download_s3_to_memory(processed_files[0])
    sp_2.upload_to_sharepoint(
        file_bytes, 
        'Terra dumps/Local', 
        'test_client_credentials',
        'csv',
        file_suffix_type= 'current_date'
    )


    

