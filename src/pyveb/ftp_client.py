
import ftplib
import sys
import logging
# import s3_client
import os

class ftpClient():

    def __init__(self, ftp_folder:str, env:str, customer:str, s3_client:any, **kwargs) -> None: 
        """
            ARGUMENTS:
                ftp_folder: in case we need to drop files in a subfolder, ie. url/folder/
                env: local, dev or prd
                bucket: bucket containing files we want to upload to ftp
                customer: esight or eportal

            RETURNS:
                None

            ADDITIONAL INFO:
                currently only tested for esight/eportal flow but should work in general
        """
        if env == 'local':
            try:
                self.url = os.environ[f'{customer.upper()}_URL']
                self.user = os.environ[f'{customer.upper()}_USER']
                self.password = os.environ[f'{customer.upper()}_PASSWORD']
            except Exception as e: 
                logging.error(f'Issue fetchig ftp credentials from local env: {e}')
                sys.exit(1)
        else: 
            try:
                self.url=os.environ['FTP_URL']
                self.user = os.environ['FTP_USER']
                self.password = os.environ['FTP_PASSWORD']
            except Exception as e:
                logging.error(f'Issue fetchig ftp credentials: {e}')
                sys.exit(1)
        self.ftp_folder = ftp_folder
        self.env = env
        self.timeout = kwargs.get('timeout',120)
        self.ftp_session = self._create_ftp_session()
        self.s3client = s3_client
        return None

    def _create_ftp_session(self):
        try:
            ftp_session = ftplib.FTP(self.url, timeout=self.timeout)
            ftp_session.set_debuglevel(2)
            ftp_session.login(self.user, self.password)
            if self.ftp_folder:
                ftp_session.cwd(self.ftp_folder)
        except Exception as e:
            logging.error(f'Issue establishing ftp connection for {self.url}: {e}')
            sys.exit(1)
        return ftp_session

    def close_ftp_session(self):
        try: 
            self.ftp_session.quit()
        except Exception as e:
            logging.error('issue executing ftp_session.quit(), executing hard close ftp_session.close()')
            # https://pd.codechef.com/docs/py/2.7.9/library/ftplib.html
            self.ftp_session.close()

    def upload_to_ftp(self, file):
        memory_csv = self.s3client.download_s3_to_memory(file)
        if self.env == 'prd':
            file_name = 'VEB_'+file.split('/')[-1]
        else:
            file_name = 'VEB_TEST_'+file.split('/')[-1]
        ftp_resp = self.ftp_session.storbinary(f'STOR {file_name}', memory_csv)
        if str(ftp_resp.upper()) != "226 Transfer Complete".upper():
            logging.error(ftp_resp)
        logging.info(ftp_resp)
        return ftp_resp
