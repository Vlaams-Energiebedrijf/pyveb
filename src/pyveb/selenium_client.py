
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
import logging
import sys
import os
import time
import urllib


## We updated the selenium client 2025 april. We had a complex logic for creating the driver in order to ensure chrome and chrome driver were in sync, 
## both locally and in docker file. This was leading to numerous issues so we decided to fix the chrome version and driver in the docker file ( see repository kbo register - src / extract / dockerfile)
## allowing for a simplified setup which works locally
class seleniumClient():

    def __init__(self, env: str, url:str, **kwargs) -> None:
        """
            Initalizes new selenium client. Client allows to download files from webpages. 
            
            env: local, dev or prd. Use local in case of local development in order to correctly create a chromedriver.
            url: url of the page where you want to download a file

            provide download_dir kwarg in case you want to set a specifc directory for downloading files via selenium
        """
        self.env = env
        self.url = url
        self.download_dir = kwargs.get('download_dir','/tmp/downloads')
        self.driver = self._create_driver()
    
    def _set_chrome_options(self) -> None:
        """
            Sets chrome options for Selenium.
            Chrome options for headless browser is enabled.
            based on https://github.com/nazliander/scrape-nr-of-deaths-istanbul/blob/master/app.py
        """
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=800,600")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-infobars")
        options.add_argument("--disable-browser-side-navigation")
        options.add_argument("--disable-features=VizDisplayCompositor")
        # Enable logging
        options.add_argument("--enable-logging")
        options.add_argument("--v=1")
        # Set download directory to /tmp/downloads
        options.add_experimental_option(
            "prefs",
            {
                "download.default_directory": self.download_dir,
                "download.prompt_for_download": False,
                "directory_upgrade": True,
                "safebrowsing.enabled": True,
            },
        )
        return options

    def _create_driver(self):
        options = self._set_chrome_options()
        try:
            if self.env == "local":
                service = Service()  # Uses PATH
            else:
                service = Service("/usr/local/bin/chromedriver")
            driver = webdriver.Chrome(service=service, options=options)
            logging.info("ChromeDriver successfully created.")
            return driver
        except Exception as e:
            logging.error(f"Error creating ChromeDriver: {e}")
            raise


    def _create_local_dir(self, file_name:str, file_extension: str):
        """
            creates local path ./temp_data/
            creates file within local path: 165384.00_file_name.file_extension 

            returns local_path, file 
        """
        logging.info("Creating local directory to temporarily store downloaded files")
        timestamp = round(time.time(), 4)
        file = f'{timestamp}_{file_name}.{file_extension}'
        local_path = './temp_data/'
        if not os.path.exists(local_path):
                os.mkdir(local_path)
        return f'{local_path}{file}'

    def get_file(self, element_type:str, element_name:str, file_name:str, file_extension:str):
        """
            element_type: 
                ID = "id"
                NAME = "name"
                XPATH = "xpath"
                LINK_TEXT = "link text"
                PARTIAL_LINK_TEXT = "partial link text"
                TAG_NAME = "tag name"
                CLASS_NAME = "class name"
                CSS_SELECTOR = "css selector"
            element_name: 
                eg. <element id="element_name_1'/> has element_name = element_name_1 and element_type=ID
            file_name:
                provide a name for the file that will be downloaded
            file_extension: 
                provide the desired extension

            This function creates a local path ./temp_data/ and creates a file within local path: 165384.00_file_name.file_extension
            Next, the driver downloads the specified file via href by accessing the HTML element specified by element_type and element_name. Retry pattern 
            with exponential back of is implemented. 

            Function returns local_file
        """ 
        for i in range(3):
        # retry pattern with some back off
            time.sleep(i*i)
            try:
                local_file = self._create_local_dir(file_name, file_extension)
                self.driver.get(self.url)
                element = self.driver.find_element(getattr(By, element_type), element_name)
                file_url = element.get_attribute('href')
                daily_file = urllib.request.URLopener()
                daily_file.retrieve(file_url, local_file )
                break
            except Exception as e:
                if i < 2:
                    logging.warning(f'Issue downloading file, trying again: {e}')
                    continue
                elif i == 2: 
                    logging.error('Cannot download file. Exiting..')
                    sys.exit(1)
        logging.info(f'Succesfully retrieved file and stored here: {local_file}')
        return local_file
        
    def get_file_via_form_button_xpath(self, selenium_xpath:str, wait_time:int = 10) -> None:
        """
            ARGS: 
                xpath: //form[@id='csvdownload']/button[1]
            
            Donwloads file to temp_data and waits 'wait_time' seconds to complete download
        """ 
        for i in range(3):
        # retry pattern with some back off
            time.sleep(i*i)
            try:
                self.driver.get(self.url)
                # safety wait
                time.sleep(1)
                button = self.driver.find_element(By.XPATH, selenium_xpath )
                button.click()
                # REFACTOR - we need to wait before closing the driver, otherwise we end up with an incomplete crcdownload
                # we should check in temp_data whether the filetype is csv
                time.sleep(wait_time)
                break
            except Exception as e:
                if i < 2:
                    logging.warning(f'Issue downloading file, trying again: {e}')
                    continue
                elif i == 2: 
                    logging.error('Cannot download file. Exiting..')
                    sys.exit(1)
        logging.info('Succesfully retrieved file and stored in ./temp_data/')
        return
    

    def get_href_via_xpath(self, xpath:str, wait_time:int = 10) -> None: 
        """
            Downloads file to temp_data and waits 'wait_time' seconds to complete download.

            ARGS: 
                xpath: //*[text()[contains(.,'SUBSTRING')]]
            
            Specify an xpath that returns a webelement containing a href attribute. The href link gets executed by execute_script function   
        """ 
        for i in range(3):
            time.sleep(i*i)
            try:
                self.driver.get(self.url)
                time.sleep(1)
                link = self.driver.find_element(By.XPATH, xpath)
                self.driver.execute_script('arguments[0].click();', link)
                time.sleep(wait_time)
                break
            except Exception as e:
                if i<2:
                    logging.warning(f'Issue downloading file, trying again: {e}')
                    continue
                if i == 2:
                    logging.error('Cannot download file. Exiting..')
                    sys.exit(1)
        logging.info('Succesfully retrieved file and stored in ./temp_data/')
        return

    def quit(self):
        self.driver.quit()
    

    
