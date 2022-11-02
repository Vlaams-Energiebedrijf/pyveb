from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.common.by import By
import logging
import sys
import os
import time
import urllib

class seleniumClient():

    def __init__(self, env: str, url:str) -> None:
        """
            Initalizes new selenium client. Client allows to download files from webpages. 
            
            env: local, dev or prd. Use local in case of local development in order to correctly create a chromedriver.
            url: url of the page where you want to download a file
        """
        self.env = env
        self.url = url
        self.driver = self._create_driver()
        self.driver.set_window_size(800, 600)

    def _set_chrome_options(self) -> None:
        """
            Sets chrome options for Selenium.
            Chrome options for headless browser is enabled.
            based on https://github.com/nazliander/scrape-nr-of-deaths-istanbul/blob/master/app.py
        """
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_prefs = {
            'download.default_directory' :
            './temp_data/'
        }
        chrome_options.experimental_options["prefs"] = chrome_prefs
        chrome_prefs["profile.default_content_settings"] = {"images": 2}
        return chrome_options

    def _create_driver(self):
        logging.info("Creating chrome driver...")
        try: 
            if self.env == 'local':
            # chrome iterates very quickly, hence chrome and chromedriver diverge. Hence, when running local, we can ensure match between both with below code
                from webdriver_manager.chrome import ChromeDriverManager
                driver = webdriver.Chrome(ChromeDriverManager().install(), options=self._set_chrome_options())
            else:
                # driver and chrome are installed in docker image so we can use standard way of creating webdriver
                driver = webdriver.Chrome(options=self._set_chrome_options())
        except Exception as error: 
            logging.error('Issue creating chrome driver. Exiting with status 1')
            logging.error(error)
            sys.exit(1)
        return driver

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
        
    def get_file_via_form_button_xpath(self, selenium_xpath:str ):
        """
            ARGS: 
                xpath: //form[@id='csvdownload']/button[1]
            
            Donwloads file to temp_data and waits 10 seconds to complete download
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
                time.sleep(10)
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
      
    def quit(self):
        self.driver.quit()
    

    

  