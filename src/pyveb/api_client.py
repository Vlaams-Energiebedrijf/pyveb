import requests
import json
import random
from time import sleep
import logging
import ast
import sys, os
from typing import Dict, List, Optional
import pandas as pd

class basicAPI():

    def __init__(self, url, auth_type = 'api_key', **kwargs ):
        if url.endswith("/"):
            None
        else:
            url = f'{url}/'
        self.API_URL= url
        self.auth_type = auth_type
        self.api_key = kwargs.get('api_key')
        if self.auth_type == 'api_key':
            self.headers = {
                'X-API-KEY' : self.api_key,
                'Accept': 'application/json'
            } 
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        return 

    def fetch_query_params(self, query_params, endpoint, retries=3, backoff_in_seconds=1):
        """
            ARGUMENTS
                query_params: list of stringified dictionary, eg. '{'ean': 12345}'
                endpoint: endpoint part within API endpoint eg. self.url/V1/endpoint?ean=12345

            RETURNS
                response_dictionary or a runtimeError
            
        """
        x = 0
        while True:
            try:
                return self._fetch_query_params(endpoint, query_params)
            except RuntimeError:
                if x == int(retries)-1:
                    raise RuntimeError(f'No 200 response after {retries} tries for query: {json.loads(query_params)}')
                else:
                    # logging.warning(f'Trying again ... query: {json.loads(query_params)}')
                    sleep_duration = (int(backoff_in_seconds) * 2 ** x + random.uniform(0, 1))
                    sleep(sleep_duration)
                    x += 1

    def fetch_path_params(self, path_params, endpoint, retries=3, backoff_in_seconds=1):
        x = 0
        while True:
            try:
                return self._fetch_path_params(endpoint, path_params)
            except RuntimeError:
                if x == int(retries)-1:
                    raise RuntimeError(f'No 200 response after {retries} tries for {endpoint}/{path_params}')
                else:
                    # logging.warning(f'Trying again ... {endpoint}/{path_params}')
                    sleep_duration = (int(backoff_in_seconds) * 2 ** x + random.uniform(0, 1))
                    sleep(sleep_duration)
                    x += 1

    def _fetch_query_params(self, endpoint, query_params):
        url_endpoint = f'{self.API_URL}{endpoint}'
        response = self.session.get(url_endpoint, params = json.loads(query_params))
        if response.status_code != 200:
            raise RuntimeError(f' {response.status_code} response for {json.loads(query_params)}')
        response_dict=json.loads(response.text)
        # we need to add the query parameter to the response in order to link tables. We add query parameter as api_id
        # for adresmatch, we add straatnaam, huisnr, gemeentenaam en postcode
        d = ast.literal_eval(query_params)
        if len(d)==1:
            (k, v), = d.items()
            response_dict['api_id'] = v
        else:
            for k,v in d.items():
                response_dict[k] = v
        return response_dict

    def _fetch_path_params(self, endpoint, path_params):
        url_endpoint = f'{self.API_URL}{endpoint}/{path_params}'
        response = self.session.get(url_endpoint)
        if response.status_code != 200:
            raise RuntimeError(f' {response.status_code} response for {endpoint}/{path_params}')
        response_dict=json.loads(response.text)
        # we need to add the path parameter to the response
        response_dict['api_id']=path_params
        return response_dict

    def close_session(self):
        try:
            self.session.close()
            logging.info("Closed API session")
        except Exception as e:
            logging.error("Error closing API session")


class CogeniusAPI():
    def __init__(self, endpoint):
        try:
            COGENIUS_API_KEY=os.environ[f'COGENIUS_API_KEY']
            COGENIUS_API_ENDPOINT = os.environ[f'COGENIUS_API_ENDPOINT']
            logging.info("Found cogenius credentials in os.environ")
        except Exception as e:
            logging.error("No cogenius credentials found in os.environ. Exiting...")
            logging.error(e)
            sys.exit(1)
        self.endpoint_url = f'{COGENIUS_API_ENDPOINT}{endpoint}'
        logging.info(self.endpoint_url)
        self.headers = {
            'X-API-KEY' : COGENIUS_API_KEY,
            'Accept': 'application/json'
        } 
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        return 

    def fetch(self, query, retries=3, backoff_in_seconds=1):
        """
            returns response_dictionary or a runtimeError
            query: stringified dictionary, eg. '{'ean': 12345}'
        """
        x = 0
        while True:
            try:
                return self._fetch(query)
            except RuntimeError:
                if x == int(retries)-1:
                    raise RuntimeError(f'No 200 response after {retries} tries for query: {json.loads(query)}')
                else:
                    logging.warning(f'Trying again ... query: {json.loads(query)}')
                    sleep_duration = (int(backoff_in_seconds) * 2 ** x + random.uniform(0, 1))
                    sleep(sleep_duration)
                    x += 1

    def _fetch(self, query):
        response = self.session.get(self.endpoint_url, params = json.loads(query))
        if response.status_code != 200:
            raise RuntimeError(f' {response.status_code} response for {json.loads(query)}')
        response_dict=json.loads(response.text)
        return response_dict

    def close_session(self):
        try:
            self.session.close()
            logging.info("Closed cogenius API session")
        except Exception as e:
            logging.error("Error closing cogenius session")
        

class basisregisterAPI():

    API_VERSION_CONTENT_TYPE_MAP = {
        'v1': 'application/json',
        'v2': 'application/ld+json'
    }

    def __init__(self, url:str, api_version:str, auth_type: Optional[str] = 'api_key', **kwargs ) -> None:
        """
            Since the content_type has changed between version 1 and 2, one class instance can only serve endpoints for a given version
        """
        if url.endswith("/"):
            None
        else:
            url = f'{url}/'
        self.API_URL= f'{url}{api_version}/'
        self.auth_type = auth_type
        self.api_key = kwargs.get('api_key')
        if self.auth_type == 'api_key':
            self.headers = {
                'X-API-KEY' : self.api_key,
                'Accept': basisregisterAPI.API_VERSION_CONTENT_TYPE_MAP[api_version]
            } 
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        return 

    def create_api_params_from_df(self,  df: pd.DataFrame) -> List[str]:
        """
            Method to create a list of json strings containing api params from a df of param columns.

            Given df: 
            +-----+-----+
            | col1| col2|
            +-----+-----+
            |   z1|   a1|
            |   z1|   b2|
            +-----+-----+

            Returns:
            ['{'col1': 'z1', 'col2': 'a1'}', '{...}']
                 

        """
        # we transform df to list of json dictionaries and remove keys if value is None
        api_params = [json.dumps({k: v for k, v in rec.items() if v is not None}) for rec in df.to_dict('records')]   
        return api_params

    def fetch(self, params: str, endpoint:str , fetch_type: str, retries=3, backoff_in_seconds=1, **kwargs) -> Dict:
        """
            ARGUMENTS
                params: json string of params, eg. '{'param1': 12345, 'param2': 'abc'}' 
                endpoint: endpoint suffix eg. url/{endpoint}
                fetch_type: path or query. In case of query, params are passed as params. In case of path, the suffix /param1_value/param2_value/... is added to the endpoint

            RETURNS
                api response object enriched with the input params prefixed with api_param_: eg api_param_param1 : 12345
        """
        assert fetch_type in {'query', 'path'}, f"Invalid fetch type {fetch_type}"
        x = 0
        while True:
            try:
                return self._fetch(params, endpoint, fetch_type, **kwargs)
            except RuntimeError as e:
                if x == int(retries)-1:
                    raise RuntimeError(
                        f'No 200 response after {retries} tries for query: {json.loads(params)}: {e}'
                    ) from e
                sleep_duration = (int(backoff_in_seconds) * 2 ** x + random.uniform(0, 1))
                sleep(sleep_duration)
                x += 1

    def _fetch(self, params: str, endpoint:str, fetch_type:str, **kwargs) -> Dict:
        if fetch_type == 'query':
            url_endpoint = f'{self.API_URL}{endpoint}'
            res = self.session.get(url_endpoint, params = json.loads(params))
        if fetch_type == 'path':
            params_suffix = '/'.join(list(ast.literal_eval(params).values()))
            url_endpoint = f'{self.API_URL}{endpoint}/{params_suffix}'
            res = self.session.get(url_endpoint)
        res_dic=json.loads(res.text)
        d = ast.literal_eval(params)
        for k,v in d.items():
            res_dic[f'api_param_{k}'] = v
        if res.status_code != 200:
            raise RuntimeError(f' {res.status_code} response for API call with {fetch_type} parameters: {json.loads(params)}')
        return res_dic

    def close_session(self):
        try:
            self.session.close()
            logging.info("Closed API session")
        except Exception as e:
            logging.error("Error closing API session")
      
