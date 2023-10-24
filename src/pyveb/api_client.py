import requests
import json
import random
from time import sleep
import logging
import ast
import sys, os
from typing import Dict, List, Optional
import pandas as pd
from requests.exceptions import RequestException, HTTPError, Timeout

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

class CogeniusAPI:

    def __init__(self, endpoint):
        """
            Instantiation create a session object to make HTTP requests to a specified cogenius endpoint. Once a CogeniusAPI object is created, we can call its fetch method
            to make individual HTTP requests

            See https://api-veb.lynx.energy/swagger/ui/index#!/ for available endpoints. Ensure that the endpoint prefix and API key are set up in your env. 

        """
        try:
            COGENIUS_API_KEY=os.environ['COGENIUS_API_KEY']
            COGENIUS_API_ENDPOINT = os.environ['COGENIUS_API_ENDPOINT']
            logging.info("Found cogenius credentials in os.environ")
        except Exception as e:
            logging.error("No cogenius credentials found in os.environ. Exiting...")
            logging.error(e)
            sys.exit(1)
        self.endpoint_url = f'{COGENIUS_API_ENDPOINT}{endpoint}'
        logging.info(f'Using endpoing {self.endpoint_url}')
        self.headers = {
            'X-API-KEY' : COGENIUS_API_KEY,
            'Accept': 'application/json'
        } 
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        logging.info('Created requests session and updated Cogenius API KEY to headers')
        return 

    def fetch(self, query, retries=2, backoff_in_seconds=1, timeout=15):
        """
        Fetches from cogenius API endpoint specified during class instantiating. Request parameters are passed via query postional argument
        which expects a stringified dictionary, eg. '{"ean": 12345}'. 

        See https://api-veb.lynx.energy/swagger/ui/index#!/Preswitch/Preswitch_RequestPreSwitchBasic for API definitions

        ARGUMENTS:
            query: stringified dictionary, eg. '{"ean": 12345}'
            retries = 2: optional, number of retries in case of api request failures
            backoff_in_seconds = 1: optional, used to calculate waiting time between retries: (backoff_in_seconds * 2 ** (retry_attempt+1) + random.uniform(0, 1))
            timeout = 15: individual api request will time out after timeout seconds 
       
        RETURNS: 
            response.text object or a RunTimeError is raised in case all attempts fail
        """
        for x in range(retries+1):
            try:
                return self._fetch(query, timeout)
            except RuntimeError as runtime_error:
                if x == retries - 1:
                    # final attempt error is raised only. 
                    # TO DO - put all intermediate errors and raise instead
                    raise RuntimeError(f'Issue fetching: {json.loads(query)}: {runtime_error}') from runtime_error
                sleep_duration = (backoff_in_seconds * 2 ** (x+1) + random.uniform(0, 1))
                sleep(sleep_duration)

    def _fetch(self, query, timeout):
        try:
            response = self.session.get(self.endpoint_url, params=json.loads(query), timeout=timeout)
            response.raise_for_status()  # Check if 2xx response returned
            return json.loads(response.text)
        except HTTPError as http_error:
            raise RuntimeError(f'No response in 2xx range for {json.loads(query)}: {http_error}') from http_error
        except Timeout as timeout_error:
            raise RuntimeError(f'Request timed out for {json.loads(query)}: {timeout_error}') from timeout_error
        except RequestException as request_error:
            raise RuntimeError(f'Request Exception {json.loads(query)}: {request_error}') from request_error
        
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
        # api_params = [json.dumps({k: v for k, v in rec.items() if v is not None}) for rec in df.to_dict('records')]   
        api_params = [json.dumps({k: v for k, v in rec.items()}) for rec in df.to_dict('records')]   

        return api_params

    def fetch(self, params: str, endpoint:str , fetch_type: str, request_timeout = 3, retries=3, backoff_in_seconds=1, **kwargs) -> Dict:
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
                return self._fetch(params, endpoint, fetch_type, request_timeout, **kwargs)
            except RuntimeError as e:
                if x == int(retries)-1:
                    raise RuntimeError(
                        f'No 200 response after {retries} tries for query: {json.loads(params)}: {e}'
                    ) from e
                sleep_duration = (int(backoff_in_seconds) * 2 ** x + random.uniform(0, 1))
                sleep(sleep_duration)
                x += 1

    def _fetch(self, params: str, endpoint:str, fetch_type:str, request_timeout: int, **kwargs) -> Dict:
        """
            params passed prefixed with fk_ will not be part of the api query but will be added to result dictionary. This way,
            the input query and response can be linked via fk_ identifiers
        """

        item_dict = json.loads(params)
        api_params_dic = {k: v for k, v in item_dict.items() if not k.startswith('fk_') and v is not None}
        api_params_nulls_dic = {k: v for k, v in item_dict.items() if not k.startswith('fk_') and v is None}
        fk_params_dic = {k: v for k, v in item_dict.items() if k.startswith('fk_')}

        # Convert the two parts back to JSON-formatted strings
        api_params = json.dumps(api_params_dic, ensure_ascii=False)
        api_params_nulls = json.dumps(api_params_nulls_dic, ensure_ascii=False)
        fk_params = json.dumps(fk_params_dic, ensure_ascii=False)

        try: 
            if fetch_type == 'query':
                url_endpoint = f'{self.API_URL}{endpoint}'
                res = self.session.get(url_endpoint, params = json.loads(api_params), timeout=request_timeout)
            if fetch_type == 'path':
                params_suffix = '/'.join(list(ast.literal_eval(api_params).values()))
                url_endpoint = f'{self.API_URL}{endpoint}/{params_suffix}'
                res = self.session.get(url_endpoint, timeout=request_timeout)
        ## handle timeouts
        except requests.exceptions.Timeout as e:
            raise RuntimeError(f' No response received within {request_timeout} second, request cancelled client-side for: {json.loads(params)}')
        
        ## handle unsuccesful responses
        if res.status_code != 200:
            raise RuntimeError(f' {res.status_code} response for API call with {fetch_type} parameters: {json.loads(params)}')
        
        res_dic=json.loads(res.text)
        res_dic_lit = ast.literal_eval(api_params)
        for k,v in res_dic_lit.items():
            res_dic[f'api_param_{k}'] = v

        if fk_params:
            e = ast.literal_eval(fk_params)
            for k,v in e.items():
                res_dic[k] = v

        if api_params_nulls:
            a = api_params_nulls.replace("null", "None")
            f = ast.literal_eval(a)
            for k,v in f.items():
                res_dic[f'api_param_{k}'] = v

       
        return res_dic

    def close_session(self):
        try:
            self.session.close()
            logging.info("Closed API session")
        except Exception as e:
            logging.error("Error closing API session")
      