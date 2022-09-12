import requests
import json
import random
from time import sleep
import logging
import ast

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
                    logging.warning(f'Trying again ... query: {json.loads(query_params)}')
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
                    logging.warning(f'Trying again ... {endpoint}/{path_params}')
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


