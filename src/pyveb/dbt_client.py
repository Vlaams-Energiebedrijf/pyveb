
import logging, sys, os
import json, requests
from dataclasses import dataclass
from datetime import datetime
import pandas as pd
from time import sleep
from typing import List
import hashlib
import inspect
from functools import lru_cache

## testing connection errors
class ConnectionErrorAdapter(requests.adapters.BaseAdapter):
    def send(self, request, stream=False, timeout=None, verify=True, cert=None, proxies=None):
        raise requests.exceptions.ConnectionError("Simulated Connection Error")
    
class RequestExceptionAdapter(requests.adapters.BaseAdapter):
    def send(self, request, stream=False, timeout=None, verify=True, cert=None, proxies=None):
        raise requests.exceptions.RequestException("Simulated Request Exception")

@dataclass
class dbtJob:
    """
    Parses a DBT job object from a dictionary.

    Args:
        obj: The dictionary representing the DBT job object.

    Returns:
        The corresponding `dbtJob` object.
    """
    job_id: str
    job_name: str
    job_description: str
    project_id: str
    environment_id: str
    created_at: datetime
    updated_at: datetime
    state: str
    deactivated: str
    schedule: str
    cron_humanized: str

    def __hash__(self):
        # Create a hashable representation of the object's data fields
        hashable_data = {
            "job_id": self.job_id,
            "state": self.state, 
            'created_at': self.created_at
        }

        # Generate a hash value using hashlib
        hash_object = hashlib.sha256()
        for key, value in hashable_data.items():
            hash_object.update(f"{key}:{value}".encode())

        return int(hash_object.hexdigest(), 16)

@dataclass
class dbtRun:
    """
    Parses a DBT run object from a dictionary.

    Args:
        obj: The dictionary representing the DBT run object.

    Returns:
        The corresponding `dbtRun` object.
    """
    run_id: int
    job_id: int
    environment_id: int
    project_id: int
    status: str
    created_at: datetime
    started_at: datetime
    finished_at: datetime
    in_progress: bool
    is_complete: bool
    is_success: bool
    is_error: bool
    is_cancelled: bool
    duration: str
    queued_duration: str
    run_duration: str
    can_retry: bool
    is_running: bool

    def __hash__(self):
        # Create a hashable representation of the object's data fields
        hashable_data = {
            "run_id": self.run_id,
            "created_at": self.created_at,
            "status": self.status
        }

        # Generate a hash value using hashlib
        hash_object = hashlib.sha256()
        for key, value in hashable_data.items():
            hash_object.update(f"{key}:{value}".encode())

        return int(hash_object.hexdigest(), 16)

@dataclass
class dbtEnvironment:
    """
    Parses a DBT environment object from a dictionary.

    Args:
        obj: The dictionary representing the DBT environment object.

    Returns:
        The corresponding `dbtEnvironment` object.
    """
    environment_id: int
    project_id: int
    connection_id: int
    repository_id: int
    name: str
    custom_branch: str
    updated_at: datetime
    state: int
 
    def __hash__(self):
        # Create a hashable representation of the object's data fields
        hashable_data = {
            "environment_id": self.environment_id,
            "state": self.state, 
            'updated_at': self.created_at
        }

        # Generate a hash value using hashlib
        hash_object = hashlib.sha256()
        for key, value in hashable_data.items():
            hash_object.update(f"{key}:{value}".encode())

        return int(hash_object.hexdigest(), 16)

@dataclass
class dbtRepository:
    """
    Parses a DBT repo object from a dictionary.

    Args:
        obj: The dictionary representing the DBT repo object.

    Returns:
        The corresponding `dbtRepository` object.
    """
    repository_id: int
    project_id: int
    project_name: str
    project_full_name: str
    remote_url: str
    state: int
    created_at: datetime
    updated_at: datetime

    def __hash__(self):
        # Create a hashable representation of the object's data fields
        hashable_data = {
            "repository_id": self.repository_id,
            "state": self.state, 
            'created_at': self.created_at
        }

        # Generate a hash value using hashlib
        hash_object = hashlib.sha256()
        for key, value in hashable_data.items():
            hash_object.update(f"{key}:{value}".encode())

        return int(hash_object.hexdigest(), 16)

@dataclass
class dbtConnection:
    """
    Parses a DBT connection object from a dictionary.

    Args:
        obj: The dictionary representing the DBT connection object.

    Returns:
        The corresponding `dbtConnection` object.
    """
    connection_id: int
    project_id: int
    conn_name: str
    conn_type: str
    conn_host: str
    conn_db: str
    conn_port: int
    state: int

    def __hash__(self):
        # Create a hashable representation of the object's data fields
        hashable_data = {
            "connection_id": self.connection_id,
            "state": self.state, 
        }

        # Generate a hash value using hashlib
        hash_object = hashlib.sha256()
        for key, value in hashable_data.items():
            hash_object.update(f"{key}:{value}".encode())

        return int(hash_object.hexdigest(), 16)

@dataclass
class dbtProject:
    """
    Parses a DBT project object from a dictionary.

    Args:
        obj: The dictionary representing the DBT project object.

    Returns:
        The corresponding `dbtProject` object.
    """
    project_id: int
    project_name: str
    connection_id: int
    repository_id:int
    created_at: datetime
    updated_at:datetime
    state: int
   
    def __hash__(self):
        # Create a hashable representation of the object's data fields
        hashable_data = {
            "project_id": self.project_id,
            "state": self.state, 
            'created_at': self.created_at
        }

        # Generate a hash value using hashlib
        hash_object = hashlib.sha256()
        for key, value in hashable_data.items():
            hash_object.update(f"{key}:{value}".encode())

        return int(hash_object.hexdigest(), 16)

@dataclass
class dbtModel:
    database: str
    schema: str
    model_name: str

    def __hash__(self):
        # Create a hashable representation of the object's data fields
        hashable_data = {
            "database": self.run_id,
            "schema": self.created_at,
            "model_name": self.status
        }

        # Generate a hash value using hashlib
        hash_object = hashlib.sha256()
        for key, value in hashable_data.items():
            hash_object.update(f"{key}:{value}".encode())

        return int(hash_object.hexdigest(), 16)





class AuthType:
    def __init__(self):
        None

    def auth_header(self):
        raise NotImplementedError("Abstract Method")
    
class AuthApiKey(AuthType):
    def __init__(self, **kwargs):
        super().__init__()
        try:  
            self.api_key = os.environ['DBT_API_KEY']
        except KeyError:
            try: 
                self.api_key = kwargs['dbt_api_key']
            except KeyError:
                logging.error('No API key found. Either set up DBT_API_KEY in your environment or pass dbt_api_key = "abc123" when initiating a dbtClient object. Exiting... ')
                sys.exit(1)

    def auth_header(self):
        return {
                'Accept': 'application/json',
                'Authorization': f'Bearer {self.api_key}'
            }
    
class dbtClient():

    DBT_ENDPOINTS = {
        'jobs': 'jobs',
        'runs': 'runs', 
        'projects': 'projects', 
        'environments': 'environments', 
        'connections': 'connections', 
        'repositories': 'repositories'
    }
 
    def __init__(self, base_url:str, api_version:str, account_id:str, env:str='local', auth_type:str = 'api_key', **kwargs):
        """
            Initializes the DBT API client with the specified URL, API version, account ID, and authentication type.

            Args:
                url (str): The base URL of the DBT API.
                api_version (str): The API version to use.
                account_id (str): The ID of the DBT account.
                auth_type (str, optional): The authentication type to use. Defaults to `api_key`.
                **kwargs: Additional keyword arguments.
        """
        if base_url.endswith("/"):
            url = f'{base_url}{api_version}/accounts/{account_id}'
        else:
            url = f'{base_url}/{api_version}/accounts/{account_id}'
        self.url = url
        self.session = requests.Session()
        AUTH_MAPPING = {
            'api_key': AuthApiKey(**kwargs)
        }
        auth = AUTH_MAPPING[auth_type]
        self.headers = auth.auth_header()
        self.session.headers.update(self.headers)
        self.env = env
        # test connection errors
        # self.session.mount("http://", ConnectionErrorAdapter())
        # self.session.mount("https://", ConnectionErrorAdapter())
        ## test request errors
        # self.session.mount("http://", RequestExceptionAdapter())
        # self.session.mount("https://", RequestExceptionAdapter())
        return
    
    @staticmethod
    def parse_dbt_job(obj) -> dbtJob : 
        job_id = obj['id']
        job_name = obj['name']
        job_description = obj['description']
        # job_description = 'testing'
        project_id = obj['project_id']
        environment_id = obj['environment_id']
        created_at = obj['created_at']
        updated_at = obj['updated_at']
        state = obj['state']
        deactivated = obj['deactivated']
        cron_schedule = obj['schedule']['cron']
        cron_humanized = obj['cron_humanized']
        parsed_object = dbtJob(job_id, job_name, job_description, project_id, environment_id, created_at, updated_at, state, deactivated, cron_schedule, cron_humanized)
        return parsed_object
    
    @staticmethod
    def parse_dbt_run(obj) -> dbtRun : 
        run_id = obj['id']
        job_id = obj['job_id']
        environment_id = obj['environment_id']
        project_id = obj['project_id']
        status = obj['status']
        created_at = obj['created_at']
        started_at = obj['started_at']
        finished_at = obj['finished_at']
        in_progress = obj['in_progress']
        is_complete = obj['is_complete']
        is_success = obj['is_success']
        is_error = obj['is_error']
        is_cancelled = obj['is_cancelled']
        duration = obj['duration']
        queued_duration = obj['queued_duration']
        run_duration = obj['run_duration']
        can_retry = obj['can_retry']
        is_running = obj['is_running']

        parsed_object = dbtRun(
            run_id, job_id, environment_id, project_id,
            status, 
            created_at, started_at, finished_at, 
            in_progress, is_complete, is_success, is_error, is_cancelled, 
            duration, queued_duration, run_duration, 
            can_retry, is_running
        )
        return parsed_object
    
    @staticmethod
    def parse_dbt_environment(obj) -> dbtEnvironment:
        environment_id = obj['id']
        project_id = obj['project_id']
        connection_id = obj['connection_id']
        repository_id = obj['repository_id']
        name = obj['name']
        custom_branch = obj['custom_branch']
        updated_at = obj['updated_at']
        state = obj['state']
        parsed_obj = dbtEnvironment(environment_id,project_id, connection_id, repository_id, name, custom_branch, updated_at, state )
        return parsed_obj

    @staticmethod
    def parse_dbt_repository(obj) -> dbtRepository:
        repository_id = obj['id']
        project_id = obj['project_id']
        project_name= obj['name']
        project_full_name = obj['full_name']
        remote_url = obj['remote_url']
        state = obj['state']
        created_at = obj['created_at']
        updated_at = obj['updated_at']

        parsed_obj = dbtRepository(repository_id, project_id, project_name, project_full_name, remote_url, state, created_at, updated_at)
        return parsed_obj

    @staticmethod
    def parse_dbt_connection(obj) -> dbtConnection:
        connection_id = obj['id']
        project_id = obj['dbt_project_id']
        conn_name = obj['name']
        conn_type = obj['type']
        conn_host = obj['hostname']
        conn_db = obj['dbname']
        conn_port = obj['port']
        state = obj['state']

        parsed_obj = dbtConnection(connection_id, project_id, conn_name, conn_type, conn_host, conn_db, conn_port, state)
        return parsed_obj

    @staticmethod
    def parse_dbt_project(obj) -> dbtProject:
        project_id = obj['id']
        project_name = obj['name']
        connection_id = obj['connection_id']
        repository_id = obj['repository_id']
        created_at = obj['created_at']
        updated_at = obj['updated_at']
        state = obj['state']

        parsed_obj = dbtProject(project_id, project_name, connection_id, repository_id, created_at, updated_at, state)
        return parsed_obj

    @staticmethod
    def parse_dbt_model(obj) -> dbtModel : 
        database = obj['database']
        schema = obj['schema']
        model_name = obj['name']
        parsed_object = dbtModel(database, schema, model_name)
        return parsed_object

    def _make_request(self, endpoint, data=None, pagination_params = {}, filter_params = {}):
        """
        Makes a request to the DBT API and handles the response.

        Args:
            endpoint: The endpoint to make the request to.
            data: The data to send in the request body.

        Returns:
            The response data from the API and pagination info
        """
        retries = 3
        retry_delay = 1
        url = f"{self.url}/{endpoint}"

        for retry in range(retries + 1):
            try:
                params_merged = {**pagination_params, **filter_params}
                if data:
                    res = self.session.get(url, json=data, params=json.loads(json.dumps(params_merged)))
                else:
                    res = self.session.get(url, params=json.loads(json.dumps(params_merged)))

                if res.status_code == 200:
                    if inspect.stack()[1].function == 'get_manifest':
                        return res.text
                    res_dict = json.loads(res.text)
                    res_data = res_dict.get('data', None)
                    res.close()
                    if res_data:
                        res_pagination = res_dict.get('extra', None)
                        logging.critical(res_pagination)
                        return res_data, res_pagination
                    else:
                        raise Exception("No valid data found in the response")
                else:
                    raise Exception(f"Request returned the following non-200 response code {res.status_code}")

            ## try again in case of connection networking issues
            except requests.exceptions.ConnectionError as e:
                logging.error(f"Error: Unable to connect to DBT API server: {e}")
                logging.warning(f"Trying again...")
                if retry < retries:
                    sleep(retry_delay * (2 ** retry))
                else:
                    logging.error(f'Max retries reached. Exiting with exit code 1')
                    sys.exit(1)
            ## try again in case of request issues
            except requests.exceptions.RequestException as e:
                logging.error(f"Error: Unable to make request to DBT API: {e}")
                logging.warning(f"Trying again...")
                if retry < retries:
                    sleep(retry_delay * (2 ** retry))
                else:
                    logging.error(f'Max retries reached. Exiting with exit code 1')
                    sys.exit(1)
            ## exit in case of non-200 status or invalid data
            except Exception as e:
                logging.error(f'Failed to fetch valid data, exiting with error code 1: {e}')
                sys.exit(1)

    def _make_paginated_request(self, endpoint: str, limit=100, **kwargs):
        all_records = []
        first_data_batch, pagination_data = self._make_request(endpoint, pagination_params={"limit": limit, "offset": 0}, **kwargs)
        
        for record in  first_data_batch:
            all_records.append(record)

        if self.env == 'local':
            total_records_to_fetch = min(limit*3+1, pagination_data["pagination"]["total_count"])
            logging.warning(f'Limiting records to fetch to: {total_records_to_fetch}')
        else:
            total_records_to_fetch = pagination_data["pagination"]["total_count"]
            logging.warning(f'Total records to fetch: {total_records_to_fetch}')
        
        ## no pagination required
        if total_records_to_fetch <= limit: 
            return all_records
        
        total_fetched_records = limit
        offset = limit

        while True :
            data, _ = self._make_request(endpoint, pagination_params={"limit": limit, "offset": offset}, **kwargs)
            for record in data:
                all_records.append(record)
                total_fetched_records += 1
                if total_fetched_records == total_records_to_fetch:
                    return all_records
            offset += limit

    def get_jobs(self):
        """
            Returns a list of all DBT jobs as dbtJob objects
        """
        jobs_data = self._make_paginated_request(self.DBT_ENDPOINTS['jobs'])
        return [self.parse_dbt_job(job) for job in jobs_data]

    def get_job_by_id(self, job_id):
        jobs_data = self._make_request(f"{self.DBT_ENDPOINTS['jobs']}/{job_id}")
        return self.parse_dbt_job(jobs_data)

    def get_runs(self, created_from = datetime(2020,1,1), created_to = datetime.now()):
        """
            Returns a list of all DBT runs as dbtRun objects
        """
        created_from_formatted = created_from.strftime("%Y-%m-%d %H:%M:%S")
        created_to_formatted = created_to.strftime("%Y-%m-%d %H:%M:%S")
        filter_params = {
            'created_at__range': f'["{created_from_formatted}", "{created_to_formatted}"]'
        }
        logging.critical(f'Using the following filter params {filter_params}')
        runs_data = self._make_paginated_request(self.DBT_ENDPOINTS['runs'], filter_params = filter_params)
        return [self.parse_dbt_run(run) for run in runs_data]

    def get_runs_gt_id(self, id__gt=0):
        """
            Returns a list of all DBT runs as dbtRun objects
        """
        filter_params = {
            'id__gt': id__gt
        }
        logging.critical(f'Using the following filter params {filter_params}')
        runs_data = self._make_paginated_request(self.DBT_ENDPOINTS['runs'], filter_params = filter_params)
        return [self.parse_dbt_run(run) for run in runs_data]
    
    def get_run_by_id(self, run_id):
        """
            Returns a list of all DBT runs as dbtRun objects
        """
        runs_data = self._make_request(f"{self.DBT_ENDPOINTS['runs']}/{run_id}")
        return self.parse_dbt_run(runs_data) 

    def get_environments(self):
        """
            Returns a list of all DBT environments as dbtEnv objects
        """
        envs_data = self._make_paginated_request(self.DBT_ENDPOINTS['environments'])
        return [self.parse_dbt_environment(env) for env in envs_data]
    
    def get_environment_by_id(self, env_id):
        envs_data = self._make_request(f"{self.DBT_ENDPOINTS['environments']}/{env_id}")
        return self.parse_dbt_environment(envs_data)
    
    def get_projects(self):
        """
            Returns a list of all DBT projects as dbtProject objects
        """
        projects_data = self._make_paginated_request(self.DBT_ENDPOINTS['projects'])
        return [self.parse_dbt_project(project) for project in projects_data]
    
    def get_project_by_id(self, project_id):
        projects_data = self._make_request(f"{self.DBT_ENDPOINTS['projects']}/{project_id}")
        return self.parse_dbt_project(projects_data)
    
    def get_connections(self):
        """
            Returns a list of all DBT connections as dbtConnection objects
        """
        connections_data = self._make_paginated_request(self.DBT_ENDPOINTS['connections'])
        return [self.parse_dbt_connection(conn) for conn in connections_data]
    
    def get_connection_by_id(self, connection_id):
        connections_data = self._make_request(f"{self.DBT_ENDPOINTS['connections']}/{connection_id}")
        return self.parse_dbt_connection(connections_data)
    
    def get_repositories(self):
        """
            Returns a list of all DBT repositories as dbtRepository objects
        """
        repositories_data = self._make_paginated_request(self.DBT_ENDPOINTS['repositories'])
        return [self.parse_dbt_repository(repo) for repo in repositories_data]
    
    def get_repository_by_id(self, repository_id):
        repositories_data = self._make_request(f"{self.DBT_ENDPOINTS['repositories']}/{repository_id}")
        return self.parse_dbt_repository(repositories_data)

    @lru_cache(maxsize = None)
    def get_manifest(self, job_id):
        current_accept = self.headers["Accept"]
        self.headers["Accept"] = 'text/html'
        self.session.headers.update(self.headers)
        manifest = self._make_request(f"jobs/{job_id}/artifacts/manifest.json")
        self.headers["Accept"] = current_accept
        self.session.headers.update(self.headers)
        return manifest
    
    @staticmethod 
    def check_duplicates(dataclass_list):
        seen = set()
        for o in dataclass_list:
            if o in seen:
                logging.error(f'found at least one duplicate object: {o}')
                return True
            else:
                seen.add(o)
        return False

    @staticmethod
    def dataclass_list_to_df(dataclass_objects_list):
        serialized_data = []

        for dataclass_object in dataclass_objects_list:
            serialized_data_item = {}

            for field_name, field_value in dataclass_object.__dict__.items():
                serialized_data_item[field_name] = field_value

            serialized_data.append(serialized_data_item)

        return pd.DataFrame(serialized_data)

    def close_session(self):
        try:
            self.session.close()
            logging.info("Closed API session")
        except Exception as e:
            logging.error("Error closing API session")
            raise e





