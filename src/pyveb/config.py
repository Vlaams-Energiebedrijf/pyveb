import yaml
import os
import sys, inspect
import logging
# from attrdict import AttrDict   # https://pypi.org/project/attrdict/
from pathlib import Path
from datetime import datetime

from collections import UserDict
from datetime import datetime
from dateutil import parser

# class AttrDict(UserDict):
#     def __getattr__(self, key):
#         return self.__getitem__(key)
#     def __setattr__(self, key, value):
#         if key == "data":
#             return super().__setattr__(key, value)
#         return self.__setitem__(key, value)


class AttrDict(UserDict):
    def __getattr__(self, key):
        try:
            return AttrDict(self.data[key]) if isinstance(self.data[key], dict) else self.data[key]
        except KeyError:
            raise AttributeError(f"'AttrDict' object has no attribute '{key}'")

    def __setattr__(self, key, value):
        if key == "data":
            return super().__setattr__(key, value)
        self.data[key] = value

    def __repr__(self):
        return repr(self.data)

def search_upwards_for_file(filename):
    """Search in the current directory and all directories above it 
    for a file of a particular name.
    Arguments:
    ---------
    filename :: string, the filename to look for.
    Returns
    -------
    pathlib.Path, the location of the first file found or
    None, if none was found
    """
    d = Path.cwd()
    logging.info(f'Current Directory: {d}')
    root = Path(d.root)
    logging.info(f'Root: {root}')
    src_path = d / 'src' / filename
    logging.info(f'Looking in {src_path}')
    if src_path.exists():
        return src_path
    while d != root:
        attempt = d / filename
        logging.info(f'Looking in {attempt}')
        if attempt.exists():
            return attempt
        d = d.parent
    return None


def clean_airflow_date(execution_date):
    if isinstance(execution_date, datetime):
        return execution_date.replace(microsecond=0).replace(tzinfo=None)

    elif isinstance(execution_date, str):
        dt = parser.parse(execution_date)
        dt = dt.replace(tzinfo=None)
        return dt.replace(microsecond=0)

    else:
        raise ValueError("Invalid input type. Must be a string or datetime.")

def create_partition_key(execution_date:datetime, partition_granularity:str = 'day') -> str:
    """
        ARGUMENT
            partition_date: date of airflow task start date. eg 2020-01-01 or 2020-01-01T00:00:00+00:00 

        kwargs: 

            partition_granularity ['day', 'hour', 'minute', 'second']

                if day: "year={year}/month={month}/day={day}/"   
                if hour: "year={year}/month={month}/day={day}/hour={hour}/"  
                if minute: "year={year}/month={month}/day={day}/hour={hour}/minute={minute}/"  
                if second: "year={year}/month={month}/day={day}/hour={hour}/minute={minute}/second={second}/"  
        
    """
    day = '{:02d}'.format(execution_date.day)
    month = '{:02d}'.format(execution_date.month)
    year = execution_date.year
    hour = '{:02d}'.format(execution_date.hour)
    minute = '{:02d}'.format(execution_date.minute)
    second = '{:02d}'.format(execution_date.second)

    if partition_granularity == 'day':
        return f"year={year}/month={month}/day={day}/"
    if partition_granularity == 'hour':
        return f"year={year}/month={month}/day={day}/hour={hour}/"
    if partition_granularity == 'minute':
        return f"year={year}/month={month}/day={day}/hour={hour}/minute={minute}/"
    if partition_granularity == 'second':
        return f"year={year}/month={month}/day={day}/hour={hour}/minute={minute}/second={second}/"
    else:
        logging.error('Provided incorrect partition granularity. Allowed values: day, hour, minute or second')


class Config():

    CONFIG_NAME = 'config.yml'
    CONFIG_PATH_AWS = f'/app/{CONFIG_NAME}'
    REQUIRED_GENERAL_KEYS = ['pipeline_name', 'pipeline_bucket', 'pipeline_type', 'prefix_env', 'prefix_logs', 'prefix_raw', 'prefix_processed', 'tasks']

    def __init__(self, **kwargs):
        self.env = kwargs.get('env')
        self.pipeline_type = kwargs.get('type')
        self.airflow_execution_date = kwargs.get('airflow_execution_date')
        self.task = kwargs.get('task')
        self.event_bucket = kwargs.get('event_bucket')
        self.event_prefix = kwargs.get('event_prefix')
        self.year = kwargs.get('year')
        self.partition_granularity = kwargs.get('partition_granularity')
        self.file = AttrDict(self._read_config_yaml())
        self.general = self._parse_general()
        self.source = self._parse_source()
        self.transform = self._parse_transform()
        self.target = self._parse_target()


    def _read_config_yaml(self) -> dict:
        file_path = search_upwards_for_file(self.CONFIG_NAME)
        if not file_path:
            try: 
               file_path = self.CONFIG_PATH_AWS
            except Exception as e:
                logging.error(f'Config file {self.CONFIG_NAME} not found') 
                sys.exit(1)
        my_config = {}  
        try:
            with open(file_path) as file:
                my_config = yaml.safe_load(file)
                return my_config
        except Exception as e:
            logging.error(f'Issue loading config file. {e} Exiting... ')
            sys.exit(1)
        

    def _parse_general(self) -> dict:
        """
            General section is valid for all environments
        """

        if self.partition_granularity:
            partition_key = create_partition_key(self.airflow_execution_date, partition_granularity=self.partition_granularity)
        else:
            partition_key = create_partition_key(self.airflow_execution_date)
        if self.file.general:
            general = self.file.general
            general['prefix_env'] = getattr(general.prefix_env, self.env) 
            
            # check of all required keys are set up in config, are <> null/empty and have correct type
            for i in Config.REQUIRED_GENERAL_KEYS:
                try:
                    general[i]
                except KeyError as e:
                    logging.error(f'Key \'{i}\' not found in config.yml')
                assert general[i], f"key general.{i} is empty or NULL"
                if i in  ['tasks', 'pipeline_type']:
                    assert isinstance(general[i], list), f"key general.{i} is not a list"
                else: 
                    assert isinstance(general[i], str), f"key general.{i} is not a str" 
        
            # add additional 'calculated' fields to config 
            if self.year:
                common_prefix = f'{general.prefix_env}/{general.pipeline_name}/reporting_year={self.year}/{self.pipeline_type}'
            else: 
                common_prefix = f'{general.prefix_env}/{general.pipeline_name}/{self.pipeline_type}'
            general['partition_raw'] = f'{common_prefix}/{general.prefix_raw}/{self.task}/{partition_key}'
            general['partition_processed'] = f'{common_prefix}/{general.prefix_processed}/{self.task}/{partition_key}'
            general['logs'] = f'{general.prefix_logs}/{common_prefix}/{self.task}/{partition_key}{datetime.now()}.log'
            return general
        logging.error('Mandatory general section not found')
        sys.exit(1)

    def _parse_source(self) -> dict:
        if self.file.source:
            src = self.file.source

            #### source LYNX
            if src.type == 'db' and src.name =='lynx':

                # required fields
                src['lynx']['schema'] = getattr(src.lynx.schema, self.env)
                src['lynx']['table'] = getattr(src.lynx.table, self.task)
                src['lynx']['extract_size'] = getattr(src.lynx.extract_size, self.task)

            #### source API
            if src.type == 'api':

                # required fields
                src['api']['endpoint']['name'] = getattr(src.api.endpoint.name, self.task)
                src['api']['endpoint']['type'] = getattr(src.api.endpoint.type, self.task)
                src['api']['endpoint']['version'] = getattr(src.api.endpoint.version, self.task)

                if src.api.input.type == 'db' and src.api.input.name =='redshift':

                    # required fields
                    src['api']['input']['redshift']['iam_role'] = getattr(src.api.input.redshift.iam_role, self.env)
                    src['api']['input']['redshift']['query'] = getattr(src.api.input.redshift.query, self.task)

            return src
        logging.error('Mandatory source section not found')
        sys.exit(1)

    def _parse_transform(self) -> dict:
        if self.file.transform:
            transform = self.file.transform

            ## to refactor - we did quick fix here 
            try: 
                if transform.convert_float_to_int:
                    val = getattr(transform.convert_float_to_int, self.task, None)
                    transform['convert_float_to_int'] = list(val) if val else val   
            except Exception:
                ...
            try:  
                if transform.convert_old_timestamps:
                    val = getattr(transform.convert_old_timestamps, self.task, None)
                    transform['convert_old_timestamps'] = list(val) if val else val
            except Exception:
                ...
            return transform
        logging.error('Mandatory transform section not found')
        sys.exit(1)

    def _parse_target(self) -> dict:  # sourcery skip: extract-method
        if self.file.target:
            target = self.file.target
            
            #### Target Redshift
            if target.type == 'db' and target.name == 'redshift':
                
                # required fields
                target['redshift']['iam_role'] = getattr(target.redshift.iam_role, self.env)
                target['redshift']['schema'] = getattr(target.redshift.schema, self.env)
                target['redshift']['table']= getattr(target.redshift.table, self.task)
                target['redshift']['insert_type']= getattr(target.redshift.insert_type, self.task)[self.pipeline_type]

                if target.redshift.insert_type == 'upsert':   # in case of upsert task, upsert keys need to be specified
                    target['redshift']['upsert_keys'] = list(getattr(target.redshift.upsert_keys, self.task))
                else:
                    target['redshift']['upsert_keys'] = None

            return target
        logging.error('Mandatory target section not found')
        sys.exit(1)





