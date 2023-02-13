import yaml
import os
import sys, inspect
import logging
from attrdict import AttrDict   # https://pypi.org/project/attrdict/
from pathlib import Path
from datetime import datetime

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
    root = Path(d.root)
    while d != root:
        attempt = d / filename
        if attempt.exists():
            return attempt
        d = d.parent
    return None

def create_partition_key(execution_date:datetime) -> str:
    """
        ARGUMENT
            partition_date: date of airflow task start date. eg 2020-01-01 12:00
    """
    day = '{:02d}'.format(execution_date.day)
    month = '{:02d}'.format(execution_date.month)
    year = execution_date.year
    return f"year={year}/month={month}/day={day}/"

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
        self.file = AttrDict(self._read_config_yaml())
        self.general = self._parse_general()
        self.source = self._parse_source()
        self.transform = self._parse_transform()
        self.target = self._parse_target()

    def _read_config_yaml(self) -> dict:
        file_path = search_upwards_for_file(Config.CONFIG_NAME)
        if not file_path:
            try: 
               file_path = Config.CONFIG_PATH_AWS
            except Exception as e:
                logging.error(f'Config file {Config.CONFIG_NAME} not found') 
                sys.exit(1)
        try:
            with open(file_path) as file:
                config = yaml.safe_load(file)
        except EnvironmentError:
            logging.error('Issue loading config file')
            
        return config

    def _parse_general(self) -> dict:
        """
            General section is valid for all environments
        """
        if self.file.general:
            dic = self.file.general
            dic['prefix_env'] = getattr(dic.prefix_env, self.env) 
            
            # check of all required keys are set up in config, are <> null/empty and have correct type
            for i in Config.REQUIRED_GENERAL_KEYS:
                try:
                    dic[i]
                except KeyError as e:
                    logging.error(f'Key \'{i}\' not found in config.yml')
                # print(i)
                assert dic[i], f"key general.{i} is empty or NULL"
                if i in  ['tasks', 'pipeline_type']:
                    assert isinstance(dic[i], list), f"key general.{i} is not a list"
                else: 
                    assert isinstance(dic[i], str), f"key general.{i} is not a str" 
            
              
            # add additional 'calculated' fields to config 
            dic['partition_raw'] = f'{dic.prefix_env}/{dic.pipeline_name}/{self.pipeline_type}/{dic.prefix_raw}/{self.task}/{create_partition_key(self.airflow_execution_date)}'
            dic['partition_processed'] = f'{dic.prefix_env}/{dic.pipeline_name}/{self.pipeline_type}/{dic.prefix_processed}/{self.task}/{create_partition_key(self.airflow_execution_date)}'
            return dic
        logging.error('Mandatory general section not found')
        sys.exit(1)

    ## TO DO - add proper error handling in case env or task is not setup for a required attribute
    def _parse_source(self) -> dict:
        if self.file.source:
            dic = self.file.source
            if dic.api.input.redshift.iam_role:
                dic['api']['input']['redshift']['iam_role'] = getattr(dic.api.input.redshift.iam_role, self.env)
            if dic.api.input.redshift.query:
                dic['api']['input']['redshift']['query'] = getattr(dic.api.input.redshift.query, self.task)
            if dic.api.endpoint.name:
                dic['api']['endpoint']['name'] = getattr(dic.api.endpoint.name, self.task)
            if dic.api.endpoint.type:
                dic['api']['endpoint']['type'] = getattr(dic.api.endpoint.type, self.task)
            return dic
        logging.error('Mandatory source section not found')
        sys.exit(1)

    def _parse_transform(self) -> dict:
        if self.file.transform:
            return self.file.transform
        logging.error('Mandatory transform section not found')
        sys.exit(1)

    def _parse_target(self) -> dict:
        if self.file.target:
            dic = self.file.target
            if dic.redshift.iam_role:
                dic['redshift']['iam_role'] = getattr(dic.redshift.iam_role, self.env)
            if dic.redshift.schema:
                dic['redshift']['schema'] = getattr(dic.redshift.schema, self.env)
            if dic.redshift.table:
                dic['redshift']['table']= getattr(dic.redshift.table, self.task)
            if dic.redshift.insert_type:
                dic['redshift']['insert_type']= getattr(dic.redshift.insert_type, self.task)
            if dic.redshift.upsert_keys:
                dic['redshift']['upsert_keys'] = getattr(dic.redshift.upsert_keys, self.task)
            return self.file.target
        logging.error('Mandatory target section not found')
        sys.exit(1)





