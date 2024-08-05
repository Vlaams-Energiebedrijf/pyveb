import os
import yaml
import inspect
from datetime import date, datetime
import logging
import sys
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import psutil
import argparse
from typing import Dict, List
from botocore.exceptions import ClientError
import boto3
import time
import json
import pandas as pd

cores = psutil.cpu_count(logical = False)

class dotdict(dict):
    """dot.notation access to dictionary attributes"""
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

def get_config():
    """
        RETURN
            config object enabling fetching of config from config.yml via dot notation

        ADDITIONAL INFO
            In docker image we copy config file into current dir, for local development we fetch config from level lower
    """
    caller_file = os.path.dirname(inspect.stack()[1].filename)
    try: 
        path = os.path.join(caller_file, 'config.yml')
        with open(f'{caller_file}/config.yml') as file:
            config = yaml.load(file, Loader=yaml.FullLoader)

    except Exception:
        path = os.path.join(caller_file, '..', 'config.yml')
        with open(path) as file:
            config = yaml.load(file, Loader=yaml.FullLoader)

    config = dotdict(config)
    return config

def create_partition_key(partition_date:str) -> str:
    """
        ARGUMENT
            partition_date: date of airflow task start date. eg 2020-01-01 12:00
        
        RETURN
            year=2020/month=01/day=01/    
    """
    if not isinstance(partition_date, date):
        date_format = "%Y-%m-%d"
        partition_date = datetime.strptime(partition_date, date_format)
    day = '{:02d}'.format(partition_date.day)
    month = '{:02d}'.format(partition_date.month)
    year = partition_date.year
    return f"year={year}/month={month}/day={day}/"

def get_args(args):
    """
        ARGUMENTS
            args: sys.args

        RETURNS
            env, pipeline_type, partition_start, partition_start, partition_key

        ADDITIONAL INFO
            Command line args are passed to python programs via airflow dag - AWS Batchoperator - Overrides - Command  
            These args are then passed to our program via entrypoint.sh via $@

            Env, pipeline type and partition start ( ie. airflow task start date) are mandatory for incremental/full pipelines.
            For incremental pipeline, additionally we require a partition end date. 
            Partition key is also returned which is calculated based on partition_start
    """
    try: 
        print(args)
        env = args[1]
        pipeline_type = args[2]
        assert env in ['local', 'dev', 'prd']
        assert pipeline_type in ['full', 'incremental']
        logging.info(f"Environment: {env}")
        logging.info(f"Pipeline type: {pipeline_type}")
    except AssertionError as e:
        logging.error("Incorrect environment or pipeline type argument passed. Exiting...")
        logging.error(e)
        sys.exit(1)
    except Exception:
        logging.error("No environment argument passed. Exiting...")
        sys.exit(1)

    # fetch airflow partition(s)
    try:
        format = "%Y-%m-%d"
        partition_start = args[3]
        try:
            is_valid_start = bool(datetime.strptime(partition_start, format))
        except:
            is_valid_start = False
        assert is_valid_start
        logging.info(f"Partition start: {partition_start}")
        if pipeline_type == 'incremental':
            partition_end = args[4]
            try:
                is_valid_end = bool(datetime.strptime(partition_end, format))
            except:
                is_valid_end= False
            assert is_valid_end
            logging.info(f"Partition end: {partition_end}")
        else:
            partition_end = None
        partition_key = create_partition_key(partition_start)
    except AssertionError as e:
        logging.error("Incorrect partition date argument passed. Dates must be passed as YYYY-MM-DD Exiting...")
        logging.error(e)
        sys.exit(1)
    except: 
        logging.error("No partition date arguments passed. Exiting")
        logging.info("Pass partition start and partition end date arguments as postional arguments 2 and 3")
        sys.exit(1)
    return env, pipeline_type, partition_start, partition_end, partition_key

def chunker(seq, size):
    """
        ARGUMENTS: 
            seq: list of items
            size: 
        process list in batches of size 
        eg. for group in chunker(list, 100):
                    print(group)
    """
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))

def multithreading_list(func, input_list, *args, max_workers=cores, **kwargs):
    """
        ARGUMENTS
            func: function we want to execute in multithreading mode on a list of items
            input_list: list of items. Items from this list are passed as the first positional arg of func
            *args: additional positional args we want to pass to func
            max_workers: amount of threads used. Defaults to nbr of physical cores of the machine
            **kwargs: additional kwargs we want to pass to func

        INFO
            Use for heavy IO operations
            https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor

        RETURNS
            (dict(item: func return value ), dict(item: func error ))

            ! item is the item from the list we pass as first positional argument of the function. 
    """
    results = {}
    errors= {}
    with ThreadPoolExecutor(max_workers) as executor:
        future_to_x = {executor.submit(func, x, *args, **kwargs): x for x in input_list}
        for future in as_completed(future_to_x):
            x = future_to_x[future]
            # If an exception was raised while executing the task, it will be re-raised when calling the result() function or can be accessed via the exception() function.
            try:
                result = future.result()
                results[x] = result
            except Exception as exc:
                errors[x] = exc
                print('%r generated an exception: %s' % (x, exc))
    return results, errors

def multiprocessing(func, input_list, *args, max_workers=cores, **kwargs):
    """
        ARGUMENTS
            func: function we want to execute in multiprocessing mode on a list of items
            input_iterable: 
                    list of items. Each item can be another list. Items are passed as the first positional arg of the func.
                    In case of a list of lists, the inner list is unpacked an passed as multiple positional args. 
                    Only use list of list in case you want to pass several specific args. In case you want to pass common args, use *args
            *args: additional common positional args we want to pass to func
            max_workers: amount of threads used. Defaults to nbr of physical cores of the machine
            **kwargs: additional kwargs we want to pass to func

        INFO
            Use for heavy CPU operations
            https://docs.python.org/3/library/concurrent.futures.html#processpoolexecutor

        RETURNS
            (dict(item: function return value ), dict(item: function error ))

            !!! item is the item from the list we pass as first positional argument of the function. 

        ! multiprocessing list of list not tested yet
    """
    results = {}
    errors= {}
    with ProcessPoolExecutor(max_workers) as executor:
        if isinstance(input_list[0], list):
            future_to_x = {executor.submit(func, *x, *args, **kwargs): x for x in input_list}
        else:
            future_to_x = {executor.submit(func, x, *args, **kwargs): x for x in input_list}
        for future in as_completed(future_to_x):
            x = future_to_x[future]
            try:
                result = future.result()
                results[x] = result
            except Exception as exc:
                errors[x] = exc
                print('%r generated an exception: %s' % (x, exc))
    return results, errors

def valid_date(x: str) -> bool:
        """ 
            Checks whether a string has date format %Y-%m-%d (ie. 2020-01-01). 
            
            Returns datetime object or raises a ValueError
        """
        try:
            return datetime.strptime(x, "%Y-%m-%d")
        except ValueError as e:
            msg = "Not a valid date: {0!r}".format(x)
            raise argparse.ArgumentTypeError(msg) from e

def parse_args() -> Dict:
    """
        Parses command line arguments and returns dictionary of arguments which can be accessed via dot notation

        ARGUMENTS
            '--env', '-e', default ='local', type = str, choices = ['local', 'dev', 'prd']
            '--type', '-t', default = 'incremental', type = str, choices = ['event', 'incremental', 'full']
            '--airflow_execution_date', '-d', default = '2020-01-01', type = valid_date
            '--task', '-task', type = str, required=True
            '--event_bucket', type=str
            '--event_prefix', type=str
            '--year', type=str. In case we need to run a pipeline yearly, for example VLABEL process is a yearly process. Specify for which year you want to execute
            '--data_sources, type = list, see dags in pipeline basisregistere_gebouw for usage 



    """
    # create parser
    parser = argparse.ArgumentParser()

    # add arguments
    parser.add_argument('--env', '-e', default ='local', type = str, choices = ['local', 'dev', 'prd', 'stg'])
    parser.add_argument('--type', '-t', default = 'incremental', type = str, choices = ['event', 'incremental', 'full_refresh'])
    parser.add_argument('--airflow_execution_date', '-d', default = '2020-01-01', type = valid_date)
    parser.add_argument('--task', '-task', type = str, required=True)
    parser.add_argument('--event_bucket', type=str )
    parser.add_argument('--event_prefix', type=str)
    parser.add_argument('--year', type=int)
    parser.add_argument('--data_sources', nargs='+', default=[], )

    # parse arguments
    args = parser.parse_args()

    # manual error handling
    if args.type == 'event' and ( args.event_bucket is None or args.event_prefix is None ):
        parser.error("In case pipeline_type = 'event', both event_bucket and event_prefix need to be included")

    return args

def remove_duplicate_from_list_of_dicts(list_of_dicts):
  unique_dicts = list(set(tuple(d.items()) for d in list_of_dicts))  
  return [{k: v for k, v in item} for item in unique_dicts] 

def remove_duplicates_from_list_of_dicts_by_key(list_of_dicts, key = None):
  seen_ids = set()
  unique_items = []

  for item in list_of_dicts:
    if item[key] not in seen_ids:
      seen_ids.add(item[key])
      unique_items.append(item)

  return unique_items

def list_of_dicts_to_json_s3(s3, list_of_dicts: List[Dict[str, str]], s3_prefix:str, s3_file_name:str, add_timestamp:bool =True) -> None:
    """Writes a list of dictionaries as a JSON file to S3.

    Args:
        data: A list of dictionaries to write.
        bucket_name: The name of the S3 bucket.
        s3_key: The key (filename) under which to store the data in S3.
    """
    logging.info('Starting upload')

    if add_timestamp:
        timestamp = round(time.time(), 4)
        s3_key = f'{s3_prefix}{timestamp}_{s3_file_name}.json'
    else:
        s3_key = f'{s3_prefix}{s3_file_name}.json'
    
    serialized_data = json.dumps(list_of_dicts, ensure_ascii=False)  # Ensure non-ASCII characters are handled
    s3.client.put_object(Body=serialized_data.encode('utf-8'), Bucket=s3.bucket_name, Key=s3_key, ContentType='application/json')
    logging.info('Succesfully uploaded to S3')
    return

def merge_dicts(array_a, array_b, merge_key, key_to_add_to_a_from_b):
    """
    Merges dictionaries from two arrays based on ID, adding keys D and F from B.

    Args:
        array_a: List of dictionaries (source array).
        array_b: List of dictionaries (data to add).
        key_d: Key in B to add as key D in the merged dictionary.
        key_f: Key in B to add as key F in the merged dictionary.

    Returns:
        List of merged dictionaries.

    Raises:
        KeyError: If an ID from A is not found in B.
    """
    merged_array = []
    id_map = {d[merge_key]: d for d in array_b}  # Create a dictionary for faster lookup by ID in B

    for item in array_a:
        item_id = item[merge_key]
        if item_id not in id_map:
            raise KeyError(f"{merge_key} {item_id} not found in array B")

        # Add key-value pairs from B to a copy of the dictionary in A
        merged_item = {**item, key_to_add_to_a_from_b: id_map[item_id][key_to_add_to_a_from_b]}
        merged_array.append(merged_item)

    return merged_array

def get_secret(secret_name, secret_region):

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=secret_region
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    return get_secret_value_response['SecretString']

def filter_list_of_dicts_on_key_value(list_of_dicts, key, target_value, case_sensitive = 'lower'):
    """
        case_sensitive = ['lower', 'upper', 'sensitive']
    """
    if case_sensitive == 'lower':
        return [item for item in list_of_dicts if str(item[key]).lower() == str(target_value).lower()]
    if case_sensitive == 'upper':
        return [item for item in list_of_dicts if str(item[key]).lower() == str(target_value).upper()]
    if case_sensitive == 'sensitive':
        return [item for item in list_of_dicts if item[key] == target_value]

def list_of_dicts_to_df(list_of_dicts):
    return pd.DataFrame(list_of_dicts)

def df_to_list_of_dicts(df):
    return df.to_dict('records')

