import os
import yaml
import inspect
from datetime import date, datetime
import logging
import sys
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import psutil
import argparse
from typing import Dict

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



    """
    # create parser
    parser = argparse.ArgumentParser()

    # add arguments
    parser.add_argument('--env', '-e', default ='local', type = str, choices = ['local', 'dev', 'prd'])
    parser.add_argument('--type', '-t', default = 'incremental', type = str, choices = ['event', 'incremental', 'full_refresh'])
    parser.add_argument('--airflow_execution_date', '-d', default = '2020-01-01', type = valid_date)
    parser.add_argument('--task', '-task', type = str, required=True)
    parser.add_argument('--event_bucket', type=str )
    parser.add_argument('--event_prefix', type=str)
    parser.add_argument('--year', type=int)

    # parse arguments
    args = parser.parse_args()

    # manual error handling
    if args.type == 'event' and ( args.event_bucket is None or args.event_prefix is None ):
        parser.error("In case pipeline_type = 'event', both event_bucket and event_prefix need to be included")

    return args