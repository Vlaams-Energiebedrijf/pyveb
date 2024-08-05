import logging
from functools import wraps
import time
import sys


## NOTE: be careful with this implementation, no error is raised by program is exited
def retry(retries: int, **fkwargs):
    """
        Retry decorator with adjustable retries. 

        USAGE:

        @retry(retries = x, error = "Failure executing Message")
        def function(..., **kwargs)
            ...
            return

        ! Ensure you setup **kwargs in the function you're wrapping. The attempt number is passed to this function as a kwarg
    
    """
    def _decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(retries):
                try:
                    return func(*args, attempt = attempt, **kwargs)
                except Exception as e:
                    logging.warning(f"Error: {fkwargs['error']}, Attempt: {attempt}, Error: {e}")
                    if attempt < retries-1:
                        time.sleep((attempt+1)*(attempt+1))
            logging.error(f"Failed after {retries} attempts. Exiting...")
            sys.exit(1)
        return wrapper
    return _decorator


def retry_v2(retries: int, **fkwargs):
    """
        Retry decorator with adjustable retries. 

        USAGE:

        @retry(retries = x)
        def function(..., **kwargs)
            ...
            return

        ! Ensure you setup **kwargs in the function you're wrapping. The attempt number is passed to this function as a kwarg
    
    """
    def _decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(retries+1):
                try:
                    return func(*args, attempt = attempt, **kwargs)
                except Exception as e:
                    if attempt == 0: 
                        logging.error(f"Function {func.__name__} failed initial attempt with the following error: '{e}' of type {type(e)}. Retrying in {(attempt+1)*(attempt+1)} seconds")
                        time.sleep((attempt+1)*(attempt+1))
                        # time.sleep(0.1)
                    elif attempt < retries:
                        logging.error(f"Function {func.__name__} failed retry attempt: {attempt} with the following error: '{e}' of type {type(e)}. Retrying in {(attempt+1)*(attempt+1)} seconds")
                        time.sleep((attempt+1)*(attempt+1))
                        # time.sleep(0.1)
                    else: 
                        logging.error(f"Function {func.__name__} failed final retry attempt with the following error: '{e}' of type {type(e)}")
                        raise e
        return wrapper
    return _decorator