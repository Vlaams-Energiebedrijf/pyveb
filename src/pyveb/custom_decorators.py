import logging
from functools import wraps
import time
import sys

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