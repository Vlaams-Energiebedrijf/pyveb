import logging
from functools import wraps
import time
import sys
import time
import logging
from functools import wraps
from typing import Callable, Tuple, Type
import requests


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


## should be modelled similary as retry_http_request
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


def retry_http_request(
    retries: int = 3,
    delay: float = 1.0,
    retry_exceptions: Tuple[Type[BaseException], ...] = ( 
        requests.exceptions.ConnectionError, 
        requests.exceptions.Timeout, 
    )
) -> Callable:
    """
    Retry a function call up to `retries` times with a `delay` in seconds between attempts.

    Args:
        retries (int): Number of retry attempts. Default is 3.
        delay (float): Delay basis for exponential backoff (in seconds) between retries. Default is 1 second.
        retry_exceptions (Tuple[Type[BaseException], ...]): Exceptions to trigger a retry. Default are requests.exceptions.ConnectionError, 
        requests.exceptions.Timeout and 500, 502, 504, 504 & 429 requests.exceptions.HTTPError 

    Returns:
        Callable: Decorated function.
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, retries + 1):
                try:
                    return func(*args, **kwargs)
                
                except retry_exceptions as e:
                    if attempt < retries:
                        exponential_delay = delay * (2 ** attempt)
                        logging.warning(
                            f"Attempt {attempt} for function {func.__name__} failed: {e}. \n Retrying in {exponential_delay} seconds..."
                        )
                        time.sleep(exponential_delay) 
                    else:
                        logging.error(f"Function {func.__name__} failed after {attempt} attempts.")
                        raise  # Propagate the error on the last attempt

                except requests.exceptions.HTTPError as e:
                    # Differentiate HTTP errors by status code
                    status_code = e.response.status_code
                    if status_code in [500, 502, 503, 504]:  # Retriable server errors
                        if attempt < retries + 1:
                            exponential_delay = delay * (2 ** attempt)
                            logging.warning(
                                f"Attempt {attempt} for function {func.__name__} failed: {e}. \n Retrying in {exponential_delay} seconds..."
                            )
                            time.sleep(exponential_delay) 
                        else:
                            logging.error(f"Function {func.__name__} failed after {attempt} attempts.")
                            raise  # Propagate the error on the last attempt

                    elif status_code == 429:  # Too Many Requests (Rate Limiting)
                        retry_after = int(e.response.headers.get("Retry-After", delay))  # Default to `delay` if no header
                        logging.warning(
                            f"Rate limited (HTTP 429). Retrying after {retry_after} seconds..."
                        )
                        time.sleep(retry_after)
                    else:
                        # Non-retriable HTTP errors (e.g., 400, 401, 403, etc.)
                        logging.error(f"Non-retriable HTTP error {status_code}: {e}")
                        raise  # Propagate non-retriable HTTP errors

                except Exception as e:
                    logging.error(f"Non-retriable error occurred: {e}")
                    raise 
        return wrapper
    return decorator
