import logging as log
from typing import List

from .utils import (
    get_nested_value
)

def debug_msg(func):
    def wrapper(*args, **kwargs):
        log.debug(f"Calling {func.__module__}.{func.__name__} with args: {args}, kwargs: {kwargs}")
        result = func(*args, **kwargs)
        log.debug(f"{func.__module__}.{func.__name__} returned: {result}")
        return result
    return wrapper

def execute_msg(func):
    def wrapper(*args, **kwargs):
        log.info(f"Executing {func.__module__}.{func.__name__} with args: {args}, kwargs: {kwargs}")
        result = func(*args, **kwargs)
        return result
    return wrapper

def check_has_kwarg(*dec_args):
    def decorator(func):
        def wrapper(*args,**kwargs):
            for ar in list(dec_args):
                if get_nested_value(kwargs,ar,ret_none=True) is None:
                    log.error(f'Impossible to find kwarg with name \'{ar}\' during call of {func.__module__}.{func.__name__}. kwargs were set to {kwargs}')
                    raise ValueError
            return func(*args,**kwargs)
        return wrapper
    return decorator
    