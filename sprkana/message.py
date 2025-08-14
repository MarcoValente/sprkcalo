import logging as log

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
