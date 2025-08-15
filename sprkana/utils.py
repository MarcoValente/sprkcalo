import os
import logging as log

def get_nested_value(data:dict, path:str, sep="/", ret_none=False):
        keys = path.split(sep)
        current = data
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                # Return original path if lookup fails
                return (None if ret_none else path)
        return current

def create_dir(directory: str):
    if not os.path.exists(directory):
        log.info(f'Creating output directory \'{directory}\'...')
        os.makedirs(directory)