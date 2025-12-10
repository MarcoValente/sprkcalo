import os
import logging as log
import yaml
from typing import Union
from pathlib import Path
import re
import subprocess

def get_nested_value(data:dict, path:str, sep="/", ret_none=False):
        keys = path.split(sep)
        current = data
        for key in keys:
            if isinstance(current, dict) and key in current.keys():
                current = current[key]
            else:
                # Return original path if lookup fails
                return (None if ret_none else path)
        return current

def get_nested_values(data:dict, path:str, sep="/", ret_none=False):
        keys = path.split(sep)
        current = data
        for key in keys:
            if isinstance(current, dict):
                found_keys_list=[]
                for sub_key in current.keys():
                    if re.match(key,sub_key):
                        found_keys_list.append(sub_key)
                if len(found_keys_list)==0:
                     return (None if ret_none else [path])
                elif len(found_keys_list)==1:
                    current = current[found_keys_list[0]]
                else:
                    current = [current[fk] for fk in found_keys_list]
            else:
                # Return original path if lookup fails
                return (None if ret_none else [path])
        return current

def create_dir(directory: str, strip_filename: bool = False, strip_str: str = r"\w+://\w+"):
    if strip_filename:
        directory = re.sub(strip_str,"",directory)
    if re.match(r"hdfs://.*",directory):
        log.info(f'Creating output directory \'{directory}\' on hdfs...')
        subprocess.run(f"hdfs dfs -mkdir -p {directory}", shell=True)
    elif not os.path.exists(directory):
        log.info(f'Creating output directory \'{directory}\'...')
        os.makedirs(directory)


def load_yaml(path: Union[str, Path]) -> dict:
    def yaml_include(loader, node):
        # Ensure we resolve relative paths correctly
        filename = Path(loader.name).parent / node.value
        with open(filename, "r") as f:
            return yaml.load(f, Loader=yaml.SafeLoader)
    # Register on SafeLoader (important!)
    yaml.SafeLoader.add_constructor("!include", yaml_include)

    path = Path(path)
    with open(path, "r") as f:
        return yaml.load(f, Loader=yaml.SafeLoader) or {}