import argparse
import logging as log
import sys
from .utils import (
    load_yaml
)

def load_config_and_merge(*args, **kwargs):
    # Load main configuration if provided
    config = {}
    if kwargs.get('mainconfig'):
        config = load_yaml(kwargs['mainconfig'])
        log.info(f"Loaded main configuration from {kwargs['mainconfig']}")
    
    # Merge command line arguments, overriding config file values
    for key, value in kwargs.items():
        if value is not None:
            config[key] = value

    return config

class Config:
    """Merges YAML config with CLI options."""
    def __init__(self, config_dict, cli_options):
        self._config = dict(config_dict)
        self._config.update({k: v for k, v in cli_options.items() if v is not None})

    def __getitem__(self, item):
        return self._config.get(item)

    def as_dict(self):
        return self._config

def print_config_rec(args, level=0, list_override=False):
    if list_override:
        log.info(f"{'  ' * level} {args},")
        return
    if isinstance(args, dict):
        for k, v in args.items():
            if isinstance(v, dict):
                log.info(f"{'  ' * level} - {k:15s}:")
                print_config_rec(v, level + 1)
            elif isinstance(v, list):
                log.info(f"{'  ' * level} - {k:15s}: [")
                print_config_rec(v, level + 1)
                log.info(f"{'  ' * level} ]")
            else:
                log.info(f"{'  ' * level} - {k:15s}: {v}")
    elif isinstance(args, list):
        for i,item in enumerate(args):
            if isinstance(item, dict):
                print_config_rec(item, level+1, list_override=True)
            else:
                log.info(f"{'  ' * level} {item},")
    else:
        log.info(f"{'  ' * level} - {args}")    

def print_config(args):
    log.info("Configuration:")
    if isinstance(args, dict):
        print_config_rec(args)
    else:
        log.info(f"Args: {args}")
    log.info("")