from typing import Union
from pathlib import Path
import yaml
import argparse
import logging as log

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
    
def parse_args():
    init_parser = argparse.ArgumentParser(description="SparkAnalysis CLI", add_help=False)
    init_parser.add_argument("-c", "--config", help="Path to general YAML config")

    known_args, remaining_argv = init_parser.parse_known_args()

    defaults = {}
    if known_args.config:
        defaults = load_yaml(known_args.config)
    defaults["config"] = known_args.config

    parser = argparse.ArgumentParser(parents=[init_parser], description="SparkAnalysis CLI")
    # print(parser.parse_args().config)
    parser.set_defaults(**defaults)

    parser.add_argument("-i", "--inputs", nargs='+', help="List of parquet input files")
    parser.add_argument("-N", "--names", nargs='+', help="List of names for parquet files")
    parser.add_argument('-l', "--output_level", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], default="INFO")
    parser.add_argument("-n", "--nevents", type=int, help="Number of events to process")

    subparsers = parser.add_subparsers(dest="command", required=True)

    # Show subcommand
    show_parser = subparsers.add_parser("show", help="Show the dataframe of the model")
    show_parser.add_argument("--epochs", type=int, help="Number of epochs")

    args= parser.parse_args(remaining_argv)

    return args

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