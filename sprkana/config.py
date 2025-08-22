import argparse
import logging as log
import sys
from .utils import (
    load_yaml
)

    
def parse_args():
    #Init parser for defaults from YAML
    _parser = argparse.ArgumentParser(description="SparkAnalysis CLI", add_help=False)

    _parser.add_argument("-c", "--mainConfig", help="Path to general YAML config", required=True)
    _parser.add_argument("-i", "--inputs", nargs='+', help="List of parquet input files")
    _parser.add_argument("-N", "--names", nargs='+', help="List of names for parquet files")
    _parser.add_argument('-l', "--output_level", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], default="INFO")
    _parser.add_argument('-o', "--output_dir", type=str, default='', help='Directory where to store output files')
    _parser.add_argument('-f', '--force', action='store_true', default=False, help='Force the script to write to existing directory')
    _parser.add_argument("-n", "--nevents", type=int, default=-1, help="Number of events to process")
    _parser.add_argument("--inputsToMatch", nargs='+', help="List of parquet input files to use for matching the main ones.")

    _subparsers = _parser.add_subparsers(dest="command", required=False)
    # Show subcommand
    _show_parser = _subparsers.add_parser("show", help="Show the dataframe of the model")
    _show_parser.add_argument("--limit", type=int, default=20, help="Number of rows to show")
    _show_parser.add_argument("--truncate", action='store_true', default=False, help="Truncate output display or not")
    # Hist dump subcommand
    _histdump_parser = _subparsers.add_parser('hist_dump', help="Dump histograms of the analysis")
    _histdump_parser.add_argument('--histConfig', type=str, default=None, help="YAML file containing histogram information to dump", required=True)

    known_args, remaining_argv = _parser.parse_known_args()

    #Set defaults
    defaults = None
    if known_args.mainConfig:
        defaults = load_yaml(known_args.mainConfig)

    default_hists = None
    if 'histConfig' in vars(known_args).keys():
        default_hists = load_yaml(known_args.histConfig)
        defaults.update(default_hists)

    #Main parser

    parser = argparse.ArgumentParser(parents=[_parser], description="SparkAnalysis CLI")
    parser.set_defaults(**defaults)

    args= parser.parse_args(sys.argv[1:])

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