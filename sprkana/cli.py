
from .config import (
    load_yaml,
    parse_args,
    print_config,
)
from .commands import run_show
from pprint import pprint as pp
import logging as log

def main():
    #Parse command line arguments
    args = parse_args()

    #Set up logging based on the output level
    log.basicConfig(level=getattr(log, args.output_level), format='%(asctime)s %(levelname)s: %(message)s')

    #Print the configuration
    print_config(vars(args))

    # Subcommand-specific arguments
    if args.command == "show":
        run_show(**vars(args))
