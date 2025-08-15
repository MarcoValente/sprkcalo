
from .config import (
    parse_args,
    print_config,
)
from .commands import (
    run_show,
    run_histdump,
)
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
    _command_dict = {
        'show' : run_show,
        'hist_dump' : run_histdump,
    }

    if not args.command in _command_dict.keys():
        log.error(f'Impossible to find command for \'{args.command}\'')
        exit(1)
    
    #Execute function
    _ = _command_dict[args.command](**vars(args))
    pass