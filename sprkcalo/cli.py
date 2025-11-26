
from .config import (
    load_config_and_merge,
    print_config,
)
from pprint import pprint as pp
import logging as log
import click

@click.group()
@click.option('-c', '--mainConfig', type=click.Path(exists=True), default=None, help="YAML file containing main configuration")
@click.option('-o', '--output_dir', type=click.Path(), default='', help="Output directory")
@click.option('-f', '--force', is_flag=True, default=False, help="Force overwrite of output directory if it exists")
@click.option('-N', '--names', multiple=True, help="Names for input files")
@click.option('-i', '--inputs', multiple=True, type=click.Path(exists=True), help="Input parquet files")
@click.option('-l', '--output_level', type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]), default="INFO", help="Logging output level")
@click.option('-n', '--nevents', type=int, default=-1, help="Number of events to process")
@click.option('--inputsToMatch', multiple=True, type=click.Path(exists=True), help="Input parquet files to use for matching the main ones")
@click.pass_context
def main(ctx, *args, **kwargs):
    log.basicConfig(level=getattr(log, kwargs['output_level']), format='%(asctime)s %(levelname)s: %(message)s')
    # Load configuration from options and YAML configuration file, and print it
    config = load_config_and_merge(*args,**kwargs)

    ctx.ensure_object(dict)
    ctx.obj['config'] = config
    pass

#Define subcommands
from .commands import (
    show,
    hist_dump,
)

main.add_command(show)
main.add_command(hist_dump)