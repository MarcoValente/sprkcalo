import click
from pprint import pprint as pp
import logging as log
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import os
from pathlib import Path


def remove_dummy_files(files,yes_remove,criteria_func = lambda fname,size: size < 10):
    log.info(f"Removing wrong input files...")
    fsizes = [os.path.getsize(fname) for fname in files]
    list_files_to_remove = []
    for fname,size in zip(files,fsizes):
        if criteria_func(fname,size):
            list_files_to_remove+=[fname]
    if len(list_files_to_remove) > 0:
        log.info(f"Do you want me to remove the following files?")
        for fname in list_files_to_remove:
            log.info(fname)
        if not yes_remove:
            IN = None
            while not IN in ['y','n']:
                IN = input("y/n: ")
            if IN=='y':
                for fname in list_files_to_remove:
                    print(f"Removing {fname}...")
                    os.remove(fname)
        else:
            for fname in list_files_to_remove:
                print(f"Removing {fname}...")
                os.remove(fname)
    else:
        log.info(f"No files to remove! All good!")


@click.command()
@click.argument('files', nargs=-1, type=click.Path(exists=True))
@click.option("-l", "--output_level", type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]), default="INFO", help="Logging output level")
@click.option("-y", "--yes_remove", is_flag=True, help="Force removal of files without asking")
@click.pass_context
def main(ctx,files,output_level,yes_remove):
    log.basicConfig(
        level=getattr(log,output_level),
        format="%(asctime)s - %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    log.info('Input files:')
    log.info(files)

    remove_dummy_files(files,yes_remove,lambda fname,size: size < 10)

    log.info("DONE")
    pass

if __name__=="__main__":
    main()