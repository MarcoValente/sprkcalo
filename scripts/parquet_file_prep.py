import click
from pprint import pprint as pp
import logging as log
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import os
from pathlib import Path


@click.command()
@click.argument('files', nargs=-1, type=click.Path(exists=True))
@click.option("-l", "--output_level", type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]), default="INFO", help="Logging output level")
@click.option("-o", "--output_dir", type=click.Path(), default=None, help="Output directory", required=True)
@click.option("-O", "--output_fname", type=str, default="output", help="Basename for outputfile")
@click.option("-s", "--output_row_size", type=int, help="Number of rows to store in each output file", required=True)
@click.option(      "--input_format", type=click.Choice(["parquet"]), default="parquet", help="Input file format")
@click.pass_context
def main(ctx,files,output_dir,output_level,input_format,output_row_size,output_fname):
    log.basicConfig(
        level=getattr(log,output_level),
        format="%(asctime)s - %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    log.info('Input files:')
    log.info(files)

    dataset = ds.dataset(files, format=input_format)
    log.info(f"Loaded dataset with the following schema:")
    print(dataset.schema)

    table = dataset.to_table()
    log.info(f"Extracted following table with size {table.shape}")

    log.info(f"Storing output files with row size of {output_row_size} to directory {output_dir}")

    if not os.path.exists(output_dir):
        log.info(f"Creating output directory '{output_dir}'...")
        os.makedirs(output_dir)

    pq.write_to_dataset(
        table,
        root_path=f"{output_dir}/",
        # partition_cols=["run_num"],
        row_group_size=output_row_size,
        basename_template=output_fname+"_part-{i}.parquet"
    )
    log.info("Writing done. Verifying the number of row groups inside the file...")

    filename = [p for p in Path(output_dir).rglob(f"*.{input_format}") if p.is_file()][0]
    parquet_file = pq.ParquetFile(filename)

    log.info(f"Number of row groups: {parquet_file.num_row_groups}")

    for i in range(parquet_file.num_row_groups):
        rg = parquet_file.metadata.row_group(i)
        log.info(f"Row Group {i+1}: {rg.num_rows} rows, {rg.total_byte_size} bytes")

    log.info("DONE")
    
    pass

if __name__=="__main__":
    main()