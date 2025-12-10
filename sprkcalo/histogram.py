from .message import (
    debug_msg,
    execute_msg,
    check_has_kwarg,
)
from .utils import (
    create_dir,
)
import logging as log
from typing import List
from pyspark.sql import functions as F
import shutil
import os
import re
import subprocess

@debug_msg
@check_has_kwarg('name','col_name','nbins','bounds')
def get_histogram_df(spark_sess, df, name : str = None, col_name : str = None, nbins : int = None, bounds:List[float]=None, **kwargs):
    """
    Get a histogram of a column in the DataFrame.
    
    Parameters:
    - df: Spark DataFrame
    - column: Column name to create histogram for
    - bins: Number of bins for the histogram
    - range: Range of values for the histogram
    
    Returns:
    - DataFrame with histogram data
    """
    log.info(f'Getting histogram \'{name}\', from col_name={col_name}, nbins={nbins}, bounds={bounds}')
    colname_bin = "bin"
    colname_binstart = "bin_start"
    colname_binend = "bin_end"

    bin_width = (bounds[1] - bounds[0]) / nbins

    bins = spark_sess.createDataFrame([(i,) for i in range(nbins + 1)], [colname_bin])
    df_h = df.withColumn(colname_bin, F.floor((F.col(col_name)-bounds[0]) / bin_width))
    df_h = df_h.groupBy(colname_bin).count()
    df_h = bins.join(df_h, on=colname_bin, how="left").fillna(0).orderBy(colname_bin)
    df_h = df_h \
        .withColumn(colname_binstart, F.col(colname_bin) * bin_width + bounds[0]) \
        .withColumn(colname_binend, F.col(colname_binstart) + bin_width) \
        .select(colname_binstart, colname_binend, "count")
    df_h = df_h.orderBy(colname_binstart)
    return df_h

@debug_msg
@check_has_kwarg('histograms', 'output_dir', 'hist_subdir')
def histogram_df(spark_sess, df,*args,histograms:dict={},save_hists=True,hist_subdir='',save_config=True,**kwargs):
    log.info(f'Retrieving histograms with dict {histograms}')
    hist_outdir=f"{kwargs['output_dir']}/{hist_subdir}"
    for hist_dict in histograms:
        df_h = get_histogram_df(spark_sess, df, **hist_dict)
        if save_hists:
            create_dir(hist_outdir)
            df_h.write.mode('overwrite').json(f"{hist_outdir}/{hist_dict['name']}")
    if save_config:
        src_file = kwargs['histconfig']
        dst_file = os.path.join(hist_outdir, os.path.basename(src_file))
        if re.match(r"hdfs://.*",hist_outdir):
            subprocess.run(f"hdfs dfs -copyFromLocal {src_file} {dst_file}", shell=True)
        else:
            shutil.copy(kwargs['histconfig'], dst_file)

    return None