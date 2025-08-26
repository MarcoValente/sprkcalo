import logging as log
from .message import (
    debug_msg,
    check_has_kwarg,
)
from .analysis import (
    get_spark_session, 
    analysis_df,
)
from .histogram import (
    histogram_df
)
from .schema import (
    set_schema
)
from .utils import (
    create_dir
)
import os

_checkpoint_dir="/tmp/checkpoints"

@debug_msg
@check_has_kwarg('analysis_df','analysis_df/data_schema','output_dir', 'force')
def run_preana(*args,**kwargs):
    log.info(f"Running 'preana' command...")
    log.debug(f"Running 'preana' command with args: {args} and kwargs: {kwargs}")
    # Set the schema if provided
    set_schema(kwargs['analysis_df']['data_schema'])
    #Create output_dir if not exists
    
    if kwargs['output_dir']=='':
        log.error(f"The output directory is empty. Please provide one with -o/--output_dir or in the main yaml configuration file.")
        raise ValueError
    elif os.path.exists(kwargs['output_dir']) and not kwargs['force']:
        log.error(f"The output directory \'{kwargs['output_dir']}\' already exists. Either change output directory, or use the -f/--force option to force.")
        raise ValueError
    else:
        create_dir(kwargs['output_dir'])

@debug_msg
@check_has_kwarg('nevents')
def run_ana(*args, spark=None, **kwargs):
    log.info(f"Running 'ana' command...")
    log.debug(f"Running 'ana' command with args: {args} and kwargs: {kwargs}")
    spark_sess = get_spark_session(spark)
    df = analysis_df(spark_sess, *args, **kwargs)
    return (spark_sess, df)

@debug_msg
@check_has_kwarg('spark')
def run_show(*args, spark=None, limit:int = 20, truncate=False, **kwargs):
    log.info(f"Running 'show' command...")
    log.debug(f"Running 'show' command with args: {args} and kwargs: {kwargs}")
    run_preana(*args,**kwargs)
    spark_sess, df = run_ana(*args,spark=spark,**kwargs)
    df.limit(limit).show(truncate=truncate)  # Display the DataFrame
    return (spark_sess, df)

@debug_msg
@check_has_kwarg('spark','output_dir','histograms')
def run_histdump(*args, spark=None, **kwargs):
    log.info(f"Running 'histdump' command...")
    log.debug(f"Running 'histdump' command with args: {args} and kwargs: {kwargs}")
    run_preana(*args,**kwargs)
    spark_sess, df = run_ana(*args,spark=spark,**kwargs)
    #Setup checkpoint to avoid multiple calculations for histograms later
    spark_sess.sparkContext.setCheckpointDir(_checkpoint_dir)
    df = df.checkpoint()
    #Calculate histograms now
    _ = histogram_df(spark_sess,df,*args,**kwargs)
    return (spark_sess, None)