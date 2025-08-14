import logging as log
from .message import (
    debug_msg,
    execute_msg,
)

from .analysis import (
    get_spark_session, 
    analysis_df,
)

from .schema import (
    set_schema
)

@debug_msg
def run_preana(*args,**kwargs):
    log.info(f"Running 'run_preana' command with args: {args} and kwargs: {kwargs}")
    # Set the schema if provided
    if 'analysis_df' in kwargs.keys():
        if 'data_schema' in kwargs['analysis_df'].keys():
            log.info(f"Setting schema: {kwargs['analysis_df']['data_schema']}")
            set_schema(kwargs['analysis_df']['data_schema'])

@debug_msg
def run_show(*args, **kwargs):
    run_preana(*args,**kwargs)
    log.info(f"Running 'show' command with args: {args} and kwargs: {kwargs}")
    spark_sess = get_spark_session(**kwargs['spark'])
    df = analysis_df(spark_sess, *args, **kwargs)
    df.limit(20).show()  # Display the DataFrame