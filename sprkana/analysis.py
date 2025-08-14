import re
import logging as log
from .message import (
    debug_msg,
    execute_msg,
)
from .schema import (
    get_schema
)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

@debug_msg
def get_spark_session(*args, **kwargs):
    log.info(f"Creating Spark session with args: {args} and kwargs: {kwargs}")
    # return spark

    spark_builder = SparkSession.builder
    # Apply all dictionary configs
    for key, value in kwargs.items():
        spark_builder = spark_builder.config(key, value)

    #Create the Spark session
    spark = spark_builder.getOrCreate()

    log.info(f"Spark session created successfully : {spark}")

    conf_dict = spark.sparkContext.getConf().getAll()

    log.info("Spark configuration:")
    for k, v in conf_dict:
        log.info(f"{k} = {v}")
    return spark

def col_from_schema(col_name, schema=None):
    if schema is None:
        schema = get_schema()
    def get_nested_value(data, path, sep="/"):
        keys = path.split(sep)
        current = data
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                # Return original path if lookup fails
                log.debug(f"Path '{path}' not found in schema, returning original path: {path}")
                return path
        log.debug(f"Found path '{path}' in schema, returning column name '{current}'")
        return current
    # Use regex to find the column in the schema
    
    return F.col(get_nested_value(schema, col_name))

def add_jets_n(df, **kwargs):
    log.info("Adding jet_n column")
    # Assuming jets/jet_pt is the path to the jet pt column in the schema
    df = df.withColumn("jet_n", F.size(col_from_schema(kwargs['count_col_name'])))
    return df

def _analyze_df(df, **kwargs):
    log.info(f"Defining DataFrame with kwargs: {kwargs}")
    
    for operation in kwargs['operations']:
        if operation['name'] == 'add_jets_n':
            df = add_jets_n(df,**operation)
        # elif operation['name'] == 'select_atleast_jet_n':
        #     value = operation.get('value', 2)
        #     log.debug(f"Selecting events with at least {value} jets")
        #     df = df.filter(F.col("jet_n") >= value)
        # elif operation['name'] == 'add_lead_jet':
        #     log.debug("Adding lead jet column")
        #     df = df.withColumn("lead_jet_pt", F.max(col_from_schema("jets/jet_pt")).over(F.Window.partitionBy()))
        else:
            log.error(f"Unknown operation: {operation['name']}")
            return None
    
    # if kwargs['operations']['add_jets_n'] :
    #     log.debug("Adding jet_n column")
    #     df = df.withColumn("jet_n", F.size(col_from_schema("jets/jet_pt")))
    return df

@debug_msg
def analysis_df(spark_sess, *args, **kwargs):
    log.info(f"Creating DataFrame with args: {args} and kwargs: {kwargs}")
    # Here you would typically read data into a DataFrame
    df = spark_sess.read.parquet(*kwargs['inputs'])
    df = _analyze_df(df, **kwargs['analysis_df'])
    return df