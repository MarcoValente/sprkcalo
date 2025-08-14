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

def colname_from_schema(col_name, schema=None):
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
    return get_nested_value(schema, col_name)

def col_from_schema(col_name, schema=None):
    return F.col(colname_from_schema(col_name, schema=schema))

def op_add_jets_n(df, *args, 
                  out_name_schema='', count_col_name='', **kwargs):
    # Assuming jets/jet_pt is the path to the jet pt column in the schema
    df = df.withColumn(colname_from_schema(out_name_schema), F.size(col_from_schema(count_col_name)))
    return df

def op_add_lead_obj(df, 
                    *args,
                    col_name_tosort:str='', 
                    col_names_toadd : list =[], 
                    indices_values:list = [0], 
                    store_flattened:bool=True, 
                    store_struct:bool=False,
                    **kwargs):
    _cname_tosort = colname_from_schema(col_name_tosort)
    _cnames = [_cname_tosort]+[colname_from_schema(cname) for cname in col_names_toadd]
    df = df \
        .withColumn("zipped", F.arrays_zip(*(F.col(cname) for cname in _cnames))) \
        .withColumn("sorted", F.expr(f"array_sort(zipped, (left, right) -> case when left.{_cname_tosort} > right.{_cname_tosort} then -1 when left.{_cname_tosort} < right.{_cname_tosort} then 1 else 0 end)"))
    for i in indices_values:
        # df = df.withColumn(f"{_cname_tosort}_struct_{i}", F.col("sorted")[i])
        df = df.withColumn(f"{_cname_tosort}_struct_{i}", 
                           F.when(F.size(F.col("sorted")) >= i+1, F.col("sorted")[i]).otherwise(F.struct(*(F.lit(-999).alias(cname) for cname in _cnames)))
                           )
    df=df.drop('zipped','sorted')
    if store_flattened:
        for i in indices_values:
            for cname in _cnames:
                df = df.withColumn(f'{cname}_{i}', F.col(f"{_cname_tosort}_struct_{i}.{cname}"))
    if not store_struct:
        df=df.drop(*(f"{_cname_tosort}_struct_{i}" for i in indices_values))
    return df

def op_filt_greateq(df, *args, 
                          col_name:str='', value:int=-1, **kwargs):
    df = df.filter(col_from_schema(col_name) >= value)
    return df

def op_filt_great(df, *args, 
                          col_name:str='', value:int=-1, **kwargs):
    df = df.filter(col_from_schema(col_name) > value)
    return df

_ops_dict = {
    #Add columns
    'add_jets_n' : op_add_jets_n,
    'add_lead_obj' : op_add_lead_obj,
    #Filters
    'filt_great' : op_filt_great,
    'filt_greateq' : op_filt_greateq,
}

def get_operation(*args,name='',**kwargs):
    if name in _ops_dict.keys():
        log.info(f"Retrieved operation '{name}' with args = {args} and kwargs = {kwargs}")
        return _ops_dict[name]
    else:
        log.error(f"Unknown operation '{name}'")
        raise ValueError

def _analyze_df(df, *args, operations=[], **kwargs):
    log.info(f"Defining DataFrame with initial df={df}")
    
    for op in operations:
        log.info(f'Adding operation {op}')
        op_func = get_operation(**op)
        df = op_func(df,*args, **op)
    
    return df

@debug_msg
def analysis_df(spark_sess, *args, analysis_df={}, inputs=[], **kwargs):
    log.info(f"Creating DataFrame with spark_sess={spark_sess}, inputs={inputs}, analysis_df={analysis_df}")
    # Here you would typically read data into a DataFrame
    df = spark_sess.read.parquet(*inputs)
    df = _analyze_df(df, *args, **analysis_df)
    return df