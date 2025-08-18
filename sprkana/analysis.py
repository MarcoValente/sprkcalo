import re
import logging as log
from typing import Union
from .message import (
    debug_msg,
    execute_msg,
    check_has_kwarg,
)
from .schema import (
    get_schema,
    add_to_schema
)
from .utils import (
    get_nested_value
)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from copy import deepcopy

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

def is_colname_in_schema(col_name,schema=None):
    if schema is None:
        schema = get_schema()
    ret = get_nested_value(schema, col_name, ret_none=True)
    return ret is not None

def check_colname_in_schema(col_name,*args,raise_err=False,**kwargs):
    for cname in [col_name] + list(args):
        ret = is_colname_in_schema(cname,**kwargs)
        if not ret:
            if raise_err:
                log.error(f'Impossible to find col_name=\'{cname}\' inside schema...')
                raise ValueError
            else:
                log.warning(f'Impossible to find col_name=\'{cname}\' inside schema...')

def colname_from_schema(col_name, schema=None):
    if schema is None:
        schema = get_schema()
    return get_nested_value(schema, col_name)

def colname_from_schema_and_check(col_name, *args, schema=None, **kwargs):
    check_colname_in_schema(col_name,schema=schema,**kwargs)
    return colname_from_schema(col_name, *args, schema=schema, **kwargs)

def col_from_schema(col_name, schema=None):
    return F.col(colname_from_schema(col_name, schema=schema))

def col_from_schema_and_check(col_name,*args,schema=None,**kwargs):
    check_colname_in_schema(col_name,schema=schema,**kwargs)
    return col_from_schema(col_name, *args, schema=schema, **kwargs)

@check_has_kwarg('out_name_schema','count_col_name')
def op_add_jets_n(df, *args, 
                  out_name_schema='', count_col_name='', **kwargs):
    # Assuming jets/jet_pt is the path to the jet pt column in the schema
    add_to_schema(out_name_schema)
    df = df.withColumn(colname_from_schema(out_name_schema), F.size(col_from_schema_and_check(count_col_name)))
    return df

@check_has_kwarg('col_names')
def op_drop(df, *args, col_names= [], **kwargs):
    return df.drop(*[col_from_schema_and_check(cname) for cname in col_names])

@check_has_kwarg('col_name_tosort','col_names_toadd','indices_values')
def op_add_lead_obj(df, 
                    *args,
                    col_name_tosort:str='', 
                    col_names_toadd : list =[], 
                    indices_values:list = [0], 
                    store_flattened:bool=True, 
                    store_struct:bool=False,
                    **kwargs):
    _cname_tosort = colname_from_schema_and_check(col_name_tosort)
    _cnames = [colname_from_schema_and_check(cname) for cname in col_names_toadd]
    _cnames_plus_tosort = ([_cname_tosort] if _cname_tosort not in _cnames else []) + _cnames
    df = df \
        .withColumn("zipped", F.arrays_zip(*(F.col(cname) for cname in _cnames_plus_tosort))) \
        .withColumn("sorted", F.expr(f"array_sort(zipped, (left, right) -> case when left.{_cname_tosort} > right.{_cname_tosort} then -1 when left.{_cname_tosort} < right.{_cname_tosort} then 1 else 0 end)"))
    for i in indices_values:
        df = df.withColumn(f"{_cname_tosort}_struct_{i}", 
                           F.when(F.size(F.col("sorted")) >= i+1, F.col("sorted")[i]).otherwise(F.struct(*(F.lit(-999).alias(cname) for cname in _cnames_plus_tosort)))
                           )
    df=df.drop('zipped','sorted')
    if store_flattened:
        for i in indices_values:
            for sch_i,cname in enumerate(_cnames):
                add_to_schema(f'{col_names_toadd[sch_i]}_{i}')
                df = df.withColumn(f'{cname}_{i}', F.col(f"{_cname_tosort}_struct_{i}.{cname}"))
    if not store_struct:
        df=df.drop(*(f"{_cname_tosort}_struct_{i}" for i in indices_values))
    else:
        add_to_schema(f'{_cname_tosort}_struct_{i}')
    return df

@check_has_kwarg('col_name','value')
def op_filt_greateq(df, *args, 
                          col_name:str='', value:Union[int,float]=-1, **kwargs):
    df = df.filter(col_from_schema_and_check(col_name) >= value)
    return df

@check_has_kwarg('col_name','value')
def op_filt_great(df, *args, 
                          col_name:str='', value:Union[int,float]=-1, **kwargs):
    df = df.filter(col_from_schema_and_check(col_name) > value)
    return df

@check_has_kwarg('col_name','value')
def op_filt_less(df, *args, 
                          col_name:str='', value:Union[int,float]=99999, **kwargs):
    df = df.filter(col_from_schema_and_check(col_name) < value)
    return df

@check_has_kwarg('col_name','value')
def op_filt_lesseq(df, *args, 
                          col_name:str='', value:Union[int,float]=99999, **kwargs):
    df = df.filter(col_from_schema_and_check(col_name) <= value)
    return df

@check_has_kwarg('var1_name','var2_name', 'out_name_schema')
def op_add_delta(df, *args, var1_name='', var2_name='', out_name_schema='', **kwargs):
    var1_col = col_from_schema_and_check(var1_name)
    var2_col = col_from_schema_and_check(var2_name)
    delta = var1_col - var2_col
    add_to_schema(out_name_schema)
    df=df.withColumn(colname_from_schema_and_check(out_name_schema),delta)
    return df

@check_has_kwarg('eta1_name','phi1_name', 'out_name_schema')
def op_add_deltaR(df, *args, eta1_name='', phi1_name='', eta2_name='', phi2_name='', out_name_schema='', **kwargs):
    if eta2_name=='':
        eta2_name=eta1_name
    if phi2_name=='':
        phi2_name=phi1_name
    eta1_col = col_from_schema_and_check(eta1_name)
    phi1_col = col_from_schema_and_check(phi1_name)
    eta2_col = col_from_schema_and_check(eta2_name)
    phi2_col = col_from_schema_and_check(phi2_name)
    d_eta = eta2_col - eta1_col
    d_phi = phi2_col - phi1_col
    add_to_schema(out_name_schema)
    df=df.withColumn(colname_from_schema_and_check(out_name_schema),F.sqrt(d_eta*d_eta + d_phi*d_phi))
    return df

_ops_dict = {
    #Add/remove columns
    'add_jets_n' : op_add_jets_n,
    'add_lead_obj' : op_add_lead_obj,
    'add_delta' : op_add_delta,
    'add_deltaR' : op_add_deltaR,
    'drop' : op_drop,
    #Filters
    'filt_great' : op_filt_great,
    'filt_greateq' : op_filt_greateq,
    'filt_less' : op_filt_less,
    'filt_lesseq' : op_filt_lesseq,
}

def get_operation(*args,name='',**kwargs):
    if name in _ops_dict.keys():
        log.info(f"Retrieved operation '{name}' with args = {args} and kwargs = {kwargs}")
        return _ops_dict[name]
    else:
        log.error(f"Unknown operation '{name}'")
        raise ValueError
    
@check_has_kwarg('matching','matching/schema_path', 'matching/vars_to_match', 'inputsToMatch')
def matched_df(spark_sess, df, *args, operations=[], how='inner', matching_suffix='_ref', sep='/', **kwargs):
    log.info(f"Performing matching to files \'{kwargs['inputsToMatch']}\'...")
    df_tomatch = spark_sess.read.parquet(*kwargs['inputsToMatch'])
    
    if 'apply_same_ops' in kwargs['matching'].keys() and kwargs['matching']['apply_same_ops']:
        log.info('Performing the same operations on dataframe to match as requested...')
        #Apply _analyze_df by forcing nevents = -1 and inputsToMatch=None from defaults
        new_kwargs = deepcopy(kwargs)
        new_kwargs['matching'] = None
        df_tomatch = _analyze_df(None, df_tomatch, *args, operations = operations, **new_kwargs)
    else:
        log.warning('I am not applying the same operations as the main dataframe for the matching dataframe. This might cause problems...')
    
    col_names_to_match = [colname_from_schema_and_check(cname) for cname in kwargs['matching']['vars_to_match']]
    col_names_to_rename = [ c for c in df_tomatch.schema.names if c not in col_names_to_match]
    col_names_renamed = [ c+(kwargs['matching']['match_suffix'] if 'match_suffix' in kwargs['matching'].keys() else matching_suffix) for c in col_names_to_rename]
    columns = [F.col(cname) for cname in col_names_to_match] + \
              [ F.col(cname).alias(cname_renamed) for cname,cname_renamed in zip(col_names_to_rename,col_names_renamed) ]
    df_tomatch = df_tomatch.select(*columns)

    for cname in col_names_renamed:
         add_to_schema(sep.join([kwargs['matching']['schema_path'],cname]))
             
    df = df.join(df_tomatch, on=col_names_to_match, how=how)
    return df

@debug_msg
def _analyze_df(spark_sess, df, *args, operations=[], nevents=-1, matching = None, inputsToMatch=None, **kwargs):
    log.info(f"Defining DataFrame with initial df={df}")

    for op in operations:
        log.info(f'Adding operation {op}')
        op_func = get_operation(**op)
        df = op_func(df,*args, **op)

    if nevents>0:
        df = df.limit(nevents)

    if matching is not None:
        df = matched_df(spark_sess, df, operations=operations, matching=matching, inputsToMatch=inputsToMatch)
        if 'operations' in matching.keys():
            for op in matching['operations']:
                log.info(f'Adding operation {op} after matching...')
                op_func = get_operation(**op)
                df = op_func(df,*args, **op)
            
    return df

@debug_msg
def analysis_df(spark_sess, *args, analysis_df={}, inputs=[], inputsToMatch=None, **kwargs):
    log.info(f"Creating DataFrame with spark_sess={spark_sess}, inputs={inputs}, analysis_df={analysis_df}")
    # Here you would typically read data into a DataFrame
    df = spark_sess.read.parquet(*inputs)
    df = _analyze_df(spark_sess, df, *args, inputsToMatch=inputsToMatch, **analysis_df)
    return df