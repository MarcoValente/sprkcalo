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
    get_nested_value,
    get_nested_values,
)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from copy import deepcopy
import pandas as pd
from pyspark.sql.functions import pandas_udf, struct
from pyspark.sql.types import StructType, StructField, ArrayType, FloatType, IntegerType
import numpy as np

_default_dummy_float = -999.

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
    ret = get_nested_values(schema, col_name, ret_none=True)
    return ret is not None

def check_colname_in_schema(col_name_expr,*args,raise_err=False,**kwargs):
    for cname in [col_name_expr] + list(args):
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

def colnames_from_schema(col_name_exrp, schema=None):
    if schema is None:
        schema = get_schema()
    return get_nested_values(schema, col_name_exrp)

def colname_from_schema_and_check(col_name, *args, schema=None, **kwargs):
    check_colname_in_schema(col_name,schema=schema,**kwargs)
    return colname_from_schema(col_name, *args, schema=schema, **kwargs)

def colnames_from_schema_and_check(col_name_exrp, *args, schema=None, **kwargs):
    check_colname_in_schema(col_name_exrp,schema=schema,**kwargs)
    return colnames_from_schema(col_name_exrp, *args, schema=schema, **kwargs)

def col_from_schema(col_name, schema=None):
    return F.col(colname_from_schema(col_name, schema=schema))

def cols_from_schema(col_name_expr, schema=None):
    return [F.col(cname) for cname in colnames_from_schema(col_name_expr, schema=schema)]

def col_from_schema_and_check(col_name,*args,schema=None,**kwargs):
    check_colname_in_schema(col_name,schema=schema,**kwargs)
    return col_from_schema(col_name, *args, schema=schema, **kwargs)

def cols_from_schema_and_check(col_name_expr,*args,schema=None,**kwargs):
    check_colname_in_schema(col_name_expr,schema=schema,**kwargs)
    return cols_from_schema(col_name_expr, *args, schema=schema, **kwargs)

@check_has_kwarg('out_name_schema','count_col_name')
def op_add_jets_n(df, *args, 
                  out_name_schema='', count_col_name='', **kwargs):
    # Assuming jets/jet_pt is the path to the jet pt column in the schema
    add_to_schema(out_name_schema)
    df = df.withColumn(colname_from_schema(out_name_schema), F.size(col_from_schema_and_check(count_col_name)))
    return df

@check_has_kwarg('col_names_expr')
def op_drop(df, *args, col_names_expr= [], **kwargs):
    for cname_expr in col_names_expr:
        df = df.drop(*cols_from_schema_and_check(cname_expr))
    return df

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
                           F.when(F.size(F.col("sorted")) >= i+1, F.col("sorted")[i]).otherwise(F.struct(*(F.lit(_default_dummy_float).alias(cname) for cname in _cnames_plus_tosort)))
                           )
    df=df.drop('zipped','sorted')
    if store_flattened:
        for i in indices_values:
            for sch_i,cname in enumerate(_cnames):
                add_to_schema(f'{col_names_toadd[sch_i]}_{i}')
                df = df.withColumn(colname_from_schema_and_check(f'{col_names_toadd[sch_i]}_{i}'), F.col(f"{_cname_tosort}_struct_{i}.{cname}"))
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

def match_dR(eta1_name='', phi1_name='', eta2_name='', phi2_name=''):

    def delta_phi(phi1, phi2):
        """Compute delta phi in [-pi, pi]."""
        dphi = phi1 - phi2
        return (dphi + np.pi) % (2*np.pi) - np.pi
    
    def compute_closest_j2(j1_eta, j1_phi, j2_eta, j2_phi):
        j1_eta, j1_phi = np.array(j1_eta), np.array(j1_phi)
        j2_eta, j2_phi = np.array(j2_eta), np.array(j2_phi)

        # Compute pairwise Δη and Δφ
        deta = j1_eta[:, None] - j2_eta[None, :]
        dphi = delta_phi(j1_phi[:, None], j2_phi[None, :])

        # ΔR matrix
        deltaR = np.sqrt(deta**2 + dphi**2)

        # For each j1, find index of closest j2
        closest_j2_idx = deltaR.argmin(axis=1)
        closest_j2_dr = deltaR.min(axis=1)

        return closest_j2_idx.tolist(), closest_j2_dr.tolist(), deltaR.tolist()
    
    @pandas_udf(StructType([
        StructField("closest_idx", ArrayType(IntegerType())),
        StructField("closest_dr", ArrayType(FloatType())),
        StructField("dr_matrix", ArrayType(ArrayType(FloatType()))),
    ]))
    def _match_dR(s: pd.Series) -> pd.Series:
        s[["closest_idx", "closest_dr", "dr_matrix"]]=s.apply(
            lambda row: pd.Series(compute_closest_j2(row[eta1_name], row[phi1_name], row[eta2_name], row[phi2_name])),
            axis=1
        )
        return s
    return _match_dR

@check_has_kwarg('eta1_name','phi1_name','eta2_name','phi2_name', 'col_vals_toadd', 'col_names_toadd')
def op_add_deltaR_vars(df, *args, eta1_name='', phi1_name='', eta2_name='', phi2_name='', col_vals_toadd=[], col_names_toadd=[], dr_to_match=0.1, **kwargs):

    df = df.withColumn(
        "matched",
        match_dR(eta1_name=colname_from_schema_and_check(eta1_name),
                 phi1_name=colname_from_schema_and_check(phi1_name),
                 eta2_name=colname_from_schema_and_check(eta2_name),
                 phi2_name=colname_from_schema_and_check(phi2_name),
                 )(
                    struct(
                    col_from_schema_and_check(eta1_name),
                    col_from_schema_and_check(phi1_name),
                    col_from_schema_and_check(eta2_name),
                    col_from_schema_and_check(phi2_name),
                    )
            )
    )
    df = df.withColumn('closest_idx',df['matched'].closest_idx)
    df = df.withColumn('closest_dr',df['matched'].closest_dr)
    df = df.drop('matched')

    cnames_to_add_from_schema = [colname_from_schema_and_check(c) for c in col_vals_toadd]

    df = df.withColumn("zipped", F.arrays_zip(*(F.col(cname) for cname in cnames_to_add_from_schema)))
    df = df.withColumn(
        "zipped_reordered",
        F.expr(f"""
        transform(
            sequence(1, size(closest_idx)),
            i -> CASE 
                    WHEN closest_dr[i-1] < {dr_to_match} THEN element_at(zipped, closest_idx[i-1] + 1)
                    ELSE named_struct({','.join([f"'{a}',{b}" for a,b in zip(cnames_to_add_from_schema,[_default_dummy_float]*len(cnames_to_add_from_schema))])}
                    )
                 END
        )
    """)
    )
    
    for cname_tostore, cname_toadd in zip(col_names_toadd,col_vals_toadd):
        cname=colname_from_schema_and_check(cname_toadd)
        add_to_schema(cname_tostore)
        df = df.withColumn(colname_from_schema_and_check(cname_tostore),F.col(f"zipped_reordered.{cname}"))

    df=df.drop(*["zipped","zipped_reordered","closest_idx",'closest_dr'])

    return df


@check_has_kwarg('eta1_name','phi1_name','eta2_name','phi2_name','col_name_toadd')
def op_add_deltaR_size(df, *args, eta1_name='', phi1_name='', eta2_name='', phi2_name='', col_name_toadd='', dr_to_match=0.4, **kwargs):

    df = df.withColumn(
        "matched",
        match_dR(eta1_name=colname_from_schema_and_check(eta1_name),
                 phi1_name=colname_from_schema_and_check(phi1_name),
                 eta2_name=colname_from_schema_and_check(eta2_name),
                 phi2_name=colname_from_schema_and_check(phi2_name),
                 )(
                    struct(
                    col_from_schema_and_check(eta1_name),
                    col_from_schema_and_check(phi1_name),
                    col_from_schema_and_check(eta2_name),
                    col_from_schema_and_check(phi2_name),
                    )
            )
    )
    df = df.withColumn('dr_matrix',df['matched'].dr_matrix)
    df = df.withColumn(
        "dr_matrix_matched",
        F.expr(f"transform(dr_matrix, inner -> transform(inner, x -> x < {dr_to_match}))")
    )
    add_to_schema(col_name_toadd)
    df = df.withColumn(
        colname_from_schema_and_check(col_name_toadd),
        F.transform(
            F.col("dr_matrix_matched"),
            lambda inner: F.aggregate(
                inner,
                F.lit(0),
                lambda acc, x: acc + F.when(x, 1).otherwise(0)
            )
        )
    )
    df = df.drop('matched','dr_matrix','dr_matrix_matched')

    return df

@check_has_kwarg('expressions')
def op_select(df, *args, expressions=[], **kwargs):
    cnames=[]
    for expr in expressions:
        cnames_tmp=colnames_from_schema_and_check(expr)
        for cname_tmp in cnames_tmp: #Make sure that schema column is in schema. Otherwise ignore
            if cname_tmp in df.columns:
                cnames+=[cname_tmp]
            else:
                log.warning(f"The column matched to be selected called '{cname_tmp}' was not found in the main df.columns ({df.columns}). Skipping...")
    return df.select(*cnames)

@check_has_kwarg('num_name','den_name', 'out_name_schema')
def op_add_division(df, *args, num_name='', den_name='', out_name_schema='', **kwargs):
    var1_colname = colname_from_schema_and_check(num_name)
    var2_colname = colname_from_schema_and_check(den_name)
    var1_col = col_from_schema_and_check(num_name)
    var2_col = col_from_schema_and_check(den_name)
    # division = F.when(var2_col != 0, var1_col / var2_col).otherwise(F.lit(_default_dummy_float))
    add_to_schema(out_name_schema)
    df=df.withColumn(colname_from_schema_and_check(out_name_schema),
        F.transform(
            F.arrays_zip(*[var1_col, var2_col]),
            lambda x: x[var1_colname] / x[var2_colname]
        )
    )
    
    return df

_ops_dict = {
    #Add/remove columns
    'add_jets_n' : op_add_jets_n,
    'add_lead_obj' : op_add_lead_obj,
    'add_delta' : op_add_delta,
    'add_deltaR' : op_add_deltaR,
    'add_deltaR_vars' : op_add_deltaR_vars,
    'add_deltaR_size' : op_add_deltaR_size,
    'add_division' : op_add_division,
    'drop' : op_drop,
    'select' : op_select,
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
    
@check_has_kwarg('matching','matching/schema_path', 'matching/vars_to_match', 'inputstomatch')
def matched_df(spark_sess, df, *args, operations=[], how='inner', matching_appendix='_ref', sep='/', **kwargs):
    log.info(f"Performing matching to files \'{kwargs['inputstomatch']}\'...")
    df_tomatch = spark_sess.read.parquet(*kwargs['inputstomatch'])
    
    if 'apply_same_ops' in kwargs['matching'].keys() and kwargs['matching']['apply_same_ops']:
        log.info('Performing the same operations on dataframe to match as requested...')
        #Apply _analyze_df by forcing nevents = -1 and inputstomatch=None from defaults
        new_kwargs = deepcopy(kwargs)
        new_kwargs['matching'] = None
        df_tomatch = _analyze_df(None, df_tomatch, *args, operations = operations, **new_kwargs)
    else:
        log.warning('I am not applying the same operations as the main dataframe for the matching dataframe. This might cause problems...')
    
    col_names_to_match = [colname_from_schema_and_check(cname) for cname in kwargs['matching']['vars_to_match']]
    col_names_to_rename = [ c for c in df_tomatch.schema.names if c not in col_names_to_match]
    col_names_renamed = [ c+(kwargs['matching']['match_suffix'] if 'match_suffix' in kwargs['matching'].keys() else matching_appendix) for c in col_names_to_rename]
    columns = [F.col(cname) for cname in col_names_to_match] + \
              [ F.col(cname).alias(cname_renamed) for cname,cname_renamed in zip(col_names_to_rename,col_names_renamed) ]
    df_tomatch = df_tomatch.select(*columns)

    for cname in col_names_renamed:
         add_to_schema(sep.join([kwargs['matching']['schema_path'],cname]))
             
    df = df.join(df_tomatch, on=col_names_to_match, how=how)
    return df

@debug_msg
def _analyze_df(spark_sess, df, *args, operations=[], nevents=-1, matching = None, inputstomatch=None, **kwargs):
    log.info(f"Defining DataFrame with initial df={df}")

    for op in operations:
        log.info(f'Adding operation {op}')
        op_func = get_operation(**op)
        df = op_func(df,*args, **op)

    if nevents>0:
        df = df.limit(nevents)

    if all(x is not None for x in [matching, inputstomatch]):
        df = matched_df(spark_sess, df, operations=operations, matching=matching, inputstomatch=inputstomatch)
        if 'operations' in matching.keys():
            for op in matching['operations']:
                log.info(f'Adding operation {op} after matching...')
                op_func = get_operation(**op)
                df = op_func(df,*args, **op)
            
    return df

@debug_msg
def analysis_df(spark_sess, *args, analysis_df={}, inputs=[], inputstomatch=None, **kwargs):
    log.info(f"Creating DataFrame with spark_sess={spark_sess}, inputs={inputs}, analysis_df={analysis_df}")
    # Here you would typically read data into a DataFrame
    df = spark_sess.read.parquet(*inputs)
    df = _analyze_df(spark_sess, df, *args, inputstomatch=inputstomatch, **analysis_df)
    return df