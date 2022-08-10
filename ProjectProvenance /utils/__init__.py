
#
# Copyright (c) 2022 Resilinc, Inc., All Rights Reserved
#   Any use, modification, or distribution is prohibited
#   without prior written consent from Resilinc, Inc.
#

# stdlib imports
import os

import pyspark.sql
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import lit ,concat_ws
from typing import List
from pandas import DataFrame
from collections import defaultdict

def get_project_root() -> str:
    """
    Gives the Absolute path of the project where this utils package resides. This may give incorrect results if
    the file is placed somewhere else.
    :return: Project Root

    """
    return os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))


def csv_to_pyspark_df(spark_session: SparkSession, csv_file_path: str) -> pyspark.sql.DataFrame:
    """
    Read and create PySpark DataFrame from a CSV file.
    :param spark_session: PySpark Session
    :param csv_file_path: File path of the CSV to read.
    :return: PySpark DataFrame Object
    :raises: TypeError
    :raises: Exception
    :raises AnalysisException
    """
    if spark_session and csv_file_path and isinstance(spark_session, SparkSession) and isinstance(csv_file_path, str):
        try:
            return spark_session.read.csv(csv_file_path, header=True)
        except TypeError:
            raise TypeError("File Path not in correct format.")
        except AnalysisException as ae:
            raise ae
    raise TypeError("Spark Session not provided/CSV file path not in correct type.")


def add_column_to_dataframe(dataframe: pyspark.sql.DataFrame, column_to_add: str, column_config: dict) -> DataFrame:
    """
    Adds a column to PySpark DataFrame with its values.
    :param dataframe: PySpark Dataframe Object
    :param column_to_add: Column name to add
    :param column_config: Dict with column values {'column_index': 'column_value', ...}
    :return: Pandas DataFrame Object
    """
    pandas_dataframe = None
    if dataframe and dataframe.count() > 0 and column_config and column_to_add:
        dataframe_with_new_column = dataframe.withColumn(column_to_add, lit(''))
        pandas_dataframe = dataframe_with_new_column.toPandas()
        for column_index, column_value in column_config.items():
            pandas_dataframe.at[column_index, column_to_add] = column_value
    elif dataframe and dataframe.count() > 0:
        pandas_dataframe = dataframe.toPandas()
    return pandas_dataframe


def dataframe_to_excel(dataframes: List[DataFrame], path: str, filename: str, sheet_names: List[str] = list()) -> bool:
    """
    Converts a list of Pandas dataframes to Excel
    :param dataframes: List of Pandas Dataframe Objects
    :param path: Path where Excel will be saved
    :param filename: Excel filename with extension (.xlsx)
    :param sheet_names: List of Sheet names
    :return: Boolean flag to check status
    """
    if path and filename and dataframes and len(dataframes) > 0:
        local_path = '{0}/{1}'.format(path, filename)
        with pd.ExcelWriter(local_path) as writer:
            try:
                for idx, dataframe in enumerate(dataframes):
                    if dataframe is not None and not dataframe.empty:
                        sheet_name = sheet_names[idx] if sheet_names and sheet_names[idx] else ('Sheet_{0}'.format(idx))
                        dataframe.to_excel(writer, sheet_name=sheet_name, encoding='utf-8', index=False)
                return True
            except Exception as exception:
                print('Exception: {}'.format(exception))
    return False


def sourcing_error_and_warnings_df(self,error,warnings):
    '''
    Merging all error and warnings to error and warnings df
    this method will add error and warnings to seprate
    self.error and self.warnings variable as pandas df
    '''

    error_schema = ["sourcing","qualification_status","part_partner_err","annual_spend_on_partner_part","partner_part","part_err","part_custom_defined_risk_score_1",'part_custom_defined_risk_score_2'\
                ,'part_custom_defined_risk_score_3','part_custom_defined_risk_score_4','part_custom_defined_risk_score_5']
    warning_schema = ['partner_part_annual_spend_on_partner_part','part_desc']

    error = error.toDF(error_schema,sampleRatio=0.01)
    warnings = warnings.toDF(warning_schema,sampleRatio=0.01)
    self.error = error.select(concat_ws('||',*error.columns).alias('error')).toPandas()
    self.warnings = warnings.select(concat_ws('||',*warnings.columns).alias('warnings')).toPandas()

"""
Utility function for sourcing tab
"""
def is_valid_risk(val):
        if not is_float(val):
            return False
        risk = round(float(str(val).strip()),2)
        if risk >= 1 and risk <= 10:
            return True
        return False

def is_float(val):
    try:
        float(str(val));
        return True;
    except Exception as ex:
        return False

        
def is_valid_revenue_between_range(val, start, end):
    val = str(val).replace(',','').replace('$','')
    if is_float(val):
#         val = round(float(val), 4)
        val = float(val)
        return (val >= start and val < end)
    del val
    return False


def comb(i,j,run=None):
    part_desc_map = defaultdict(set)
    if run == 'spend':
        i,j = j,i
    desc_set = part_desc_map[j]
    if(len(desc_set)<=1):
        if type(i)==str:
            i=i.lower()
        desc_set.add(i)
    del part_desc_map
    return desc_set
