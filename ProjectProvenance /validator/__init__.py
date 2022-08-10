
#
# Copyright (c) 2022 Resilinc, Inc., All Rights Reserved
#   Any use, modification, or distribution is prohibited
#   without prior written consent from Resilinc, Inc.
#

from typing import Iterable, Any

import pyspark.sql
from pyspark.sql import SparkSession

from Log import log_global

#LOGGER = log_global()


class Validator:
    def validate(self):
        pass


class NotNullValidator(Validator):
    def __init__(self, spark_session: SparkSession, temp_view: str, columns: Iterable[Any]):
        self.spark_session: SparkSession = spark_session
        self.temp_view = temp_view
        self.columns: Iterable[Any] = columns

    def validate(self):
        if self.columns and isinstance(self.columns, Iterable) and self.temp_view:
            condition = ' and '.join(['{} is null'.format(column) for column in self.columns])

            query = """SELECT * FROM {temp_view} where {condition}""".format(temp_view=self.temp_view,
                                                                             condition=condition)
            #LOGGER.info("Query is : {}".format(query))
            df = self.spark_session.sql(query)
            df.show(10)


class EmailValidator(Validator):
    def __init__(self, spark_session: SparkSession, input_df: pyspark.sql.DataFrame, column: str):
        self.spark_session: SparkSession = spark_session
        self.input_df = input_df
        self.column = column

    def validate(self):
        return True


class LookupValidator(Validator):
    def __init__(self, spark_session: SparkSession, df_column_combination1: tuple, df_column_combination2: tuple):
        self.spark_session: SparkSession = spark_session
        self.df_column_combination1: tuple = df_column_combination1
        self.df_column_combination2: tuple = df_column_combination2

    def validate(self):
        if self.df_column_combination1 and self.df_column_combination2 and isinstance(
                self.df_column_combination1, tuple) and isinstance(self.df_column_combination2, tuple):
            pass
        else:
            pass

