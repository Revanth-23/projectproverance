
#
# Copyright (c) 2022 Resilinc, Inc., All Rights Reserved
#   Any use, modification, or distribution is prohibited
#   without prior written consent from Resilinc, Inc.
#
import inspect
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import when
from airflow.models import Variable
parent_folder = os.path.realpath(os.path.abspath(os.path.join(os.path.split(inspect.getfile(
    inspect.currentframe()))[0], "..", '..', '..')))
if parent_folder not in sys.path:
    sys.path.insert(0, parent_folder)


class CommonValidations:

    @staticmethod
    def check_not_null(spark_session, df, column_name) -> tuple:
        df2 = df.withColumn("error",
                            when(df[column_name].isNull(), 'abcd'))
        # self.spark_session.sql("""SELECT * FROM global_temp.partner where partner_name is null""").show()
        print(df2)

    def lookup_check(self, sheet_column_to_check: dict, sheet_column_to_be_checked: dict,
                     error_message: str = 'lookup error') -> tuple:
        # TODO: check parameter name
        pass


class Validation:
    def validate(self):
        pass


class NotNullValidation(Validation):

    def __init__(self, columns_to_check: list, temp_view_to_check, spark_session: SparkSession):
        pass

    def validate(self):
        pass


class ExpressionEngine:
    def parse(self):
        pass


class ExpressionAgent:
    pass

