
#
# Copyright (c) 2022 Resilinc, Inc., All Rights Reserved
#   Any use, modification, or distribution is prohibited
#   without prior written consent from Resilinc, Inc.
#

import unittest

# Third Party Imports
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.utils import AnalysisException

# Local Imports.
from Log import log_global

from utils import csv_to_pyspark_df
LOGGER = log_global()
spark = SparkSession \
    .builder.config('spark.driver.memory', '12g') \
    .appName("CSVReader").master("local[*]") \
    .getOrCreate()


class GenericUtilsTest(unittest.TestCase):
    def test_csv_to_pyspark_df_simple(self):
        df: DataFrame = csv_to_pyspark_df(spark, '../data/csvs/esea_master_dmg_demos.part1.csv')
        self.assertEqual(True, isinstance(df, DataFrame))  # add assertion here

    def test_csv_to_pyspark_df_file_not_found(self):
        try:
            df: DataFrame = csv_to_pyspark_df(spark, '../data/csvs/esea_master_dmg_demos.part1.cs')
        except AnalysisException as re:
            self.assertEqual(True, True)

    def test_csv_to_pyspark_df_type_error(self):
        try:
            df: DataFrame = csv_to_pyspark_df(spark, None)
        except TypeError as t:
            self.assertEqual(True, True)


if __name__ == '__main__':
    unittest.main()
