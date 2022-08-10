
#
# Copyright (c) 2022 Resilinc, Inc., All Rights Reserved
#   Any use, modification, or distribution is prohibited
#   without prior written consent from Resilinc, Inc.
#

import inspect
import os
import sys
import time
from pyspark.sql import SparkSession

parent_folder = os.path.realpath(os.path.abspath(os.path.join(os.path.split(inspect.getfile(
    inspect.currentframe()))[0], "..", '..', '..')))
if parent_folder not in sys.path:
    sys.path.insert(0, parent_folder)


class Ingester:
    def __init__(self):
        print('============ ran once Ingester ================')
        self.spark_session = SparkSession \
            .builder.config('spark.driver.memory', '12g') \
            .appName("ProjectProvenance").master("local[*]").getOrCreate()

    def get_df(self):

        # A CSV dataset is pointed to by path.
        # The path can be either a single CSV file or a directory of CSV files
        file_to_read = "../data/csvs/1._Partners.csv"
        # print(f'variable is : {Variable.get("spark_session")}')
        # spark_session = get_spark_session()

        s = time.time()
        df = self.spark_session.read.csv(file_to_read, header=True)
        e = time.time()
        df.rdd.saveAsPickleFile('abcd.df')
        return 0
