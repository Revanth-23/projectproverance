#
# Copyright (c) 2022 Resilinc, Inc., All Rights Reserved
#   Any use, modification, or distribution is prohibited
#   without prior written consent from Resilinc, Inc.
#

# Third Party Import
from prefect import resource_manager
from pyspark import SparkConf
from pyspark.sql import SparkSession


@resource_manager
class SparkCluster:
    def __init__(self, conf: SparkConf = SparkConf()):
        self.conf = conf

    def setup(self) -> SparkSession:
        return SparkSession.builder.config(conf=self.conf).getOrCreate()

    # noinspection PyMethodMayBeStatic
    def cleanup(self, spark: SparkSession):
        spark.stop()
