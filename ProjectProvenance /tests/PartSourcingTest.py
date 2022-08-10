#
# Copyright (c) 2022 Resilinc, Inc., All Rights Reserved
#   Any use, modification, or distribution is prohibited
#   without prior written consent from Resilinc, Inc.
#

from pyspark.sql import SparkSession
import re
from utils import csv_to_pyspark_df
from validator.SourcingValidation import PartSourcing


spark = SparkSession \
    .builder.config('spark.driver.memory', '12g') \
    .appName("CSVReader").master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext

part_file_path = "../data/csvs/part_sourcing.csv"
part_df = csv_to_pyspark_df(spark, part_file_path)
# columns renaming
new_column_names = [re.sub(r"[-()\"#/@;:<>{}`+=~|.!?,]","",column).lower().strip().replace(" ","_") for column in part_df.columns]
part_df = part_df.toDF(*new_column_names)

sourcing_validation = PartSourcing(part_df)


error,warning = sourcing_validation.validation()

# this method will add error and warnings to seprate self.error and self.warnings dataframe 
# as pandas df
sourcing_validation.merger(error,warning)

