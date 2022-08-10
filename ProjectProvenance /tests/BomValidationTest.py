#
# Copyright (c) 2022 Resilinc, Inc., All Rights Reserved
#   Any use, modification, or distribution is prohibited
#   without prior written consent from Resilinc, Inc.
#

from pyspark.sql import SparkSession

from utils import csv_to_pyspark_df
from validator.BomValidation import BomValidation

spark = SparkSession \
    .builder.config('spark.driver.memory', '12g') \
    .appName("CSVReader").master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext

bom_file_path = "../data/csvs/bom.csv"
bom_df = csv_to_pyspark_df(spark, bom_file_path)
new_column_names = [f"{column.lower().replace(' ', '_').replace('#', 'number')}" for column in bom_df.columns]
bom_df = bom_df.toDF(*new_column_names)

part_file_path = "../data/csvs/part_sourcing.csv"
part_df = csv_to_pyspark_df(spark, part_file_path)
new_column_names = [f"{column.lower().replace(' ', '_').replace('#', 'number')}" for column in part_df.columns]
part_df = part_df.toDF(*new_column_names)

product_file_path = "../data/csvs/product.csv"
product_df = csv_to_pyspark_df(spark, product_file_path)
new_column_names = [f"{column.lower().replace(' ', '_').replace('#', 'number')}" for column in product_df.columns]
product_df = product_df.toDF(*new_column_names)

bom_validation = BomValidation(bom_df)
bom_validation.validation(part_df, product_df)
