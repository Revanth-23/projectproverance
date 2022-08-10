#
# Copyright (c) 2022 Resilinc, Inc., All Rights Reserved
#   Any use, modification, or distribution is prohibited
#   without prior written consent from Resilinc, Inc.
#

from pyspark.sql import SparkSession

from Log import log_global
from utils import csv_to_pyspark_df
from validator.PartSiteMapValidation import PartSiteMapValidation

LOGGER = log_global()

spark = SparkSession \
    .builder.config('spark.driver.memory', '12g') \
    .appName("CSVReader").master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext


def replace_column_names(column):
    return column.lower().replace('  ', '_').replace(' ', '_').replace('#', 'number').replace('?', '').replace('/',
                                                                                                               '').replace(
        '-', '').replace(':', '').replace('(', '').replace(')', '')


part_site_map_file_path = "../data/csvs/8. Part - Site Map.csv"
part_site_map_df = csv_to_pyspark_df(spark, part_site_map_file_path)
LOGGER.info(f"part_site_map_df : {part_site_map_df}")
new_column_names = [f"{replace_column_names(column)}" for column in part_site_map_df.columns]
part_site_map_df = part_site_map_df.toDF(*new_column_names)
LOGGER.info(f"part_site_map_df after : {part_site_map_df}")

partner_file_path = "../data/csvs/1. Partners.csv"
partner_df = csv_to_pyspark_df(spark, partner_file_path)
LOGGER.info(f"partner_df : {partner_df}")
new_column_names = [f"{replace_column_names(column)}" for column in partner_df.columns]
partner_df = partner_df.toDF(*new_column_names)
LOGGER.info(f"partner_df after : {partner_df}")

part_file_path = "../data/csvs/2. Part sourcing.csv"
part_df = csv_to_pyspark_df(spark, part_file_path)
LOGGER.info(f"part_df : {part_df}")
new_column_names = [f"{replace_column_names(column)}" for column in part_df.columns]
part_df = part_df.toDF(*new_column_names)
LOGGER.info(f"part_df after : {part_df}")

site_file_path = "../data/csvs/6. Site List.csv"
site_df = csv_to_pyspark_df(spark, site_file_path)
LOGGER.info(f"site_df : {site_df}")
new_column_names = [f"{replace_column_names(column)}" for column in site_df.columns]
site_df = site_df.toDF(*new_column_names)
LOGGER.info(f"site_df after : {site_df}")

part_site_map_validation = PartSiteMapValidation(part_site_map_df)
part_site_map_validation.validation(partner_df, part_df, site_df)
