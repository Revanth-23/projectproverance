from pyspark.sql import SparkSession
from utils import csv_to_pyspark_df
from validator.SiteToSiteMappingValidation import SiteMappingValidation

spark = SparkSession \
    .builder.config('spark.driver.memory', '12g') \
    .appName("CSVReader").master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext

site_mapping_path = "../data/csvs/sitetositemapping.csv"
df_siteMapping = csv_to_pyspark_df(spark, site_mapping_path)

site_list_path = "../data/csvs/sitelist.csv"
df_site_list = csv_to_pyspark_df(spark, site_list_path)

part_path = "../data/csvs/partsourcing.csv"
df_part_sourcing = csv_to_pyspark_df(spark, part_path)

siteMapping_validation = SiteMappingValidation(df_siteMapping)
siteMapping_validation.validate(df_site_list, df_part_sourcing)
