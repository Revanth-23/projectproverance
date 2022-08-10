import sys
sys.path.append('../')
from pyspark.sql import SparkSession

from utils import csv_to_pyspark_df
from validator.ProductVaildation import ProductValidation
spark = SparkSession \
    .builder.config('spark.driver.memory', '12g') \
    .appName("CSVReader").master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext

# A CSV dataset is pointed to by path.
# The path can be either a single CSV file or a directory of CSV files
file_path = "../data/csvs/product - Sheet1.csv"
df = csv_to_pyspark_df(spark, file_path)

product_validation = ProductValidation(df)
product_validation.validate(df)
# product_validation.warning_validation()


