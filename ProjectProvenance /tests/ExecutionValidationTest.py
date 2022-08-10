
from pyspark.sql import SparkSession

from utils import csv_to_pyspark_df
from validator.ExecutionValidation import ExecutionValidation
spark = SparkSession \
    .builder.config('spark.driver.memory', '12g') \
    .appName("CSVReader").master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext

# A CSV dataset is pointed to by path.
# The path can be either a single CSV file or a directory of CSV files
file_path = "../data/csvs/Demo_Execution.csv"
df = csv_to_pyspark_df(spark, file_path)

file_path = "../data/csvs/Demo_Sourcing.csv"
df_sourcing = csv_to_pyspark_df(spark, file_path)

execution_validation = ExecutionValidation(df)
execution_validation.validation(df_sourcing)

