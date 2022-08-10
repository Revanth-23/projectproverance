from pyspark import pandas as pd
from pyspark.sql import SparkSession
#
spark = SparkSession \
    .builder.config('spark.driver.memory', '12g') \
    .appName("CSVReader").master("local[*]") \
    .getOrCreate()

df = pd.read_excel('/home/varx/Downloads/NARUTO_ValidationReport_23-12-2021_10-50-06.xlsx', dtype=str)
print(df.head(1))

print(type(df))
# print(type(spark_df))
