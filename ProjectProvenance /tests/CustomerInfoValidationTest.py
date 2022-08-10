from pyspark.sql import SparkSession
from validator.CustomerInfoValidation import CustomerValidation
from utils import csv_to_pyspark_df

spark = SparkSession \
    .builder.config('spark.driver.memory', '12g') \
    .appName("CSVReader").master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext

file_path = "../data/csvs/Cust_cont_info.csv"
df_customer = csv_to_pyspark_df(spark, file_path)

customer_validation = CustomerValidation(df_customer)
customer_validation.validate()
