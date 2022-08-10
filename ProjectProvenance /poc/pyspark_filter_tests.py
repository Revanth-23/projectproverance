
#
# Copyright (c) 2022 Resilinc, Inc., All Rights Reserved
#   Any use, modification, or distribution is prohibited
#   without prior written consent from Resilinc, Inc.
#

import time
import pandas

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder.config('spark.driver.memory', '12g') \
    .appName("CSVReader").master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext

# A CSV dataset is pointed to by path.
# The path can be either a single CSV file or a directory of CSV files
path = 'Stock_Report_-_Part_Site_Mapping_Report_New_Schema1.csv'
df = spark.read.csv(path, header=True)

# All tests are performed on a 300MB csv file with 967231 records
# Changing column names for ease of filtering
new_column_names = [f"{c.lower().replace(' ','_').replace('?','')}" for c in df.columns]
df = df.toDF(*new_column_names)
df.select(df.columns[:2]).show(10)

# Filter using Spark TempView
df.createOrReplaceTempView("part")
spark.catalog.cacheTable('part')
s = time.time()
df1 = spark.sql("""SELECT part, part_description FROM part where part IS NOT NULL AND part_description IS NULL""")
e = time.time()
df1.select(df1.columns[:2]).show(10)
print(f'filter time: {(e - s) * 1000}')
# Results: 1. 129 ms
#          2. 119 ms
#          3. 121 ms


# Filter using Pyspark filter operation
s = time.time()
df1 = df.filter(df.part.isNotNull() & df.part_description.isNull())
e = time.time()
df1.select(df1.columns[:2]).show(10)
print(f'filter time: {(e - s) * 1000}')
# Results: 1. 15.6 ms
#          2. 20.1 ms
#          3. 20.5 ms


# Pandas basic filtering for reference
pdf1 = pandas.read_csv(path)
pdf1.rename(columns={'Part': 'part', 'Part Description': 'part_description'}, inplace=True)
print(pdf1[pdf1.columns[0:2]].head(10))
s = time.time()
pdf2 = pdf1[pdf1.part.notnull() & pdf1.part_description.isnull()]
e = time.time()
print(pdf2[pdf2.columns[0:2]].head(10))
print(f'filter time: {(e - s) * 1000}')
# Results: 1. 268 ms
#          2. 359 ms
#          3. 304 ms
