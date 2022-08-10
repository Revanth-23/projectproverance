
#
# Copyright (c) 2022 Resilinc, Inc., All Rights Reserved
#   Any use, modification, or distribution is prohibited
#   without prior written consent from Resilinc, Inc.
#

import time

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder.config('spark.driver.memory', '12g') \
    .appName("CSVReader").master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext

# A CSV dataset is pointed to by path.
# The path can be either a single CSV file or a directory of CSV files
path = "data/csvs/esea_master_dmg_demos.part1.csv"
df = spark.read.csv(path, header=True)
# df.show()

s = time.time()
df = spark.read.csv(path, header=True)


df.createOrReplaceTempView("partner")
spark.catalog.cacheTable('partner')

# df.cache()

# filter1 = df["att_team"] == "Animal Style"
df2 = None
for i in range(0, 50):
# df2 = df.filter(df.att_team == 'Animal Style')
# spark.catalog.cacheTable('partner')
    t = time.time()
    spark.sql(f"""SELECT * FROM partner where hp_dmg = {i}""")
    # df2.write.csv('xyz_pyspark_where.csv', header=True)
    k = time.time()
    print(f"query time: {k - t}")
df2.write.csv('xyz.csv', header=True, mode='overwrite')
e = time.time()
print(f'Spark Read time: {(e - s) * 1000}')
exit(0)


def view():
    # create csv
    # create spark df
    df.createOrReplaceTempView('partner')


# there prefect
# validating partner tab
def parnter_validation_task():
    # temp view
    # partner df
    df = spark.sql("select * from parnter")
    df = spark.sql("select * from partner where partner_name is null")

df.createOrReplaceTempView("partner")
spark.catalog.cacheTable('partner')
spark.sql("""SELECT * FROM partner""").count()
s = time.time()
spark.sql("""SELECT * FROM partner where tick > 100""").show()
e = time.time()
print(f'filter time: {(e - s) * 1000}')

s = time.time()
spark.sql("""SELECT * FROM partner where att_team = 'Animal Style'""").show()
e = time.time()
print(f'filter time: {(e - s) * 1000}')

s = time.time()
spark.sql("""SELECT count(is_bomb_planted) as total_bomb_planted FROM partner group by vic_side""").show()
e = time.time()
print(f'filter time: {(e - s) * 1000}')
exit(0)
# Read a csv with delimiter, the default delimiter is ","
df2 = spark.read.option("delimiter", ';').csv(path)
df2.show()

# Read a csv with delimiter and a header
df3 = spark.read.option("delimiter", ";").option("header", True).csv(path)
df3.show()
pdf = df.toPandas()

# You can also use options() to use multiple options
df4 = spark.read.options(delimiter=";", header=True).csv(path)

# "output" is a folder which contains multiple csv files and a _SUCCESS file.
df3.write.csv("output")

# Read all files in a folder, please make sure only CSV files should present in the folder.
folderPath = "examples/src/main/resources"
df5 = spark.read.csv(folderPath)
df5.show()