import pyspark.sql.readwriter
from pyspark.sql import SparkSession
from configuration import JDBC_DB_URI, DB_DRIVER

spark = SparkSession \
    .builder.config('spark.driver.memory', '12g') \
    .appName("DBAccessor").master("local[*]") \
    .getOrCreate()

# Create Spark DB Connection.
# This Requires PostgreSQL JDBC Driver Jar to be placed in PySpark's jars directory
# (e.g ../venv/lib/python3.10/site-packages/pyspark/jars, this is example taken from a Linux machine where PySpark is
# installed in a Virtualenv 'venv'. This can vary based on OS and virtualenv.

db_conn: pyspark.sql.readwriter.DataFrameReader = spark.read.format("jdbc").options(url=JDBC_DB_URI, driver=DB_DRIVER)

df = db_conn.option("query", 'select id, tenant_name from tenant where id = 14053').load()
df.show()


