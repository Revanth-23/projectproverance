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
#print(db_conn)
def get_partner_picklist():
    enum_labels=[]
    if db_conn:
        enum_labels = db_conn.option("query", "SELECT enumlabel FROM PG_ENUM WHERE ENUMTYPID IN ('public.company_type'::REGTYPE) ORDER BY OID").load()
        is_enum_labels_empty = (enum_labels.count() == 0)
        if not is_enum_labels_empty:
                enum_labels = [row.enumlabel for row in enum_labels.collect()]
                enum_labels = list(set(enum_labels))
    print(enum_labels)
    return enum_labels
get_partner_picklist()



    


 
        

