import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CDIF_Testing").getOrCreate()
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql import functions as f
from pyspark.sql.functions import monotonically_increasing_id


def duplicated(self, subset=None, orderby=None, ascending=False, keep="first"):

    if subset == None:
        subset = self.schema.names

    subset = [subset] if isinstance(subset, str) else subset

    assert keep in ["first", "last", False], "keep must be either first, last or False"

    if orderby == None:
        orderby = subset
    elif isinstance(orderby, str):
        orderby = [orderby]

    if isinstance(ascending, bool):
        if ascending:
            ordering = [f.asc] * len(orderby)
        else:
            ordering = [f.desc] * len(orderby)

    elif isinstance(ascending, list):
        assert all(
            [isinstance(i, bool) for i in ascending]
        ), "ascending should be bool or list of bool"
        ordering = [f.asc if i else f.desc for i in ascending]

    w1 = (
        Window.partitionBy(*subset)
        .orderBy(*[ordering[idx](i) for idx, i in enumerate(orderby)])
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    w2 = (
        Window.partitionBy(*subset)
        .orderBy(*[ordering[idx](i) for idx, i in enumerate(orderby)])
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )

    self = self.sort(orderby, ascending=ascending).withColumn(
        "seq", f.row_number().over(w1)
    )

    if keep == "first":
        self = self.withColumn(
            "duplicate_indicator", f.when(f.col("seq") == 1, False).otherwise(True)
        ).drop(*["seq"])

    elif keep == "last":
        self = (
            self.withColumn("max_seq", f.max("seq").over(w2))
            .withColumn(
                "duplicate_indicator",
                f.when(f.col("seq") == f.col("max_seq"), False).otherwise(True),
            )
            .drop(*["seq", "max_seq"])
        )

    else:
        self = (
            self.withColumn("max_seq", f.max("seq").over(w2))
            .withColumn(
                "duplicate_indicator",
                f.when(f.col("max_seq") > 1, True).otherwise(False),
            )
            .drop(*["seq", "max_seq"])
        )

    return self


pyspark.sql.DataFrame.duplicated = duplicated

file = "bom (copy).csv"
df = spark.read.csv(file, header=True)
df = df.select("*").withColumn("id", monotonically_increasing_id())
# df.duplicated(subset=["Product name", "Part #"]).orderBy(col("id").asc()).show(35)
df = df.duplicated(subset=["Product name", "Part #"])
df.orderBy(col("id").asc()).show(35)
