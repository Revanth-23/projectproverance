from functools import reduce
from xlsx2csv import Xlsx2csv
import logging, os, re
import pyspark
from pyspark.sql.window import Window
from pyspark.sql import functions as f


def excel_csv_converter(tab=None, cdif_file=None, output_csv_file=None, LOGGER=None):
    x2c = Xlsx2csv(cdif_file)
    sheet_id = x2c.getSheetIdByName(tab)
    if sheet_id:
        x2c.convert(output_csv_file, sheet_id)
        LOGGER.info(f"CSV generated for {tab}")
        return True
    LOGGER.info(f"{tab} not found")
    return False


def get_sparkview_name(tab):
    spark_view_name = tab.replace(".", "").replace(" ", "_").replace("-", "_")
    return spark_view_name


def view_name_to_tab_name(view_name=None):
    reg_exp = "^\d+_"
    starting_number = re.findall(reg_exp, view_name)[0].strip("_")
    tab_name = re.sub(reg_exp, f"{starting_number}. ", view_name)
    tab_name = tab_name.replace("___", " - ").replace("_", " ")
    return tab_name


def get_next_tab_name(tab_name):
    reg_exp = r"\(\d+\)"
    trailing_number = re.findall(reg_exp, tab_name)
    if trailing_number:
        trailing_number = int(trailing_number[0].strip("(").strip(")"))
        return re.sub(reg_exp, f"({trailing_number+1})", tab_name)
    return tab_name + "(2)"


def merge_dataframes(pyspark_dfs_list):
    """Check all dataframes have same headers. If any 1 header is missing, it will raise error"""
    same_headers = all(
        set(df.columns) == set(pyspark_dfs_list[0].columns) for df in pyspark_dfs_list
    )
    if not same_headers:
        return "Unmatched headers"
    try:
        return reduce(
            lambda pyspark_df1, pyspark_df2: pyspark_df1.union(
                pyspark_df2.select(pyspark_df1.columns)
            ),
            pyspark_dfs_list,
        )
    except Exception as e:
        return str(e)


def parallel_process_logger(tenant_cdif_dir):
    log_file = os.path.join(tenant_cdif_dir, f"excel2csv_{str(os.getpid())}.log")
    logging.basicConfig(
        filename=log_file, format="%(asctime)s %(message)s", filemode="w"
    )

    # Creating an object
    logger = logging.getLogger()

    # Setting the threshold of logger to DEBUG
    logger.setLevel(logging.DEBUG)
    return logger


def duplicated(self, subset=None, orderby=None, ascending=False, keep=False):

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
            "is_duplicate", f.when(f.col("seq") == 1, False).otherwise(True)
        ).drop(*["seq"])

    elif keep == "last":
        self = (
            self.withColumn("max_seq", f.max("seq").over(w2))
            .withColumn(
                "is_duplicate",
                f.when(f.col("seq") == f.col("max_seq"), False).otherwise(True),
            )
            .drop(*["seq", "max_seq"])
        )

    else:
        self = (
            self.withColumn("max_seq", f.max("seq").over(w2))
            .withColumn(
                "is_duplicate",
                f.when(f.col("max_seq") > 1, True).otherwise(False),
            )
            .drop(*["seq", "max_seq"])
        )

    return self
