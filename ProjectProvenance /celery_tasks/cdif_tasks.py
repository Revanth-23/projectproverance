#
# Copyright (c) 2022 Resilinc, Inc., All Rights Reserved
#   Any use, modification, or distribution is prohibited
#   without prior written consent from Resilinc, Inc.
#

import os, re  # ,asyncpg, asyncio
from managers.CeleryAppManager import celery_app
from managers.SparkSessionManager import SparkCluster
from celery.signals import task_postrun, setup_logging
from celery.result import GroupResult
from xlsx2csv import Xlsx2csv
from Log import log_global
import functools
from .cdif_data import cdif_columns_dict, sheet_names

# from asgiref.sync import async_to_sync
LOGGER = None

current_working_dir_path = os.getcwd()
cwd = current_working_dir_path.split("/")[-1]

if cwd != "server":
    spark = SparkCluster()
    spark = spark.setup()

FAILURE_STATUS = "FAILURE"
EXCEL_SHEET_NOT_FOUND_ERROR = (
    "'>' not supported between instances of 'NoneType' and 'int'"
)
EXCEL_SHEET_NOT_FOUND_MESSAGE = "Sheet not found in the uploaded file"


@setup_logging.connect
def setup_logger(**kwargs):
    global LOGGER
    LOGGER = log_global()


@task_postrun.connect
def task_post_run(task_id=None, task=None, sender=None, *args, **kwargs):
    sender_name = re.findall(r"'.+'", str(type(sender)))[0].strip("'").split(".")[-1]
    kwargs_dict = kwargs["kwargs"]
    group_id = kwargs_dict.get("group_task_id", None)
    user_id = kwargs_dict.get("user_id", None)
    sheet_name = kwargs_dict.get("sheet_name")
    if sender_name == "process_excel_sheet":
        saved_group_result = GroupResult.restore(group_id)
        if saved_group_result.ready():
            #     query_status, db_response = async_to_sync(update_dataload_status)(
            #         user_id=user_id, task_id=group_id, dataload_status="READY"
            #     )
            # if query_status:
            if saved_group_result.failed():
                for child in saved_group_result.children:
                    if child.status == FAILURE_STATUS:
                        sheet_name, error = str(child.result).split("ERROR:")
                        if error == EXCEL_SHEET_NOT_FOUND_ERROR:
                            error = EXCEL_SHEET_NOT_FOUND_MESSAGE

                        LOGGER.error(
                            f"user_id:{user_id}; Error while converting excel to CSV for '{sheet_name}':{error}"
                        )
                LOGGER.info(f"user_id:{user_id}; Errors Found. Exiting")
            if saved_group_result.successful():
                LOGGER.info(
                    f"user_id:{user_id}; Excel to CSV files conversion completed."
                )
                # next_step(sheet_name, saved_group_result.children)
                start_cdif_processing(user_id)


def create_pyspark_df(user_id, csv_file):
    attempt = 3
    while attempt > 0:
        try:
            pyspark_df = spark.read.csv(csv_file, header=True)

            attempt = 0
            return pyspark_df

        except Exception as e:
            attempt -= 1
            if attempt == 0:
                LOGGER.error(
                    f"user_id : {user_id};FAILED: CSV to pyspark for {csv_file}, reason : {e}"
                )
                return None


def merge_dataframes(pyspark_dfs_list):
    try:
        return functools.reduce(
            lambda py_spark_df1, py_spark_df2: py_spark_df1.union(
                py_spark_df2.select(py_spark_df1.columns)
            ),
            pyspark_dfs_list,
        )
    except Exception as e:
        return e


def start_cdif_processing(user_id):
    LOGGER.info(f"user_id:{user_id}; Starting CDIF processing")
    user_cdif_dir = os.path.join("cdif_files", user_id)
    user_cdif_dir_files = os.listdir(user_cdif_dir)
    errors = []
    for sheet_name in sheet_names:
        spark_view_name = get_spark_view_name(user_id, sheet_name)
        pyspark_dfs_list = []  # list of dataframes for a particular sheet
        csvs_to_merge = []  # used for logging purpose only
        for file in user_cdif_dir_files:
            if file.startswith(spark_view_name) and file.endswith(".csv"):
                csv_file = os.path.join(user_cdif_dir, file)
                pyspark_df = create_pyspark_df(user_id, csv_file)
                if pyspark_df:
                    LOGGER.info(f"user_id:{user_id}; pyspark_df_created for {file}")
                    pyspark_df_columns = pyspark_df.columns
                    required_columns = cdif_columns_dict.get(sheet_name, None)
                    if not required_columns:
                        LOGGER.error(
                            f"user_id:{user_id}; Missing {sheet_name} key in cdif_columns_dict"
                        )
                        continue
                    missing_columns = set(required_columns) - set(pyspark_df_columns)

                    if len(missing_columns) > 0:
                        errors.append(file)
                        LOGGER.error(
                            f"user_id:{user_id}; Missing columns {','.join(col_name for col_name in missing_columns)} in {file}"
                        )
                    else:
                        pyspark_df = pyspark_df.na.drop(how="all").dropDuplicates()
                        pyspark_dfs_list.append(pyspark_df)
                        csvs_to_merge.append(file)
                else:
                    errors.append(file)
        if errors:
            continue
        if len(pyspark_dfs_list) > 1:
            merged_data = merge_dataframes(pyspark_dfs_list)
            if isinstance(merged_data, str):
                LOGGER.error(
                    f"Error occurred while merging dataframes of {','.join(filename for filename in csvs_to_merge)}. Reason:{merged_data}"
                )
                continue
            pyspark_df = merged_data
            LOGGER.info(
                f"user_id:{user_id}; Merged dataframes of {','.join(filename for filename in csvs_to_merge)}"
            )
        pyspark_df.createOrReplaceTempView(spark_view_name)
        LOGGER.info(f"user_id:{user_id}; TempView created for {spark_view_name}")
    if errors:
        LOGGER.info(f"user_id:{user_id}; Errors Found . Exiting")
    else:
        LOGGER.info(f"user_id:{user_id};All Pyspark dataframes created")


def get_spark_view_name(user_id, sheet_name):
    spark_view_name = (
        sheet_name.replace(".", "")
        .replace(" ", "_")
        .replace("(", "_")
        .replace(")", "")
        .replace("-", "")
    )
    return spark_view_name


@celery_app.task()
def process_excel_sheet(user_id=None, sheet_name=None, group_task_id=None):
    try:
        LOGGER.info(f"user_id:{user_id}; Starting sheet_name {sheet_name}")
        excel_to_csv_converter(user_id=user_id, sheet_name=sheet_name)
        LOGGER.info(f"user_id:{user_id}; CSV created for sheet : {sheet_name}")
    except Exception as e:
        raise Exception(f"{sheet_name}ERROR:{e}")
    while True:
        try:
            sheet_name = get_next_sheet_name(sheet_name)
            LOGGER.info(f"user_id:{user_id}; Checking Next sheet for {sheet_name}")
            excel_to_csv_converter(user_id=user_id, sheet_name=sheet_name)
            LOGGER.info(f"user_id:{user_id}; CSV created for sheet : {sheet_name}")
        except:
            LOGGER.info(f"user_id:{user_id}; Next sheet '{sheet_name}' not found")
            break


def excel_to_csv_converter(user_id=None, sheet_name=None):
    user_cdif_dir = os.path.join("cdif_files", user_id)
    cdif_file = os.path.join(user_cdif_dir, f"{user_id}.xlsx")
    x2c = Xlsx2csv(cdif_file)
    sheetid = x2c.getSheetIdByName(sheet_name)
    spark_view_name = get_spark_view_name(user_id, sheet_name)
    csv_file_name = spark_view_name + ".csv"
    csv_file = os.path.join(user_cdif_dir, csv_file_name)
    x2c.convert(csv_file, sheetid)


def get_next_sheet_name(sheet_name):
    reg_exp = r"\(\d+\)"
    trailing_number = re.findall(reg_exp, sheet_name)
    if trailing_number:
        trailing_number = int(trailing_number[0].strip("(").strip(")"))
        return re.sub(reg_exp, f"({trailing_number+1})", sheet_name)
    return sheet_name + "(2)"
