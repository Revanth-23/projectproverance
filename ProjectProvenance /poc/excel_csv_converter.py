
#
# Copyright (c) 2022 Resilinc, Inc., All Rights Reserved
#   Any use, modification, or distribution is prohibited
#   without prior written consent from Resilinc, Inc.
#
import uuid
from pyspark import pandas as sprk_pd
import pandas as pd
from pyspark.sql import SparkSession
from xlsx2csv import Xlsx2csv
#
# df = pd.DataFrame()
from utils import csv_to_pyspark_df, get_project_root

cdif_file = get_project_root() + '/data/excels/RIO_ValidationReport_06-12-2021_22-38-07.xlsx'
# # cdif_file = 'GM_CDIF_MD5.xlsx'
#
# xl = pd.ExcelFile(cdif_file, engine='openpyxl')
#
# sheet_names = xl.sheet_names

spark = SparkSession \
    .builder.config('spark.driver.memory', '12g') \
    .appName("CSVReader").master("local[*]") \
    .getOrCreate()

sheet_names = ['1. Partners', '2. Part sourcing', '2. Part sourcing(2)', '3. BOM and Alt Parts',
               '3. BOM and Alt Parts(2)',
               '4. Product', '6. Site List', '7. Product - Site Map', '8. Part - Site Map']
print(f'Sheet names are: {sheet_names}')
# x2c = Xlsx2csv(cdif_file)

from xlsx2csv import Xlsx2csv
from io import StringIO
# import pandas as pd


def read_excel(path: str, sheet_name: str) -> pd.DataFrame:
    buffer = StringIO()
    Xlsx2csv(path, outputencoding="utf-8", sheet_name=sheet_name).convert(buffer)
    buffer.seek(0)
    df = pd.read_csv(buffer)
    return df



ps = list()
# for sheet_name in sheet_names:
#     sheetid = x2c.getSheetIdByName(sheet_name)
#     print(f"Processing: [{sheetid}] {sheet_name}")
#     if sheetid:
#         with open('tempfile.csv', 'w+') as f:
#             # x2c.convert(sheet_name.replace(' ', '_') + '.csv', sheetid)
#             x2c.convert(f, sheetid)
#             data = f.read()
#             print(data)
#             df = pd.read_csv(data)
#             print(df)
#             exit(0)
# spark.read.format("com.crealytics.spark.excel").option("inferSchema", "true").load(cdif_file)
# exit(0)
for sheet_name in sheet_names:
    print(f"Processing: {sheet_name}")
    # df = pd.read_excel(cdif_file, sheet_name=sheet_name, engine='openpyxl')
    # sprk_df = spark.createDataFrame(df)
    read_excel(cdif_file, sheet_name)
    # spark.read.format("com.crealytics.spark.excel").option("inferSchema", "true").load(cdif_file)

print("converted to csv")

