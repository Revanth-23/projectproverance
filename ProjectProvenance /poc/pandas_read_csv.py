
#
# Copyright (c) 2022 Resilinc, Inc., All Rights Reserved
#   Any use, modification, or distribution is prohibited
#   without prior written consent from Resilinc, Inc.
#

import time
import pandas as pd
pd.options.display.precision = 2
pd.options.display.max_rows = 10


file_to_read = './data/csvs/esea_master_dmg_demos.part1.csv'

s = time.time()
df = pd.read_csv(file_to_read)
filter1 =  df["att_team"] == "Animal Style"
df2 = df.where(filter1)
df3 = df2.dropna(how='all')
df3.to_csv('abcd_pandas.csv')
# print(df.head(10))
e = time.time()
print(f'total pandas time: {(e - s) * 1000}')
exit(0)