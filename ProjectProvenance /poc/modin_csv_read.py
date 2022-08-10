
#
# Copyright (c) 2022 Resilinc, Inc., All Rights Reserved
#   Any use, modification, or distribution is prohibited
#   without prior written consent from Resilinc, Inc.
#

import time
import os
import modin.pandas as pd
pd.options.display.precision = 2
pd.options.display.max_rows = 10

# os.environ["MODIN_ENGINE"] = "dask"


file_to_read = './data/csvs/esea_master_dmg_demos.part1.csv'

s = time.time()
df = pd.read_csv(file_to_read)
# filter1 = df['att_team'].str.equals("Animal Style") # df["att_team"] == "Animal Style" #df["att_team"] == "Animal Style"
df3 = None
for i in range(0, 100):
    t = time.time()
    df2 = data = df[df['hp_dmg'] == i] #df.where(df['att_team'].str.contains("Animal Style"))
    df3 = df2.dropna(how='all')
    df3.to_csv('abcd_modin.csv')
    k = time.time()
    print(f'Each query time: {k - t}')
# print(df.head(10))
e = time.time()
print(f'total pandas time: {(e - s) * 1000}')
exit(0)