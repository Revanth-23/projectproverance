
#
# Copyright (c) 2022 Resilinc, Inc., All Rights Reserved
#   Any use, modification, or distribution is prohibited
#   without prior written consent from Resilinc, Inc.
#

import time
import dask.dataframe as dd
import pandas as pd
pd.options.display.precision = 2
pd.options.display.max_rows = 10


file_to_read = './data/csvs/esea_master_dmg_demos.part1.csv'

s = time.time()
df = dd.read_csv(file_to_read, dtype={'bomb_site': 'object'})
filter1 = df["att_team"] == "Animal Style"
df2 = df.where(filter1)
df3 = df2.dropna(how='all')
# df3.compute()
df3.to_csv('abcd.csv', single_file=True)
# print(df.head(10))
e = time.time()
print(f'total dask time: {(e - s) * 1000}')
exit(0)

# s = time.time()
# print(df.count)
# print(df.head(10))
# e = time.time()
# print(f'total dask count time: {(e - s) * 1000}')

# s = time.time()
# filter1 = df["tick"] > 100
# df2 = df.where(filter1)
# print(df2.head(10))
# e = time.time()
# print(f'total dask filter time: {(e - s) * 1000}')

s = time.time()
filter1 = df["att_team"]=="Animal Style"
df2 = df.where(filter1)
# print(df2.head(33267))
df2.to_csv('abcd.csv')
e = time.time()
print(f'total dask filter time: {(e - s) * 1000}')
#
# exit(0)
# s = time.time()
# df2 = df.groupby('vic_side')
# e = time.time()
# print(f'total dask group by time: {(e - s) * 1000}')
#
# # df2 = df[df['Partner Name'] == '']
# print(f'total dask time: {e - s}')
# print(df2.head(30))


