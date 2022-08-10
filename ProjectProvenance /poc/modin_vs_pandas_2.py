
#
# Copyright (c) 2022 Resilinc, Inc., All Rights Reserved
#   Any use, modification, or distribution is prohibited
#   without prior written consent from Resilinc, Inc.
#

# Load csv file using pandas
import time
import pandas as pandas_pd
from multiprocessing import Pool

s = time.time()
pandas_df = pandas_pd.read_csv("data/csvs/8. Part - Site Map.csv")
e = time.time()
print("Pandas Loading Time = {}".format(e-s))

s = time.time()
x = pandas_df['Partner Name'].isnull().values
y = pandas_df['Partner Part #'].isnull().values
e = time.time()
print("Pandas find Time = {}".format(e-s))
print(x)
print(y)
# Using modin ray:

# Load csv file using Modin and Ray
import os
os.environ["MODIN_ENGINE"] = "ray"  # Modin will use Ray
import ray
ray.init(num_cpus=8)
import modin.pandas as ray_pd

s = time.time()
mray_df = ray_pd.read_csv("data/csvs/creditcard.csv")
e = time.time()
# print("Modin Loading Time = {}".format(e-s))

# s = time.time()
# mray_df = ray_pd.read_csv("data/csvs/8. Part - Site Map.csv")
# e = time.time()
# print("Modin Loading Time = {}".format(e-s))

s = time.time()
mray_df = ray_pd.read_csv("data/csvs/8. Part - Site Map.csv")
p = ray_pd.DataFrame(mray_df)
print(type(p))
e = time.time()
print("Modin Loading Time = {}".format(e-s))

s = time.time()
x =mray_df['Partner Name'].isnull().values
y = mray_df['Partner Part #'].isnull().values
e = time.time()
print("Modin find Time = {}".format(e-s))
# print(x)
# print(y)
exit(0)
