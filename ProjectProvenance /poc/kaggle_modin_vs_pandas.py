
#
# Copyright (c) 2022 Resilinc, Inc., All Rights Reserved
#   Any use, modification, or distribution is prohibited
#   without prior written consent from Resilinc, Inc.
#

# import pandas as pd
import modin.pandas as pd
import time
import ray
ray.init(num_cpus=8)
s = time.time()
df = pd.read_csv("./esea_master_dmg_demos.part1.csv")
e = time.time()
print("Modin Loading Time = {}".format(e-s))

# s = time.time()
# df = df.fillna(value=0)
# e = time.time()
# print("Pandas fillna Time = {}".format(e-s))

s = time.time()
for row in df.itertuples():
    pass
e = time.time()
print("Pandas Itertuple Time = {}".format(e-s))
