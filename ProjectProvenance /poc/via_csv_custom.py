
#
# Copyright (c) 2022 Resilinc, Inc., All Rights Reserved
#   Any use, modification, or distribution is prohibited
#   without prior written consent from Resilinc, Inc.
#

import time
from csv import reader
from multiprocessing import Pool
# read csv file as a list of lists
s = time.time()

def row_checker(row):
    if not row[0]:
        print('found')
    if not row[1]:
        pass

with open('data/csvs/8. Part - Site Map.csv', 'r') as read_obj:
    # pass the file object to reader() to get the reader object

    csv_reader = reader(read_obj)
    # Pass reader object to list() to get a list of lists
    list_of_rows = list(csv_reader)
    e = time.time()
    print(f'read time: {e - s}')
    # print(list_of_rows[:10])
    s = time.time()
    # for row in list_of_rows:
    #     if not row[0]:
    #         pass
    #     if not row[1]:
    #         pass
    with Pool(8) as p:
        p.map(row_checker, list_of_rows)
    e = time.time()
    print(f'total: {e - s}')
