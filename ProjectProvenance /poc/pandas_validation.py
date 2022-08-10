
#
# Copyright (c) 2022 Resilinc, Inc., All Rights Reserved
#   Any use, modification, or distribution is prohibited
#   without prior written consent from Resilinc, Inc.
#

#!/usr/bin/env python

import os.path
import time
import pandas as pd

# import modin.pandas as pd


def read_csv_df():
    print("Starting excel read.")
    cdif_file_to_read = 'partsitemap_1mx10.csv'
    start_time = time.time()
    excel_obj: dict = pd.read_csv(cdif_file_to_read)
    # dfs = [excel_obj] * 10
    # mdf = pd.concat(dfs)
    # mdf.to_csv('partsitemap_1mx10.csv')
    read_finish = time.time()
    total_read_time = read_finish - start_time
    print(
        f"Read whole file {cdif_file_to_read} of size {os.path.getsize(cdif_file_to_read)} byte in : {total_read_time}.")
    return total_read_time, excel_obj



def validate_tab(part_site_data):
    part_site_data_copy = part_site_data
    columnlist = part_site_data_copy.columns.values.tolist()
    partner_part_index = columnlist.index('Partner Part #') + 1
    partner_name_index = columnlist.index('Partner Name') + 1
    # site_index = columnlist.index('site number') + 1
    # activity_index = columnlist.index('activity') + 1
    # alt_site_index = columnlist.index('alternate site number') + 1
    # recovery_index = columnlist.index('recovery time') + 1
    # alt_bring_up_time_index = columnlist.index('alternate site bring up time') + 1

    for row in part_site_data_copy.itertuples():
        # index = row.__getattribute__('Index')
        # part = row.__getattribute__('_' + str(partner_part_index))
        # partner = row.__getattribute__('_' + str(partner_name_index))
        # # site = row.__getattribute__('_' + str(site_index))
        # # activity = row.__getattribute__('activity')
        # # alt_site = row.__getattribute__('_' + str(alt_site_index))
        # # recovery_time = row.__getattribute__('_' + str(recovery_index))
        # # alt_bring_up_time = row.__getattribute__('_' + str(alt_bring_up_time_index))
        # if pd.isnull(partner):
        #     # comments += Messages.partsitemap_messages['partner_name_blank'];
        #     # print("partner_name_blank")
        #     pass
        #
        # if pd.isnull(part):
        #     # comments += Messages.partsitemap_messages['partner_name_blank'];
        #     # print("part blank")
            pass


if __name__ == '__main__':
    time_taken, part_site_map_data = read_csv_df()
    validation_time = time.time()
    validate_tab(part_site_map_data)
    validation_finish_time = time.time() - validation_time
    print(f'time taken to validated: {validation_finish_time}')