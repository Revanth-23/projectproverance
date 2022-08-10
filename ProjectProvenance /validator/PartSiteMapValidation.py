#
# Copyright (c) 2022 Resilinc, Inc., All Rights Reserved
# Any use, modification, or distribution is prohibited
# without prior written consent from Resilinc, Inc.
#

from typing import List

import pyspark.pandas as ps
# from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, lit, concat_ws

from Log import log_global
from utils.Queries import get_site_activities
from validator.ValidationMessages import part_site_map_messages

LOGGER = log_global()


# spark = SparkSession \
#     .builder.config('spark.driver.memory', '12g') \
#     .appName("CSVReader").master("local[*]") \
#     .getOrCreate()
#
# sc = spark.sparkContext


class PartSiteMapValidation(object):

    def __init__(self, data):
        ps.set_option('compute.ops_on_diff_frames', True)
        self.data = data
        self.comments = {}
        self.warnings = {}
        LOGGER.info(f"Data loaded for part-site map validation : {self.data}")

    def add_to_comments(self, error_message: str, index_list: List[int]):
        if error_message and index_list:
            for index in index_list:
                if index in self.comments:
                    self.comments[index] += ' || ' + error_message
                else:
                    self.comments[index] = error_message

    def add_to_warnings(self, warning_message: str, index_list: List[int]):
        if warning_message and index_list:
            for index in index_list:
                if index in self.warnings:
                    self.warnings[index] += ' || ' + warning_message
                else:
                    self.warnings[index] = warning_message

    def validation(self, partner_df, part_df, site_df):
        LOGGER.info("Part-Site Map Validation started....")
        site_activities_df = get_site_activities()
        print('site_activities_df --------> : ', site_activities_df)
        indexed_df = self.data.select("*").withColumn("id", monotonically_increasing_id())
        indexed_df = indexed_df.withColumn("partner_part_key", concat_ws("_", "partner_name", 'partner_part_number'))
        indexed_df = indexed_df.withColumn("partner_site_key", concat_ws("_", "partner_name", 'site_number'))
        indexed_df = indexed_df.withColumn("partner_alt_site_key",
                                           concat_ws("_", "partner_name", 'alternate_site_number'))
        print('indexed_df --------> : ', indexed_df)

        # print_df = [row for row in indexed_df.collect()]
        # print('print_df --------> : ', print_df)
        # print_df = set(print_df)
        # print('print_df --------> : ', print_df)

        invalid_partners = []
        invalid_parts = []
        invalid_partner_parts = []
        invalid_sites = []
        invalid_partner_sites = []
        invalid_alt_sites = []
        invalid_partner_alt_sites = []
        is_partner_tab_empty = (partner_df.count() == 0)
        is_part_tab_empty = (part_df.count() == 0)
        is_site_tab_empty = (site_df.count() == 0)

        if not is_partner_tab_empty:
            partner_lookup_df = self.data.join(partner_df, self.data["partner_name"] == partner_df["partner_name"],
                                               "leftanti")
            invalid_partners = [row.partner_name for row in partner_lookup_df.collect()]
            invalid_partners = set(invalid_partners)
            print('invalid_partners --------> : ', invalid_partners)

        if not is_part_tab_empty:
            partner_part_col_list = ["partner_name", "partner_part_number"]
            part_lookup_df = self.data.join(part_df, partner_part_col_list, "leftanti")
            part_lookup_df = part_lookup_df.withColumn("partner_part_key",
                                                       concat_ws("_", "partner_name",
                                                                 'partner_part_number'))
            print('part_lookup_df --------> : ', part_lookup_df)
            invalid_parts = [part_row.partner_part_number for part_row in part_lookup_df.collect()]
            invalid_parts = set(invalid_parts)
            print('invalid_parts --------> : ', invalid_parts)

            invalid_partner_parts = [part_row.partner_part_key for part_row in part_lookup_df.collect()]
            invalid_partner_parts = set(invalid_partner_parts)
            print('invalid_partner_parts --------> : ', invalid_partner_parts)

        if not is_site_tab_empty:
            partner_site_col_list = ["partner_name", "site_number"]
            site_lookup_df = self.data.join(site_df, partner_site_col_list, "leftanti")
            site_lookup_df = site_lookup_df.withColumn("partner_site_key",
                                                       concat_ws("_", "partner_name",
                                                                 'site_number'))
            site_lookup_df = site_lookup_df.withColumn("partner_alt_site_key",
                                                       concat_ws("_", "partner_name",
                                                                 'alternate_site_number'))
            print('site_lookup_df --------> : ', site_lookup_df)
            invalid_sites = [site_row.site_number for site_row in site_lookup_df.collect()]
            invalid_sites = set(invalid_sites)
            print('invalid_sites --------> : ', invalid_sites)

            invalid_partner_sites = [site_row.partner_site_key for site_row in site_lookup_df.collect()]
            invalid_partner_sites = set(invalid_partner_sites)
            print('invalid_partner_sites --------> : ', invalid_partner_sites)

            alt_site_lookup_df = self.data.join(site_df, self.data["alternate_site_number"] == site_df["site_number"],
                                                "leftanti")
            invalid_alt_sites = [site_row.alternate_site_number for site_row in alt_site_lookup_df.collect()]
            invalid_alt_sites = set(invalid_alt_sites)
            print('invalid_alt_sites --------> : ', invalid_alt_sites)

            partner_alt_site_col_list = ["partner_name", "alternate_site_number"]
            partner_alt_site_lookup_df = self.data.join(site_df, partner_alt_site_col_list, "leftanti")
            partner_alt_site_lookup_df = partner_alt_site_lookup_df.withColumn("partner_alt_site_key",
                                                                               concat_ws("_", "partner_name",
                                                                                         'alternate_site_number'))
            invalid_partner_alt_sites = [site_row.partner_alt_site_key for site_row in
                                         partner_alt_site_lookup_df.collect()]
            invalid_partner_alt_sites = set(invalid_partner_alt_sites)
            print('invalid_partner_alt_sites --------> : ', invalid_partner_alt_sites)

        # Validation: 'Partner name can not be blank'
        # Actual message : "Please insert partner name (column A) || "
        filtered_df = indexed_df.filter(indexed_df.partner_name.isNull())
        filtered_ids = [int(row.id) for row in filtered_df.collect()]
        self.add_to_comments(part_site_map_messages['partner_name_blank'], filtered_ids)

        # Validation: 'Partner name should be valid and present in Tab 1 'Partners''
        # Actual message : "Resilinc Partner Name provided here should be present in Tab '1. Partners', column A  (column A) || "
        filtered_df = indexed_df.filter((indexed_df.partner_name.isNotNull()) & (
                lit(is_partner_tab_empty) | (indexed_df.partner_name.isin(invalid_partners))))
        filtered_ids = [int(row.id) for row in filtered_df.collect()]
        self.add_to_comments(part_site_map_messages['partner_name_absent'], filtered_ids)

        # Validation: 'Partner part can not be blank'
        # Actual message : "Please insert the partner part # (column B) || "
        filtered_df = indexed_df.filter(indexed_df.partner_part_number.isNull())
        filtered_ids = [int(row.id) for row in filtered_df.collect()]
        self.add_to_comments(part_site_map_messages['partner_part_blank'], filtered_ids)

        # Validation: 'Partner part should be valid and present in Tab 2 'Part Sourcing''
        # Actual message : "Partner Part # provided here should be present in \"Tab 2. Part Sourcing\", Column F (column B) || "
        filtered_df = indexed_df.filter((indexed_df.partner_part_number.isNotNull()) & (
                lit(is_part_tab_empty) | (indexed_df.partner_part_number.isin(invalid_parts))))
        filtered_ids = [int(row.id) for row in filtered_df.collect()]
        self.add_to_comments(part_site_map_messages['partner_part_absent'], filtered_ids)

        # Validation: 'Partner part should corresponds with its partner'
        # Actual message : "Please ensure the partner part # corresponds with its respective partner name (column B) || "
        filtered_df = indexed_df.filter(
            (indexed_df.partner_part_number.isNotNull() & indexed_df.partner_name.isNotNull()) & (
                indexed_df.partner_part_key.isin(invalid_partner_parts)))
        filtered_ids = [int(row.id) for row in filtered_df.collect()]
        self.add_to_comments(part_site_map_messages['partner_part_same'], filtered_ids)

        # Validation: 'Site number can not be blank'
        # Actual message : "Please insert the site number of the Tier 1 partner from \"Tab 6 Site List\"  Column A (column D) || "
        filtered_df = indexed_df.filter(indexed_df.site_number.isNull())
        filtered_ids = [int(row.id) for row in filtered_df.collect()]
        self.add_to_comments(part_site_map_messages['site_number_blank'], filtered_ids)

        # Validation: 'Site number should not be absent'
        # Actual message : "Please insert the site number of the Tier 1 partner from \"Tab 6 Site List\"  Column A (column D) || "
        filtered_df = indexed_df.filter((indexed_df.site_number.isNotNull()) & (
                lit(is_site_tab_empty) | (indexed_df.site_number.isin(invalid_sites))))
        filtered_ids = [int(row.id) for row in filtered_df.collect()]
        self.add_to_comments(part_site_map_messages['site_number_absent'], filtered_ids)

        # Validation: 'Site should corresponds with its partner'
        # Actual message : "Please ensure the site number corresponds with its respective partner name (column D) || "
        filtered_df = indexed_df.filter((indexed_df.site_number.isNotNull() & indexed_df.partner_name.isNotNull()) & (
            indexed_df.partner_site_key.isin(invalid_partner_sites)))
        filtered_ids = [int(row.id) for row in filtered_df.collect()]
        self.add_to_comments(part_site_map_messages['partner_not_same_for_site'], filtered_ids)

        # Validation: 'Site activity should not be blank'
        # Actual message : "Please choose activity from the drop-down (column E) || "
        filtered_df = indexed_df.filter(indexed_df.activity.isNull())
        filtered_ids = [int(row.id) for row in filtered_df.collect()]
        self.add_to_comments(part_site_map_messages['activity_blank'], filtered_ids)

        # TO DO Validation: 'Site activity should be selected from provided drop-down values'
        # Actual message : "Please choose activity from the drop-down (column E) || "
        filtered_df = indexed_df.filter(
            (indexed_df.activity.isNotNull()) & (~ indexed_df.activity.isin(site_activities_df)))
        filtered_ids = [int(row.id) for row in filtered_df.collect()]
        self.add_to_comments(part_site_map_messages['activity_blank'], filtered_ids)

        # Validation: 'Recovery time should not be blank if Site activity is provided'
        # Actual message : "Please insert the recovery time between 1 to 52 (weeks) if the site activity has been provided (column F) || "
        filtered_df = indexed_df.filter(indexed_df.activity.isNotNull() & indexed_df.recovery_time.isNull())
        filtered_ids = [int(row.id) for row in filtered_df.collect()]
        self.add_to_comments(part_site_map_messages['recovery_time_blank'], filtered_ids)

        # Validation: 'Recovery time should not be invalid, it should be number between 1 to 52'
        # Actual message : "Please insert the recovery time between 1 to 52 (weeks) if the site activity has been provided (column F) || "
        # TO DO : check if float values are accepted in original code, because in pyspark it is allowing float
        filtered_df = indexed_df.filter(
            indexed_df.recovery_time.isNotNull() & (~ indexed_df.recovery_time.between(1, 52)))
        filtered_ids = [int(row.id) for row in filtered_df.collect()]
        self.add_to_comments(part_site_map_messages['recovery_time_invalid'], filtered_ids)

        # Validation: 'Alternate Site Number should be valid from tab 6 'Site List''
        # Actual message : "Input alternate site number from Tab '6. Site List', column A (column G) || "
        filtered_df = indexed_df.filter((indexed_df.alternate_site_number.isNotNull()) & (
                lit(is_site_tab_empty) | (indexed_df.alternate_site_number.isin(invalid_alt_sites))))
        filtered_ids = [int(row.id) for row in filtered_df.collect()]
        self.add_to_comments(part_site_map_messages['alt_site_number_absent'], filtered_ids)

        # Validation: 'Alternate site and Primary site should not be same'
        # Actual message : "Please ensure alternate site is different from the primary site (column G) || "
        filtered_df = indexed_df.filter(
            (indexed_df.site_number.isNotNull() & indexed_df.alternate_site_number.isNotNull()) & (
                    indexed_df.site_number == indexed_df.alternate_site_number))
        filtered_ids = [int(row.id) for row in filtered_df.collect()]
        self.add_to_comments(part_site_map_messages['altsite_primarysite_same'], filtered_ids)

        # Validation: 'Alternate Site Number should not be blank if alternate site bring up time is provided'
        # Actual message : "Please insert alternate site number if the alternate site bring up time has been provided (column G) || "
        filtered_df = indexed_df.filter(
            indexed_df.alternate_site_number.isNull() & indexed_df.alternate_site_bring_up_time.isNotNull())
        filtered_ids = [int(row.id) for row in filtered_df.collect()]
        self.add_to_comments(part_site_map_messages['altsite_blank'], filtered_ids)

        # Validation: 'Alternate Site Bring Up Time should not be blank and it should be number between 1 to 51 if alternate site number is provided'
        # Actual message : "Please ensure alternate site bring up time is a whole number between 1 to 51 ( weeks) if alternate site number has been provided (column H) || "
        filtered_df = indexed_df.filter(
            indexed_df.alternate_site_number.isNotNull() & indexed_df.alternate_site_bring_up_time.isNull())
        filtered_ids = [int(row.id) for row in filtered_df.collect()]
        self.add_to_comments(part_site_map_messages['altsite_bringuptime_blank'], filtered_ids)

        # Validation: 'Alternate Site Bring Up Time should be between number 1 to 51 and should be less than recovery time'
        # Actual message : "Please ensure alternate site bring up time  is a whole number between 1 to 51 (Weeks) and it should be less than recovery time (column H) || "
        # TO DO : check if float values are accepted in original code, because in pyspark it is allowing float
        filtered_df = indexed_df.filter(
            indexed_df.alternate_site_bring_up_time.isNotNull() & (
                ~ indexed_df.alternate_site_bring_up_time.between(1, 51)))
        filtered_ids = [int(row.id) for row in filtered_df.collect()]
        self.add_to_comments(part_site_map_messages['altsite_bringuptime_invalid'], filtered_ids)

        # Validation: 'Alternate Site Bring Up Time should not be greater than recovery time'
        # Actual message : "Please insert the alternate site bring up time between 1 to 51 (Weeks) and it should be less than recovery time (column H) || "
        # TO DO : check if float values are accepted in original code, because in pyspark it is allowing float
        filtered_df = indexed_df.filter(
            indexed_df.alternate_site_bring_up_time.isNotNull() & (
                ~ indexed_df.alternate_site_bring_up_time.between(1, 51)) & (
                    indexed_df.alternate_site_bring_up_time >= indexed_df.recovery_time))
        filtered_ids = [int(row.id) for row in filtered_df.collect()]
        self.add_to_comments(part_site_map_messages['bringuptime_greater_recoverytime'], filtered_ids)

        # Validation: 'Primary and alternate sites should be of same partner'
        # Actual message : "Please ensure the partner name of the alternate site matches the partner name of the primary site (column D and column G ) || "
        filtered_df = indexed_df.filter((indexed_df.site_number.isNotNull()
                                         & indexed_df.alternate_site_number.isNotNull()
                                         & indexed_df.partner_name.isNotNull())
                                        & (indexed_df.partner_alt_site_key.isin(invalid_partner_alt_sites)))
        filtered_ids = [int(row.id) for row in filtered_df.collect()]
        self.add_to_comments(part_site_map_messages['partner_not_same_for_alt_site'], filtered_ids)

        # TO DO Validation: 'Row should not be duplicated with columns - Site number, resilinc partner name, partner part, activity, recovery time, alternate site number, alternate site bring up time'
        # Actual message : "The Site number, resilinc partner name, partner part, activity, recovery time, alternate site number, alternate site bring up time should not be duplicate (column D, column I, column B, column E,column G and column H) || "
        # filtered_df = indexed_df.filter((indexed_df.partner_name.isNotNull() & indexed_df.site_number.isNotNull()
        #                                  & indexed_df.partner_part_number.isNotNull() & indexed_df.activity.isNotNull()
        #                                  & indexed_df.recovery_time.isNotNull())
        #                                 & (indexed_df.duplicate == True))
        # filtered_ids = [int(row.id) for row in filtered_df.collect()]
        # self.add_to_comments(part_site_map_messages['duplicate_pname_ppart_snum'], filtered_ids)

        print('Errors : ', self.comments)
        print('Warnings : ', self.warnings)

        LOGGER.info(f"Errors : {self.comments}")
        LOGGER.info(f"Warnings : ', {self.warnings}")
        LOGGER.info("Part-Site Map Validation completed....")
