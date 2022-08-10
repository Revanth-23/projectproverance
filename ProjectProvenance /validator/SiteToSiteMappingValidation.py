from pyspark.sql.functions import monotonically_increasing_id, concat_ws
from typing import List
from Log import log_global
from validator.ValidationMessages import sitemapping_messages

LOGGER = log_global()


class SiteMappingValidation(object):

    def __init__(self, data):
        self.data = data
        self.comments = {}
        self.warnings = {}
        LOGGER.info(f"Data loaded: {self.data}")

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

    def validate(self, df_site_list, df_part_sourcing):
        LOGGER.info("Validation begins.")
        # Give index to filter data
        df_index = self.data.select("*").withColumn("id", monotonically_increasing_id())

        #  from_site_number_blank
        filter_df = df_index.filter(df_index['From Site Number'].isNull())
        filtered_id = [int(row.id) for row in filter_df.collect()]
        self.add_to_comments(sitemapping_messages['from_site_number_blank'], filtered_id)

        # from_site_number_absent
        filter_df = df_index.filter(df_index['From Site Number'].isNotNull()) #Else condition pending
        if filter_df.count() > 0 and df_site_list.count() > 0:
            df_diff = filter_df.join(df_site_list, filter_df["From Site Number"] == df_site_list["Site Number"],
                                     how='leftanti')
            filtered_id = [int(row.id) for row in df_diff.collect()]
            self.add_to_comments(sitemapping_messages['from_site_number_absent'], filtered_id)

        # to_site_number_blank
        filter_df = df_index.filter(df_index['To Site Number'].isNull())
        filtered_id = [int(row.id) for row in filter_df.collect()]
        self.add_to_comments(sitemapping_messages['to_site_number_blank'], filtered_id)

        # to_site_number_absent
        filter_df = df_index.filter(df_index['From Site Number'].isNotNull()) #Else condition pending
        if filter_df.count() > 0 and df_site_list.count() > 0:
            df_diff = filter_df.join(df_site_list, filter_df["To Site Number"] == df_site_list["Site Number"],
                                    how='leftanti')
            filtered_id = [int(row.id) for row in df_diff.collect()]
            self.add_to_comments(sitemapping_messages['to_site_number_absent'], filtered_id)

        # identical_fromsite_tosite
        filter_df = df_index.filter(df_index['From Site Number'] == df_index['To Site Number'])
        filtered_id = [int(row.id) for row in filter_df.collect()]
        self.add_to_comments(sitemapping_messages['identical_fromsite_tosite'], filtered_id)

        # # duplicate_fromsite_tosite
        # filter_df = df_index.filter(
        #     df_index['From Site Number'].isNotNull() & df_index['To Site Number'].isNotNull())
        # duplicate = filter_df.exceptAll(
        #     filter_df.drop_duplicates(['From Site Number', 'To Site Number']))
        # filtered_id = [int(row.id) for row in duplicate.collect()]
        # self.add_to_comments(sitemapping_messages['duplicate_fromsite_tosite'], filtered_id)

        # partner_part_absent
        notNull_part = df_index.filter(df_index['Partner Part #'].isNotNull()) #Else condition pending
        if df_part_sourcing.count() > 0 or notNull_part.count() > 0:
            filter_df = notNull_part.join(df_part_sourcing,
                                      notNull_part["Partner Part #"] == df_part_sourcing["Partner Part #"],
                                      how='leftanti')
            filtered_id = [int(row.id) for row in filter_df.collect()]
            self.add_to_comments(sitemapping_messages['partner_part_absent'], filtered_id)

        # partner_part_invalid
        # filter_df = notNull_part.join(df_site_list, notNull_part['From Site Number'] == df_site_list['Site Number'],
        #                               how="leftsemi")
        # valid_part = filter_df.join(df_part_sourcing, filter_df['Partner Part #'] == df_part_sourcing['Partner Part #'],
        #                             how="leftsemi")
        # concate_site_part = valid_part.select(
        #     concat_ws('_', valid_part['Partner Part #'], valid_part['From Site Number'].alias('valid')), 'id')
        # filter_df = df_part_sourcing.join(df_site_list,
        #                                   df_part_sourcing['Partner Name'] == df_site_list['Partner Name'],
        #                                   how="inner")
        # concate_part_site = filter_df.select(
        #     concat_ws('_', filter_df['Partner Part #'], filter_df['Site Number'].alias('valid')))
        # filter_df = concate_site_part.join(concate_part_site, concate_site_part[
        #     'concat_ws(_, Partner Part #, From Site Number AS valid)'] == concate_part_site[
        #                                        'concat_ws(_, Partner Part #, Site Number AS valid)'],
        #                                    how="leftanti")
        # filtered_id = [int(row.id) for row in filter_df.collect()]
        # self.add_to_comments(sitemapping_messages['partner_part_invalid'], filtered_id)

        LOGGER.info("Validation finished.")
