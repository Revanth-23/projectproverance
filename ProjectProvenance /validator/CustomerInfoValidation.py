from pyspark.sql.functions import monotonically_increasing_id, when, col, lit
from typing import List
from Log import log_global
from validator.ValidationMessages import contact_info_messages
LOGGER = log_global()


class CustomerValidation(object):

    def __init__(self, data):
        self.data = data
        self.comments = {}
        self.warnings = {}
        self.regex = '^\w+([\.-]?\w+)*@\w+([\.-]?\w+)*(\.\w{2,3})+$'
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

    def validate(self):
        LOGGER.info("Validation begins.")
        # Give index to filter data
        df_index = self.data.select("*").withColumn("id", monotonically_increasing_id())

        # validation for name blank
        df_filtered = df_index.filter(
            df_index['Contact Email Address'].isNotNull() & df_index['Name of Contact'].isNull())
        df = [int(row.id) for row in df_filtered.collect()]
        self.add_to_comments(contact_info_messages['name_blank'], df)

        # validation for email blank
        df_filtered = df_index.filter(
            df_index['Name of Contact'].isNotNull() & df_index['Contact Email Address'].isNull())
        df = [int(row.id) for row in df_filtered.collect()]
        self.add_to_comments(contact_info_messages['email_blank'], df)

        # validation for name and email blank
        df_filtered = df_index.filter(
            df_index['Contact Phone Number'].isNotNull() & df_index['Contact Email Address'].isNull() & df_index[
                'Name of Contact'].isNull())
        df = [int(row.id) for row in df_filtered.collect()]
        self.add_to_comments(contact_info_messages['email_name_null_provided'], df)

        # Email validation
        mail = df_index.filter(df_index['Contact Email Address'].isNotNull())
        valid = mail.withColumn("flag",
                                when(col("Contact Email Address").rlike(self.regex), lit("valid")).otherwise(
                                    lit("invalid")))
        df_filtered = valid.where(valid.flag != 'valid')
        df = [int(row.id) for row in df_filtered.collect()]
        self.add_to_comments(contact_info_messages['email_invalid'], df)

        # Get Duplicate rows validation(name and email combination same)
        # name_email = df_index.filter(df_index['Contact Email Address'].isNotNull())
        # df_filtered = name_email.exceptAll(name_email.drop_duplicates(['Name of Contact', 'Contact Email Address']))
        # df = [int(row.id) for row in duplicate.df_filtered()]
        # self.add_to_warnings('duplicate_cust_contact_info_tab', df)

        # Get Duplicate rows validation (different name and same email)
        # df_filtered = mail.exceptAll(mail.drop_duplicates(['Contact Email Address']))
        # df = [int(row.id) for row in df_filtered.collect()]
        # self.add_to_warnings(contact_info_messages['duplicate_email_diff_contact_tab'], df)

        print('Errors : ', self.comments)
        print('Warnings : ', self.warnings)
        LOGGER.info("Validation finished.")




