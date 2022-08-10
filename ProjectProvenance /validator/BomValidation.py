import pyspark.pandas as ps

from pyspark.sql.functions import monotonically_increasing_id, lit
from typing import List
from Log import log_global
from validator.ValidationMessages import bom_messages
LOGGER = log_global()


class BomValidation(object):

    def __init__(self, data):
        ps.set_option('compute.ops_on_diff_frames', True)
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

    def validation(self, part_df, product_df):
        LOGGER.info("Validation begins.")
        df_index = self.data.select("*").withColumn("id", monotonically_increasing_id())
        invalid_products = []
        invalid_parts = []
        is_sourcing_tab_empty = (part_df.count() == 0)
        is_product_tab_empty = (product_df.count() == 0)

        if not is_sourcing_tab_empty:
            lookup_df = self.data.join(part_df, self.data["part_number"] == part_df["part_number"], "leftanti")
            invalid_parts = [row.part_number for row in lookup_df.collect()]
            invalid_parts = set(invalid_parts)

        if not is_product_tab_empty:
            lookup_df = self.data.join(product_df, self.data["product_name"] == product_df["product_name"], "leftanti")
            invalid_products = [row.product_name for row in lookup_df.collect()]
            invalid_products = set(invalid_products)

        # Validation: 'Product name can not be blank'
        df_filtered = df_index.filter(df_index.product_name.isNull())
        filtered_ids = [int(row.id) for row in df_filtered.collect()]
        self.add_to_comments(bom_messages['product_name_blank'], filtered_ids)

        # Validation: 'Part number can not be blank'
        df_filtered = df_index.filter(df_index.part_number.isNull())
        filtered_ids = [int(row.id) for row in df_filtered.collect()]
        self.add_to_comments(bom_messages['customer_part_blank'], filtered_ids)

        # Validation: 'A product name provided should be present in tab 4 Product'
        df_filtered = df_index.filter((df_index.product_name.isNotNull()) & (
                    lit(is_product_tab_empty) | (df_index.product_name.isin(invalid_products))))
        filtered_ids = [int(row.id) for row in df_filtered.collect()]
        self.add_to_warnings(bom_messages['product_name_invalid'], filtered_ids)

        # Validation: ' A part number provided should be present in tab 2 Part sourcing'
        df_filtered = df_index.filter((df_index.part_number.isNotNull()) & (
                    lit(is_sourcing_tab_empty) | (df_index.part_number.isin(invalid_parts))))
        filtered_ids = [int(row.id) for row in df_filtered.collect()]
        self.add_to_warnings(bom_messages['customer_part_absent'], filtered_ids)

        '''
        # Validation: 'The product name, part number should not be duplicate'
        df_filtered = df_index.filter((df_index.product_name.isNotNull()) & (df_index.part_number.isNotNull()) & (df_index.duplicate == True))
        filtered_ids = [int(row.id) for row in df_filtered.collect()]
        self.add_to_warnings(bom_messages['duplicate_bom_tab'], filtered_ids)
        '''

        print('Errors : ', self.comments)
        print('Warnings : ', self.warnings)
        LOGGER.info("Validation finished.")
