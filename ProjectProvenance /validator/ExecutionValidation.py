import pyspark.pandas as ps
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import round, col, ceil
from pyspark.sql.functions import regexp_replace
from Log import log_global
LOGGER = log_global()


class  ExecutionValidation(object):

    def __init__(self, df_exec):
        ps.set_option('compute.ops_on_diff_frames', True)
        self.df_exec = df_exec
        self.comments = {}
        self.warnings = {}
        self.executiondetail_messages = {
            'cust_part_blank': "Please insert the part # (column A) || ",
            'cust_part_invalid': "Part# provided is not present in Column A of Tab 2 Part Sourcing (column A) || ",
            '3m_demand_invalid': "Please insert a number between 0 to 1000000000 (inclusive) for 3 month demand (column B) || ",
            '6m_demand_invalid': "Please insert a number between 0 to 1000000000 (inclusive) for 6 month demand (column C) || ",
            'inventory_onhand_invalid': "Please insert a number between 0 to 1000000000 (inclusive) for inventory on-hand (column D) || ",
            'inventory_onorder_invalid': "Please insert a number between 0 to 1000000000 (inclusive) for inventory on-order (column E) || ",
            'reduced_lead_time_invalid': "Please insert a number between 1 to 51 (inclusive) for reduced lead time and reduced leadtime should be less than full lead time (column F) || ",
            'full_lead_time_invalid': "Please insert a number between 1 to 52 (inclusive) for full lead time (column G) || ",
            'reduce_lead_less_full_lead': "Per industry recommendations, reduced lead time should be less than full lead time (column F) || ",
            'part_custom_defined_risk1_invalid': "Please enter a value between 1 to 10 for company part custom defined risk score 1 (column I) || ",
            'part_custom_defined_risk2_invalid': "Please enter a value between 1 to 10 for company part custom defined risk score 2 (column J) || ",
            'part_custom_defined_risk3_invalid': "Please enter a value between 1 to 10 for company part custom defined risk score 3 (column K) || ",
            'part_custom_defined_risk4_invalid': "Please enter a value between 1 to 10 for company part custom defined risk score 4 (column L) || ",
            'part_custom_defined_risk5_invalid': "Please enter a value between 1 to 10 for company part custom defined risk score 5 (column M) || ",
            'duplicate_part': "The part # should not be duplicate (column A) (If different risk scores are provided, then maximum value of risk score will be uploaded) || "
        }
        LOGGER.info(f"Data loaded: {self.df_exec}")

    def invalid_revenue_between_range(self,df_exec, column_name, error_message, start, end):
        df_exec = df_exec.withColumn(column_name, regexp_replace(col(column_name), "[$,]", ""))
        df_exec = df_exec.select("*", ceil(round(column_name))).drop(column_name).withColumnRenamed(
            "CEIL(round({}, 0))".format(column_name), column_name)
        df_invalid = df_exec.filter(
            (df_exec[column_name].isNotNull()) & ((df_exec[column_name] < start) | (df_exec[column_name] > end)))
        self.invalid_data_comments(df_invalid, error_message)

    # def invalid_risk(self,df_exec, column_name, error_message, start, end):
    #     df_invalid = df_exec.filter((df_exec[column_name].isNotNull()) &
    #                                 ((df_exec[column_name].like("%.%")) |
    #                                  ((df_exec[column_name] < start) | (
    #                                          df_exec[column_name] > end))))
    #     self.invalid_data_comments(df_invalid, error_message)

# Accpecting float values upto 2 decimals
    def invalid_risk(self,df_exec, column_name, error_message, start, end):
        df_exec=df_exec.select("*", round(col(column_name),2)).drop(column_name).withColumnRenamed(
            "round({}, 2)".format(column_name), column_name)
        df_exec.show()
        df_invalid = df_exec.filter((df_exec[column_name].isNotNull()) &
                                    ((df_exec[column_name] < start) | (
                                             df_exec[column_name] > end)))
        self.invalid_data_comments(df_invalid, error_message)

    def invalid_int_between_range(self,df_exec, column_name, error_message, start, end):
        df_invalid = df_exec.filter((df_exec[column_name].isNotNull()) &
                                    ((~df_exec[column_name].rlike("[0-9]")) |
                                     (df_exec[column_name].like("%.%")) |
                                     ((df_exec[column_name] < start) | (df_exec[column_name] > end))))
        self.invalid_data_comments(df_invalid, error_message)

    def invalid_data_comments(self,df_exec, error_message):
        df = [int(row.id) for row in df_exec.collect()]
        self.comments[self.executiondetail_messages[error_message]] = df

    def invalid_data_warnings(self,df_exec, error_message):
        df = [int(row.id) for row in df_exec.collect()]
        self.warnings[self.executiondetail_messages[error_message]] = df

    def validations_dict(self,comments):
        temp_comments = {}
        for error_msg, index in comments.items():
            for j in index:
                if j not in temp_comments:
                    temp_comments[j] = [str(error_msg)]

                else:
                    temp_comments[j].append(str(error_msg))
        comments = {k: (" ".join(v)) for k, v in temp_comments.items()}
        return comments

    def validation(self,df_sourcing):
        LOGGER.info("Validation begins.")
        ## Validation for PART column
        # Give index to filter data
        self.df_exec = self.df_exec.select("*").withColumn("id", monotonically_increasing_id())

        # validation for Part blank
        # Filter data
        df_filter = self.df_exec.filter(self.df_exec['Part #'].isNull())
        df = df_filter.select("id")
        self.invalid_data_comments(df, 'cust_part_blank')

        # Identifying Invalid Part number(If not present in Sourcing Tab)
        df_filter = self.df_exec.filter((self.df_exec['Part #'].isNotNull())) #will modify later
        if df_sourcing.count() > 0 and df_filter.count() > 0:
            df_diff = df_filter.join(df_sourcing, df_filter["Part #"] == df_sourcing["Part #"], how='leftanti')
            self.invalid_data_warnings(df_diff, 'cust_part_invalid')

        # # Identifying Duplicate records in Part column
        # df_dup = self.df_exec.groupBy("Part #").count().where("count > 1").alias("e").drop("count").withColumnRenamed(
        #     "Part #", 'part')
        # duplicate_ids = df_dup.select('part')
        # df_duplicate_parts = [int(row.part) for row in duplicate_ids.collect()]
        # df_invalid = self.df_exec.filter(self.df_exec["Part #"].isin(df_duplicate_parts))
        # self.invalid_data_warnings(df_invalid, 'duplicate_part')


        # Validations
        self.invalid_revenue_between_range(self.df_exec, '3M Demand', '3m_demand_invalid', 0, 1000000000)
        self.invalid_revenue_between_range(self.df_exec, '6M Demand', '6m_demand_invalid', 0, 1000000000)
        self.invalid_revenue_between_range(self.df_exec, 'Inventory On-Hand', 'inventory_onhand_invalid', 0, 1000000000)
        self.invalid_revenue_between_range(self.df_exec, 'Inventory On-Order', 'inventory_onorder_invalid', 0, 1000000000)
        self.invalid_risk(self.df_exec, 'Company Part Custom Defined Risk Score (1)', 'part_custom_defined_risk1_invalid', 0.1, 10)
        self.invalid_risk(self.df_exec, 'Company Part Custom Defined Risk Score (2)', 'part_custom_defined_risk2_invalid', 0.1, 10)
        self.invalid_risk(self.df_exec, 'Company Part Custom Defined Risk Score (3)', 'part_custom_defined_risk3_invalid', 0.1, 10)
        self.invalid_risk(self.df_exec, 'Company Part Custom Defined Risk Score (4)', 'part_custom_defined_risk4_invalid', 0.1, 10)
        self.invalid_risk(self.df_exec, 'Company Part Custom Defined Risk Score (5)', 'part_custom_defined_risk5_invalid', 0.1, 10)
        self.invalid_int_between_range(self.df_exec, 'Reduced Lead Time', 'reduced_lead_time_invalid', 1, 51)
        self.invalid_int_between_range(self.df_exec, 'Full Lead Time', 'full_lead_time_invalid', 1, 52)

        # Reduced lead time< Full lead time)
        df_invalid = self.df_exec.filter((self.df_exec['Reduced Lead Time'].isNotNull()) &
                                    (self.df_exec['Full Lead Time'].isNotNull()) &
                                    (((self.df_exec['Reduced Lead Time'] < 0) | (self.df_exec['Reduced Lead Time'] > 51)) |
                                     ((self.df_exec['Full Lead Time'] < 0) | (self.df_exec['Full Lead Time'] > 52)) |
                                     (self.df_exec['Full Lead Time'] < self.df_exec['Reduced Lead Time'])))

        self.invalid_data_comments(df_invalid, 'reduce_lead_less_full_lead')
        print(self.validations_dict(self.comments))
        print(self.validations_dict(self.warnings))

