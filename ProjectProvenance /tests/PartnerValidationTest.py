from pyspark.sql import SparkSession
from utils import csv_to_pyspark_df
from validator.ParnterValidation import PartnerValidation
from utils.regex_constant import partner_file_path

spark = SparkSession \
    .builder.config('spark.driver.memory', '12g') \
    .appName("CSVReader").master("local[*]") \
    .getOrCreate()

spark.conf.set("spark.sql.debug.maxToStringFields", 1000)

sc = spark.sparkContext

# A CSV dataset is pointed to by path.
# The path can be either a single CSV file or a directory of CSV files

partner_df = csv_to_pyspark_df(spark, partner_file_path)
new_column_names = [col.lower().replace(" ", "_").replace("#", "number").replace('-','').replace(':','').replace('(','').replace(')','') for col in partner_df.columns]
partner_df = partner_df.toDF(*new_column_names)


err_schm=["partner_name","partner_contact_name","partner_emergency_contact_name","partner_emergency_contact_email","partner_contact__email","partner_emergency_contact_work_phone","partner_contact_work_phone","financial_risk_1_credit_risk_score"\
                            "financial_risk_2_health_risk_score","financial_risk_3_debt_rating_score","financial_risk_4__z_risk_score","partner_custom_defined_risk_score_1","partner_custom_defined_risk_score_2","partner_custom_defined_risk_score_3","partner_custom_defined_risk_score_4","partner_custom_defined_risk_score_5","partner_type","partner_duns_number"]

partner_validation = PartnerValidation(partner_df)
error,warning=partner_validation.validate()

errors = error.toDF(err_schm,sampleRatio=0.01)
Errors,Warnings=partner_validation.merger(errors,warning)
