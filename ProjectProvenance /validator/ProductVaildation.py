import sys
sys.path.append('../')
import pyspark
from Messages import Messages
import json
from Log import log_global
LOGGER = log_global()
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as f
from pyspark.sql.functions import collect_set,sum,avg,max,countDistinct,count
from pyspark.sql.functions import monotonically_increasing_id 
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType    
from pyspark.sql.window import Window
import time



@udf(returnType=StringType()) 
def is_valid_revenue_between_range( val,column):
    try:
        
        if val:
            val = str(val).replace(',','').replace('$','')
            val = float(val)
            if not (val >= 0 and val < 100000000000):
                return Messages.product_messages[column]
            else:
                return None
        else:
            return None
    except :
        return Messages.product_messages[column]

@udf(returnType=StringType()) 
def fg_range( val):
    try:
        if int(val):
            val = int(val)
            if not (val >= 1 and val < 52):
                return Messages.product_messages['fg_inventory_invalid']
            else:
                return None
    except:
        return Messages.product_messages['fg_inventory_invalid']

def is_float(val):
    try:
        float(str(val))
        return True
    except Exception as ex:
        return False




class ProductValidation(object):

    def __init__(self, data):
        self.data = data.select("*").withColumn("id", monotonically_increasing_id())
 
        # LOGGER.info(f"Data loaded: {self.data}")


    def duplicated(self, subset=None, orderby=None, ascending=False, keep="first" ):

        if subset == None:
            subset = self.schema.names

        subset = [subset] if isinstance(subset, str) else subset

        assert keep in ["first", "last", False], "keep must be either first, last or False"

        if orderby == None:
            orderby = subset
        elif isinstance(orderby, str):
            orderby = [orderby]

        if isinstance(ascending, bool):
            if ascending:
                ordering = [f.asc] * len(orderby)
            else:
                ordering = [f.desc] * len(orderby)

        elif isinstance(ascending, list):
            assert all(
                [isinstance(i, bool) for i in ascending]
            ), "ascending should be bool or list of bool"
            ordering = [f.asc if i else f.desc for i in ascending]

        w1 = (
            Window.partitionBy(*subset)
            .orderBy(*[ordering[idx](i) for idx, i in enumerate(orderby)])
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
        w2 = (
            Window.partitionBy(*subset)
            .orderBy(*[ordering[idx](i) for idx, i in enumerate(orderby)])
            .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        )

        self = self.sort(orderby, ascending=ascending).withColumn(
            "seq", f.row_number().over(w1)
        )

        if keep == "first":
            self = self.withColumn(
                "duplicate_indicator", f.when(f.col("seq") == 1, False).otherwise(True)
            ).drop(*["seq"])

        elif keep == "last":
            self = (
                self.withColumn("max_seq", f.max("seq").over(w2))
                .withColumn(
                    "duplicate_indicator",
                    f.when(f.col("seq") == f.col("max_seq"), False).otherwise(True),
                )
                .drop(*["seq", "max_seq"])
            )

        else:
            self = (
                self.withColumn("max_seq", f.max("seq").over(w2))
                .withColumn(
                    "duplicate_indicator",
                    f.when(f.col("max_seq") > 1, True).otherwise(False),
                )
                .drop(*["seq", "max_seq"])
            )

        return self


    pyspark.sql.DataFrame.duplicated = duplicated


    def error_validation(self):
        LOGGER.info("Start Error Validation")

        data = self.data
        try:
            # Product Name 
            data = data.withColumn("product_name" , when(data['Product Name'].isNull() , Messages.product_messages['product_name_blank']))
            
            #RevenueQ1
            data  =  data.withColumn("revenue_q1" , when(data['Revenue Q1'].isNull() ,  Messages.product_messages['revenue_not_provided']))
            
            # FG Inventory range validation
            data = data.withColumn("fg_inventory", fg_range(data["FG Inventory (weeks)"]))

            # Revenues columns Range  Validation
            data = data.withColumn("revenue_q1_value", is_valid_revenue_between_range(data["Revenue Q1"],lit("revenue_q1_invalid"))) 
            data = data.withColumn("revenue_q2_value", is_valid_revenue_between_range(data["Revenue Q2"],lit("revenue_q2_invalid"))) 
            data = data.withColumn("revenue_q3_value", is_valid_revenue_between_range(data["Revenue Q3"],lit("revenue_q3_invalid"))) 
            data = data.withColumn("revenue_q4_value", is_valid_revenue_between_range(data["Revenue Q4"],lit("revenue_q4_invalid"))) 
        except Exception as e:
             LOGGER.error(e)
            
        df2=data.select(concat_ws('||',data.product_name,data.revenue_q1,data.fg_inventory,
        data.revenue_q1_value,data.revenue_q2_value,data.revenue_q3_value,data.revenue_q4_value).alias("error"))

        LOGGER.info(df2.show(100,truncate= False))
       
        LOGGER.info("End Error Validation")
       
        return df2 



    def warning_validation(self):

        LOGGER.info("Start Warning Validation")

        data = self.data

        try:
            # Duplicates column Validations
            warning_dup =  data.duplicated(subset = ["Product name","PRODUCT HIERARCHY LEVEL 1","PRODUCT HIERARCHY LEVEL 2","PRODUCT HIERARCHY LEVEL 3","PRODUCT HIERARCHY LEVEL 4","PRODUCT HIERARCHY LEVEL 5","PRODUCT HIERARCHY LEVEL 6"]).orderBy(col("id").asc())
            data=warning_dup.withColumn("duplipcate_warnings", when( warning_dup.duplicate_indicator , Messages.product_messages['duplicate_product_tab'])) 
            
            # Reveuns duplicate colums validation
            data = data.drop('duplicate_indicator')
            revenue_q1_warning =  data.duplicated(subset = ["Product name","Revenue Q1"]).orderBy(col("id").asc())
            data =revenue_q1_warning.withColumn("revene_q1_multiple", when( revenue_q1_warning.duplicate_indicator , Messages.product_messages['revenue_q1_multiple'])) 
            
            data = data.drop('duplicate_indicator')
            revenue_q2_warning =  data.duplicated(subset = ["Product name","Revenue Q2"]).orderBy(col("id").asc())
            data =revenue_q2_warning.withColumn("revene_q2_multiple", when( revenue_q2_warning.duplicate_indicator , Messages.product_messages['revenue_q2_multiple'])) 
        
            data = data.drop('duplicate_indicator')
            revenue_q3_warning =  data.duplicated(subset = ["Product name","Revenue Q3"]).orderBy(col("id").asc())
            data =revenue_q3_warning.withColumn("revene_q3_multiple", when( revenue_q3_warning.duplicate_indicator , Messages.product_messages['revenue_q3_multiple'])) 
            
            data = data.drop('duplicate_indicator')
            revenue_q4_warning =  data.duplicated(subset = ["Product name","Revenue Q4"]).orderBy(col("id").asc())
            data =revenue_q4_warning.withColumn("revene_q4_multiple", when( revenue_q4_warning.duplicate_indicator , Messages.product_messages['revenue_q4_multiple'])) 
        except Exception as e:
             LOGGER.error(e)   
        
        df2=data.select(concat_ws('||',data.duplipcate_warnings,data.revene_q1_multiple,data.revene_q2_multiple,
        data.revene_q3_multiple,data.revene_q4_multiple).alias("warnings"))
        LOGGER.info(df2.show(100,truncate= False))

        LOGGER.info("End Warning Validation")

        return df2

   
    def validate(self,orginal_df):
        error = self.error_validation()
        warnings = self.warning_validation()
        return error,warnings

        ## testing 
        ##create combine df with error and warnings cloums
        # try:
        #     error_df = self.error_validation()
        #     warning_df = self.warning_validation()
        #     LOGGER.info("\n\nMerging started!!")
        #     df2 = orginal_df.toPandas()
        #     df2['error'] = error_df.toPandas()
        #     df2['warnings'] = warning_df.toPandas()
        #     LOGGER.info(df2)
        #     LOGGER.info("\n\nMerging done")
        #     return df2
        # except Exception as e:
        #     LOGGER.error(e)

