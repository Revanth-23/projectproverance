import time
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from poc.Messages import Messages
from poc.duplicate_finder import duplicated
from pyspark.sql import window as w
from utils.generic_function import *
from utils.Queries import get_partner_picklist

from Log import log_global
LOGGER = log_global()

class PartnerValidation(object):

    def __init__(self, df):
        
        self.df = df.select("*").withColumn("id", monotonically_increasing_id())
        self.error,self.warnings = None,None
        
    #generating error and warning dataframes
    def merger(self,error,warning):

        try:
            LOGGER.info('Merging error and warning into dataframe')
            self.error=error.select(concat_ws('||',*error.columns).alias('error')).toPandas()
            self.warnings=warning.select('warnings').toPandas()
            return self.error,self.warnings
                    
        except Exception as e:
            LOGGER.error(e)
            #print(e)

    def validate(self):

        try:
            LOGGER.info('Validate starts')

            #Connecting to database getting picklist for partner type
            enum_labels=get_partner_picklist()
            pyspark.sql.DataFrame.duplicated = duplicated
            
            #error validate
            errors = self.df.rdd.map(lambda x: (None if x.partner_name != None  else\
                        Messages.partners_messages['partner_name_blank'],\
                        None if x.partner_contact_name!=None else\
                        Messages.partners_messages['partner_contact_name_blank'],\
                        None if x.partner_emergency_contact_name != None  else\
                        Messages.partners_messages['emergency_contact_name_blank'],\

                        #Email_validate

                        None if x.partner_emergency_contact_email !=None and is_valid_email(x.partner_emergency_contact_email) else\
                        Messages.partners_messages['emergency_contact_email_blank'],\
                        None if x.partner_contact__email!=None and is_valid_email(x.partner_contact__email) else\
                        Messages.partners_messages['partner_contact_email_blank'],\
                                                
                        #Contact_work_phone

                        None if x.partner_emergency_contact_work_phone !=None  else\
                        Messages.partners_messages['emergency_contact_name_email_blank'],\
                        None if x.partner_contactwork_phone !=None  else\
                        Messages.partners_messages['partner_contact_name_email_blank'],\
                    
                        # Financial risk validate

                        None if x.financial_risk_1_credit_risk_score !=None and is_valid_risk(x.financial_risk_1_credit_risk_score) else\
                        Messages.partners_messages['financial_risk1_invalid'],\
                        None if x.financial_risk_2_health_risk_score !=None and is_valid_risk(x.financial_risk_2_health_risk_score) else\
                        Messages.partners_messages['financial_risk2_invalid'],\
                        None if x.financial_risk_3_debt_rating_score !=None and is_valid_risk(x.financial_risk_3_debt_rating_score) else\
                        Messages.partners_messages['financial_risk3_invalid'],\
                        None if x.financial_risk_4__z_risk_score !=None and is_valid_risk(x.financial_risk_4__z_risk_score) else\
                        Messages.partners_messages['financial_risk4_invalid'],\
                                                                    
                        #Custom defined risk validate

                        None if x.partner_custom_defined_risk_score_1 !=None and is_valid_risk(x.partner_custom_defined_risk_score_1) else\
                        Messages.partners_messages['custom_defined_risk1_invalid' ],\
                        None if x.partner_custom_defined_risk_score_2 !=None and is_valid_risk(x.partner_custom_defined_risk_score_2) else\
                        Messages.partners_messages['custom_defined_risk2_invalid'  ],\
                        None if x.partner_custom_defined_risk_score_3 !=None and is_valid_risk(x.partner_custom_defined_risk_score_3) else\
                        Messages.partners_messages['custom_defined_risk3_invalid'  ],\
                        None if x.partner_custom_defined_risk_score_4 !=None and is_valid_risk(x.partner_custom_defined_risk_score_4) else\
                        Messages.partners_messages['custom_defined_risk4_invalid'  ],\
                        None if x.partner_custom_defined_risk_score_5 !=None and is_valid_risk(x.partner_custom_defined_risk_score_5) else\
                        Messages.partners_messages['custom_defined_risk5_invalid'  ],\
                                                
                        # Check Partner Type is from picklist

                        None if x.partner_type!=None and x.partner_type in enum_labels else\
                        Messages.partners_messages['partner_type_picklist'],\

                        # Duns Validation 

                        None if x.partner_duns_number!=None and is_duns_valid(x.partner_duns_number) else\
                        Messages.partners_messages['invalid_duns']
                        ))
                                              
            #Warning Validate
            warning_dup = self.df.duplicated(subset=["partner_contact__email","partner_emergency_contact_email"]).orderBy(col("id").asc())
            warning_dup =warning_dup.withColumn("warnings", when( warning_dup.duplicate_indicator , Messages.partners_messages['partners_duplicate']))
            
            return errors,warning_dup
                 
        except Exception as e:
            LOGGER.error(e) 
            
        