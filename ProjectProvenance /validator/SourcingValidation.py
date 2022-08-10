from validator.Messages import Messages
from utils import *
from Log import log_global

LOGGER = log_global()


class PartSourcing(object):

    def __init__(self, df):
        """
            validation() :   this function is used to validate sourcing data for error and warnings!!
            merger()     :   this merger is used to merge all error in error columns and warnings in warnings columns
                             and assign both error & warnings dataframe to instance error,warnings variables!
        Args:
            df (pyspark dataframe): All the validation are performed on this pyspark dataframe
                                   error and warnings are generated based on the validation logics.
        """
        self.df = df
        self.error, self.warnings = None, None
        self.sourcing_status = ['Sole-Sourced', 'Multi-Sourced', 'Single-Sourced']
        self.qualification_status_list = ['Qualified', 'In Progress', 'Going EoL']

    def validation(self):
        """
            This validation function perform validation on the original input dataframe
            and generate error and warnings with respect to the row!!
        Returns:
            this return dataframe with merged error columns
            and warnings columns with all warnings
        """
        try:
            LOGGER.info("\n\n\n" + "-" * 20 + "Validation started!!" + "-" * 20 + "\n\n")
            # validation for error and warnings generation of data
            error = self.df.rdd.map(
                lambda x: (None if x.sourcing is not None and x.sourcing in self.sourcing_status else
                           Messages.partsourcing_messages['sourcing_field_invalid'],
                           None if x.qualification_status is not None
                           and x.qualification_status in self.qualification_status_list else
                           Messages.partsourcing_messages['qualification_status_invalid'],
                           None if not (x.part is None and x.partner_part is None) else
                           Messages.partsourcing_messages['either_cpn_ppn'],
                           None if is_valid_revenue_between_range(x.annual_spend_on_partner_part, 0,
                                                                  1000000000000) else
                           Messages.partsourcing_messages['annual_spend_invalid'],
                           None if x.partner_part is not None else Messages.partsourcing_messages[
                               'partner_part_blank'],
                           (None if x.split is not None and is_float(x.split) and not (
                                   float(x.split) < 0 or float(x.split) > 100)
                            else Messages.partsourcing_messages['split_invalid']) if (
                                   x.part is not None) else
                           Messages.partsourcing_messages['customer_part_blank'],
                           None if x.part_custom_defined_risk_score_1 is not None and is_valid_risk(
                               x.part_custom_defined_risk_score_1) else
                           Messages.partsourcing_messages['custom_defined_risk1_invalid'],
                           None if x.part_custom_defined_risk_score_2 is not None and is_valid_risk(
                               x.part_custom_defined_risk_score_2) else
                           Messages.partsourcing_messages['custom_defined_risk2_invalid'],
                           None if x.part_custom_defined_risk_score_3 is not None and is_valid_risk(
                               x.part_custom_defined_risk_score_3) else
                           Messages.partsourcing_messages['custom_defined_risk3_invalid'],
                           None if x.part_custom_defined_risk_score_4 is not None and is_valid_risk(
                               x.part_custom_defined_risk_score_4) else
                           Messages.partsourcing_messages['custom_defined_risk4_invalid'],
                           None if x.part_custom_defined_risk_score_5 is not None and is_valid_risk(
                               x.part_custom_defined_risk_score_5) else
                           Messages.partsourcing_messages['custom_defined_risk5_invalid']
                           ))
            LOGGER.info("\n\nError Validation completed!!\n")

            # warnings generation code
            warnings = self.df.select(['part', 'Description', 'partner_part', 'annual_spend_on_partner_part']).rdd.map(
                lambda x: ((Messages.partsourcing_messages['multiple_spends'] if
                            len(comb(x.partner_part, x.annual_spend_on_partner_part, run='spend')) >= 2 else
                            None) if x.partner_part is not None else Messages.partsourcing_messages[
                    'customer_part_blank'],
                           (Messages.partsourcing_messages['multiple_description'] if not len(
                               comb(x.part, x.Description)) >= 2 else None)
                           if x.part is not None else Messages.partsourcing_messages['customer_part_blank']))
            LOGGER.info("\n\nWarnings Validation completed!!\n\n")
            LOGGER.info("\n\n\n" + "-" * 30 + "Validation done!!" + "-" * 30 + "\n\n")
            return error, warnings
        except Exception as e:
            LOGGER.error(e)
