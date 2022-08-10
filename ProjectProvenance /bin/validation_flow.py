#!/usr/bin/env python3

#
# Copyright (c) 2022 Resilinc, Inc., All Rights Reserved
#   Any use, modification, or distribution is prohibited
#   without prior written consent from Resilinc, Inc.
#

# Third Party Imports
import prefect.utilities.logging
from prefect import Flow
from prefect.run_configs import LocalRun
from pyspark import SparkConf

# Local Imports
from Log import log_global
from managers.SparkSessionManager import SparkCluster
from configuration import APP_NAME
from utils import get_project_root
from workflow_task_definitions.ValidationTasks import prepare_partner_tab_input_df, prepare_product_tab_input_df, \
    prepare_site_tab_input_df, prepare_bom_input_df, validate_not_null_partner_name, validate_not_null_product_name, \
    validate_not_null_site_site_number, validate_lookup_partner_name_product_partner_name, \
    validate_not_null_bom_product_name, write_back_errors_to_cdif_file, node, cleanup, \
    validate_customer_con_info_email_address

LOGGER = log_global()
PREFECT_LOGGER = prefect.utilities.logging.get_logger()

with Flow("Validation") as validation_flow:
    conf = SparkConf().setMaster('local[*]').setAppName(APP_NAME).set('spark.driver.memory', '12g')
    with SparkCluster(conf) as spark_s:

        first_step = [prepare_partner_tab_input_df(spark_s),
                      prepare_product_tab_input_df(spark_s),
                      prepare_site_tab_input_df(spark_s),
                      prepare_bom_input_df(spark_s),
                      ]
        second_step = [validate_not_null_partner_name(spark_s),
                       validate_not_null_product_name(spark_s),
                       validate_not_null_site_site_number(spark_s),
                       validate_lookup_partner_name_product_partner_name(),
                       validate_not_null_bom_product_name(spark_s),
                       validate_customer_con_info_email_address(spark_s)
                       ]

        node().set_dependencies(upstream_tasks=first_step, downstream_tasks=second_step)
        last_step = write_back_errors_to_cdif_file().set_dependencies(upstream_tasks=second_step)
        cleanup(spark_s).set_dependencies(upstream_tasks=[last_step])


if __name__ == '__main__':
    work_dir: str = get_project_root()
    LOGGER.info("Using [{}] as work directory for this flow.".format(work_dir))
    validation_flow.run_config = LocalRun(
        working_dir=work_dir
    )

    # To see the schematic of the flow.
    validation_flow.visualize()

    # To run the flow as a Python program, can be used for local debugging and pre-deployment testing.
    validation_flow.run()

    # To Register the flow to the project.
    # PREFECT_LOGGER.info(validation_flow.register(project_name=APP_NAME))

