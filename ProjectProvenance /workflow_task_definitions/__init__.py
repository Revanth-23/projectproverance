#
# Copyright (c) 2022 Resilinc, Inc., All Rights Reserved
#   Any use, modification, or distribution is prohibited
#   without prior written consent from Resilinc, Inc.
#

import prefect.utilities.logging
from prefect import task
from pyspark.sql import SparkSession

from Log import log_global

LOGGER = log_global()
PREFECT_LOGGER = prefect.utilities.logging.get_logger()

@task
def get_data(spark_session: SparkSession, csv_file_to_read):
    LOGGER.info("Running task get data")

    path = "../data/csvs/esea_master_dmg_demos.part1.csv"
    df = spark_session.read.csv(path, header=True)
    df.createOrReplaceTempView("partner")
    return 'partner'