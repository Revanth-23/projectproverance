
import prefect
from prefect import task, Flow, resource_manager
from prefect.run_configs import LocalRun
from pyspark import SparkConf
from pyspark.sql import SparkSession
from validator import NotNullValidator
from utils import csv_to_pyspark_df, get_project_root

PREFECT_LOGGER = prefect.context.get('logger')

CSV_BASE_PATH: str = get_project_root() + '/data/csvs/'


@resource_manager
class SparkCluster:
    def __init__(self, conf: SparkConf = SparkConf()):
        self.conf = conf

    def setup(self) -> SparkSession:
        return SparkSession.builder.config(conf=self.conf).getOrCreate()

    # noinspection PyMethodMayBeStatic
    def cleanup(self, spark: SparkSession):
        spark.stop()


def prepare_input_dfs(spark_session: SparkSession, csv_file_path: str, temp_view_name: str):
    if spark_session and csv_file_path and temp_view_name:
        PREFECT_LOGGER.info("Inside Get Data Task for {}".format(csv_file_path))
        df = csv_to_pyspark_df(spark_session, csv_file_path)
        PREFECT_LOGGER.info("Creating temp view: {}".format(temp_view_name))
        df.createOrReplaceTempView(temp_view_name)
        return temp_view_name
    else:
        raise ValueError("Correct Definition for task not provided.")


@task
def start_up():
    PREFECT_LOGGER.info("Running Preflight.")
    return True

@task
def prepare_partner_tab_input_df(spark_session: SparkSession, params: dict):
    if spark_session and params:
        tenant_id = params.get("tenant_id")
        prepare_input_dfs(spark_session, CSV_BASE_PATH + '1._Partners.csv', 'partner')
    return 'partner'

@task
def prepare_product_tab_input_df(spark_session: SparkSession):
    if spark_session:
        prepare_input_dfs(spark_session, CSV_BASE_PATH + '4._Product.csv', 'product')
    return 'product'

@task
def prepare_site_tab_input_df(spark_session: SparkSession):
    if spark_session:
        prepare_input_dfs(spark_session, CSV_BASE_PATH + '6._Site_List.csv', 'site')
    return 'site'


@task
def prepare_bom_input_df(spark_session: SparkSession):
    if spark_session:
        prepare_input_dfs(spark_session, CSV_BASE_PATH + '3._BOM_and_Alt_Parts.csv', 'bom')
    return 'bom'

# Add New Preparation here

# Validation Tasks:

@task
def validate_not_null_partner_name(spark_session: SparkSession):
    not_null_validator = NotNullValidator(spark_session=spark_session, temp_view='partner', columns=['partner_name'])
    not_null_validator.validate()


@task
def validate_not_null_product_name(spark_session: SparkSession):
    not_null_validator = NotNullValidator(spark_session=spark_session, temp_view='product',
                                          columns=['Product_Hierarchy_Level_1'])
    not_null_validator.validate()

@task
def validate_not_null_site_site_number(spark_session: SparkSession):
    not_null_validator = NotNullValidator(spark_session=spark_session, temp_view='site', columns=['site_number'])
    not_null_validator.validate()

@task
def validate_not_null_bom_product_name(spark_session: SparkSession):
    not_null_validator = NotNullValidator(spark_session=spark_session, temp_view='bom', columns=['Product_name'])
    not_null_validator.validate()

@task
def validate_lookup_partner_name_product_partner_name():
    PREFECT_LOGGER.info("Check JIRA for the ticket CDIF-??")

# Add New Validation Above.

@task
def merge_errors():
    PREFECT_LOGGER.info("Check JIRA CDIF-28 or something.")


@task
def write_back_errors_to_cdif_file():
    PREFECT_LOGGER.info("Atul will fill this one, he is working on it.")


@task
def node():
    PREFECT_LOGGER.info("Now validated data goes for load.")


@task
def cleanup(spark_session: SparkSession):
    PREFECT_LOGGER.info("Cleaning up temp views")
    spark_session.catalog.dropTempView('partner')
    spark_session.catalog.dropTempView('product')
    spark_session.catalog.dropTempView('site')
    PREFECT_LOGGER.info("Temp views cleaned.")


with Flow("Validation") as validation_flow:
    conf = SparkConf().setMaster('local[*]').setAppName('ProjectProvenance').set('spark.driver.memory', '12g')
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
                       ]

        node().set_dependencies(upstream_tasks=first_step, downstream_tasks=second_step)
        last_step = write_back_errors_to_cdif_file().set_dependencies(upstream_tasks=second_step)
        cleanup(spark_s).set_dependencies(upstream_tasks=[last_step])


validation_flow.run_config = LocalRun(
    env={"SOME_VAR": "VALUE"},
    working_dir=get_project_root()
)

# To see the schematic of the flow.
validation_flow.visualize()

# To run the flow as a Python program, can be used for local debugging and pre-deployment testing.
# validation_flow.run()

# To Register the flow to the project.
# PREFECT_LOGGER.info(validation_flow.register(project_name=APP_NAME))
