import time
import uuid

from prefect import task, Flow, Parameter, Client
from prefect.utilities.logging import prefect_logger
from quart import Quart, request, g, jsonify

from Log import log_global

PREFECT_LOGGER = prefect_logger
LOGGER = log_global()
app = Quart(__name__)
prefect_client = Client()


@task
def validation(tenant_id):
    PREFECT_LOGGER.info("Running validation for tenant: {}".format(tenant_id))
    time.sleep(20)
    PREFECT_LOGGER.info("Completed validation for tenant: {}".format(tenant_id))


with Flow("Validation") as validation_flow:
    tenant_id_param = Parameter('tenant_id')
    validation(tenant_id=tenant_id_param)

# We are registering the flow globally because we don't want to register the flow with every request.
# We need to check this further.
flow_id = validation_flow.register(project_name="ProjectProvenance")
LOGGER.info("Flow id is : {}".format(flow_id))


@app.route("/validate/<tenant_id>", methods=["GET", "POST"])
async def run_validation(tenant_id):
    if flow_id:
        if request.method == "GET":
            # Return validation status for the tenant.
            return {"status": "Validation Status."}
            pass

        if request.method == "POST":
            # Check if validation is already running for this tenant. Write a utils method.

            PREFECT_LOGGER.info("Starting validation for {}".format(tenant_id))
            prefect_client.create_flow_run(flow_id, parameters={'tenant_id': tenant_id},
                                           run_name=str(tenant_id) + '_' + str(time.time()) + '_' + str(uuid.uuid4()))
            return jsonify({"status": "Your validation request has been submitted, you'll get the notification"}), 202
    else:
        PREFECT_LOGGER.error("Flow has not been registered.")
        LOGGER.error("Flow has not been registered.")
        return jsonify({"status": "Validation can not be started right now."}), 500


if __name__ == '__main__':
    app.run()
