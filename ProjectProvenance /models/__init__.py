#
# Copyright (c) 2022 Resilinc, Inc., All Rights Reserved
#   Any use, modification, or distribution is prohibited
#   without prior written consent from Resilinc, Inc.
#

from validator.expression_engine import Validation, NotNullValidation
from Log import log_global

LOGGER = log_global()


class DAG:
    def __init__(self, dag_id: str, tasks: list):
        self.dag_id = dag_id
        self.tasks = tasks

    def execute(self):
        pass


class Task:
    def __init__(self, task_id: str, depends_on: str):
        self.task_id: str = task_id
        self.depends_on: str = depends_on


    def process(self):
        pass

    @classmethod
    def create_task_with_task_definition(cls, task_definition: dict):
        pass


class Operation:
    def __init__(self, op_id: str, op_type: str):
        self.op_id: str = op_id
        self.op_type: str = op_type

    @classmethod
    def parse_operation(cls, ):
        pass


class ValidationDefinition:
    def __init__(self, validation_id: str, validation: Validation):
        self.validation_id: str = validation_id
        self.validation = validation

    @classmethod
    def parse_definition(cls, validation_definition: dict):
        if validation_definition and isinstance(validation_definition, dict):
            definition = validation_definition.get('definition')
            validation_id = validation_definition.get('id', '')
            if not validation_id:
                LOGGER.error("TODO: raise error?")
            if definition and isinstance(definition, dict):
                validation_type = definition.get('type')
                if validation_type == 'not_null':

                    columns_to_check = validation_definition.get('columns_to_check') or list()
                    view_to_check = validation_definition.get('view_to_check')
                    not_null_validation = NotNullValidation(columns_to_check, view_to_check, None)
                    return cls(validation_id, not_null_validation)
                elif validation_type == 'lookup':
                    LOGGER.info("Implement Lookup")

            else:
                LOGGER.error("TODO: error or default validation type.")


class TransformerDefinition:
    pass
