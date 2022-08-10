
#
# Copyright (c) 2022 Resilinc, Inc., All Rights Reserved.
#   Any use, modification, or distribution is prohibited
#   without prior written consent from Resilinc, Inc.
#

import ast
import os
from Log.Log import Log


def get_env_var_as_boolean(env_var: str, default=False) -> bool:
    """
    Get an environment variable value as boolean.
    This function is already in Helper, but we are not importing it for the sake of avoiding circular imports.
    :param: env_var: Environment Variable to get value for.
    :param: default: Default Value if Environment Variable is not set.
    :return: bool
    """
    if env_var is None:
        return bool(default) or False
    try:
        return ast.literal_eval(os.environ.get(env_var, str(bool(default))).strip().capitalize())
    except ValueError:
        return False
    except SyntaxError:
        return False
    except Exception as e:
        print(e)
        return False


_LOGGER = None

host = os.environ.get('HOST')
port = os.environ.get('PORT')

LOG_LEVEL = os.environ.get('RESILINC_PYTHON_LOG_LEVEL', 'info')
LOGGER_NAME = os.environ.get('RESILINC_PYTHON_LOGGER_NAME', 'log_global')
if not LOG_LEVEL or LOG_LEVEL not in ('critical', 'error', 'warning', 'info', 'debug', 'log', 'warning'):
    LOG_LEVEL = 'info'

LOG_ACTIVATE_FILE_HANDLER: bool = get_env_var_as_boolean('RESILINC_PYTHON_LOG_ACTIVATE_FILE_HANDLER', True)
LOG_FILE_DIRECTORY: str = os.environ.get('RESILINC_PYTHON_LOG_FILE_PATH', '/var/tmp/logs')
LOG_FILE_NAME_STR: str = os.environ.get('RESILINC_PYTHON_LOG_FILE_NAME', 'caped_event_creator')
LOG_FILE_SUFFIX: str = '_' + (host if host else 'host') + ':' + (port if port else 'port')

LOG_FILE_NAME: str = LOG_FILE_NAME_STR + LOG_FILE_SUFFIX + '.log'

try:
    LOG_NUMBER_OF_BACKUPS = int(os.environ.get('RESILINC_PYTHON_LOG_BACKUP_NUMBERS', '4'))
except:
    LOG_NUMBER_OF_BACKUPS = 4
try:
    LOG_FILE_SIZE_MB = int(os.environ.get('RESILINC_PYTHON_LOG_FILE_SIZE_MB', '5'))
except:
    LOG_FILE_SIZE_MB = 50

LOG_ROTATION_TYPE: str = os.environ.get('RESILINC_PYTHON_LOG_ROTATION_TYPE', 'timed')
LOG_ROTATION_TIMED_INTERVAL_TYPE: str = os.environ.get('RESILINC_PYTHON_TIMED_LOG_ROTATION_INTERVAL_TYPE', 'D') or 'D'
if LOG_ROTATION_TIMED_INTERVAL_TYPE and LOG_ROTATION_TIMED_INTERVAL_TYPE.lower() == 'midnight':
    LOG_ROTATION_TIMED_INTERVAL_TYPE = LOG_ROTATION_TIMED_INTERVAL_TYPE.lower()
elif LOG_ROTATION_TIMED_INTERVAL_TYPE not in ('S', 'M', 'H', 'D', 'midnight', 'W0', 'W1', 'W2', 'W3', 'W4', 'W5', 'W6'):
    LOG_ROTATION_TIMED_INTERVAL_TYPE = 'D'
else:
    LOG_ROTATION_TIMED_INTERVAL_TYPE = LOG_ROTATION_TIMED_INTERVAL_TYPE.upper()

try:
    LOG_ROTATION_TIMED_INTERVAL: int = int(os.environ.get('RESILINC_PYTHON_TIMED_LOG_ROTATION_INTERVAL', 1))
except TypeError:
    LOG_ROTATION_TIMED_INTERVAL = 1 if LOG_ROTATION_TIMED_INTERVAL_TYPE != 'S' else 86400

LOG_STARTUP_TEXT: str = os.environ.get('LOG_STARTUP_TEXT', "Data is the new oil, let's get some in...")


def log_global(**kwargs) -> Log:
    global _LOGGER
    if _LOGGER:
        return _LOGGER
    name = kwargs.get('name', LOGGER_NAME)
    default_level = kwargs.get('default_level', LOG_LEVEL)
    log_file_dir = kwargs.get('log_file_directory') or LOG_FILE_DIRECTORY
    log_file_name = kwargs.get('log_file_name') or LOG_FILE_NAME
    log_file_size = kwargs.get('log_file_size') or LOG_FILE_SIZE_MB
    log_number_of_backups = kwargs.get('number_of_backups') or LOG_NUMBER_OF_BACKUPS
    log_rotation_type = kwargs.get('log_rotation_type') or LOG_ROTATION_TYPE
    log_rotation_timed_interval_type = \
        kwargs.get('log_rotation_timed_interval_type') or LOG_ROTATION_TIMED_INTERVAL_TYPE
    log_rotation_timed_interval = kwargs.get('log_rotation_timed_interval') or LOG_ROTATION_TIMED_INTERVAL

    _LOGGER = Log(name=name, default_level=default_level, startup_message=LOG_STARTUP_TEXT)
    if LOG_ACTIVATE_FILE_HANDLER:
        _LOGGER.activate_file_handler(path=log_file_dir, file_name=log_file_name, size=log_file_size,
                                      backup=log_number_of_backups, log_rotation_type=log_rotation_type,
                                      log_rotation_timed_interval_type=log_rotation_timed_interval_type,
                                      log_rotation_timed_interval=log_rotation_timed_interval)
    return _LOGGER
