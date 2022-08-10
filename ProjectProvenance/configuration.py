#
# Copyright (c) 2022 Resilinc, Inc., All Rights Reserved.
# Any use, modification, or distribution is prohibited
# without prior written consent from Resilinc, Inc.
#

# stdlib Imports.
import ast
import os

# Third Party Imports.

# Local Imports.


basedir = os.path.abspath(os.path.dirname(__file__))


def get_environment_variable_str(env_var: str, default: str = None, strip: bool = True):
    try:
        if isinstance(env_var, str):
            value = os.environ.get(env_var, default=default)
            return str(value).strip() if strip else str(value)
    except AttributeError:
        return default
    return default


def get_environment_variable_int(env_var: str, default: int = None):
    try:
        if isinstance(env_var, str):
            value = os.environ.get(env_var, default=default)
            if isinstance(value, str) and value.isdigit():
                return int(value)
            else:
                return default
    except AttributeError:
        return default
    return default


def get_env_var_as_boolean(env_var: str, default=False) -> bool:
    """
    Get an environment variable value as boolean.
    This function is already in Helper, but we are not importing it for the sake of avoiding circular imports.
    :param env_var: Environment Variable to get value for.
    :param default: Default Value if Environment Variable is not set.
    :return: bool
    """
    if env_var is None:
        return bool(default) or False
    try:
        return ast.literal_eval(os.environ.get(env_var, bool(default)).strip().capitalize())
    except ValueError:
        return False
    except SyntaxError:
        return False
    except Exception:
        return False


def get_environment_variable_as_list(env_var: str, default: str = "", strip_elements: bool = True,
                                     filter_blank: bool = False):
    env_var_value: str = get_environment_variable_str(env_var, default)
    env_var_value_list: list = [element.strip() if strip_elements else element for element in env_var_value.split(',')]
    if filter_blank:
        return list(filter(lambda x: x, env_var_value_list))
    return env_var_value_list


# Server/Application/Flask Configurations.
APP_NAME = get_environment_variable_str('APP_NAME', 'ProjectProvenance')
SERVER_SSL_ENABLE: bool = get_env_var_as_boolean('SERVER_SSL_ENABLE', False)
SERVER_PORT: str = get_environment_variable_str('SERVER_PORT', '5004')
QUART_PROFILE_APP: bool = get_env_var_as_boolean('QUART_PROFILE_APP', False)
QUART_USE_RELOADER: bool = get_env_var_as_boolean('QUART_USE_RELOADER', False)

# Postgres Configurations:
POSTGRES_USER: str = get_environment_variable_str('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD: str = get_environment_variable_str('POSTGRES_PASSWORD', 'postgres')
POSTGRES_HOST: str = get_environment_variable_str('POSTGRES_HOST', '172.16.1.236')
POSTGRES_PORT: str = get_environment_variable_str('POSTGRES_PORT', '5432')
POSTGRES_DB: str = get_environment_variable_str('POSTGRES_DB', 'resilincscrm')
DB_DRIVER: str = 'org.postgresql.Driver'
DB_URI: str = "postgresql://" + \
              POSTGRES_USER + ":" + POSTGRES_PASSWORD + "@" + POSTGRES_HOST + ":" + POSTGRES_PORT + "/" + POSTGRES_DB
JDBC_DB_URI: str = "jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_db}?" \
                   "user={postgres_user}&password={postgres_password}".format(postgres_host=POSTGRES_HOST,
                                                                              postgres_port=POSTGRES_PORT,
                                                                              postgres_db=POSTGRES_DB,
                                                                              postgres_user=POSTGRES_USER,
                                                                              postgres_password=POSTGRES_PASSWORD)

#

MONITORING_POLL_INTERVAL = get_environment_variable_int('MONITORING_POLL_INTERVAL', '3')

# ElasticSearch Configuration
ELASTICSEARCH_USER: str = get_environment_variable_str('ELASTICSEARCH_USER', 'postgres')
ELASTICSEARCH_PASSWORD: str = get_environment_variable_str('ELASTICSEARCH_PASSWORD', 'resilinc-1234')
ELASTICSEARCH_HOSTS: str = get_environment_variable_str('ELASTICSEARCH_HOSTS', '172.16.1.236')
ELASTICSEARCH_HOST_LIST: list = list(map(lambda c: c.strip(), filter(lambda x: x and isinstance(x, str),
                                                                     ELASTICSEARCH_HOSTS.strip().split(','))))

# Redis Configuration:
REDIS_HOST: str = get_environment_variable_str('REDIS_HOST', '172.16.1.236')
REDIS_PORT: str = get_environment_variable_str('REDIS_PORT', '6379')
REDIS_PASSWORD: str = get_environment_variable_str('REDIS_PASSWORD', 'redisclient')
REDIS_DB: str = get_environment_variable_str('REDIS_DB', '8')

REDIS_AUTH_DB: str = get_environment_variable_str('REDIS_AUTH_DB', '1')

REDIS_URI: str = 'redis://:' + REDIS_PASSWORD + '@' + REDIS_HOST + ':' \
                 + REDIS_PORT + '/' + REDIS_DB
REDIS_AUTH_URI: str = 'redis://:' + REDIS_PASSWORD + '@' + REDIS_HOST + ':' \
                 + REDIS_PORT + '/' + REDIS_AUTH_DB

CELERY_REDIS_BROKER: str = get_environment_variable_str('CELERY_REDIS_BROKER', '5')
CELERY_REDIS_BROKER_URI: str = 'redis://:' + REDIS_PASSWORD + '@' + REDIS_HOST + ':' \
                 + REDIS_PORT + '/' + CELERY_REDIS_BROKER

CELERY_REDIS_BACKEND: str = get_environment_variable_str('CELERY_REDIS_BACKEND', '6')
CELERY_REDIS_BACKEND_URI: str = 'redis://:' + REDIS_PASSWORD + '@' + REDIS_HOST + ':' \
                 + REDIS_PORT + '/' + CELERY_REDIS_BACKEND

EW_WEBSERVICE_URL: str = get_environment_variable_str('EW_WEBSERVICE_URL', 'https://ewservicedev.resilinc.com')
ECM_WEBSERVICE_URL: str = get_environment_variable_str('ECM_WEBSERVICE_URL', 'https://ecmdev.resilinc.com')


# App Specific Configuration:
CDIF_FILE_MAX_UPLOAD_SIZE = get_environment_variable_int('CDIF_FILE_MAX_UPLOAD_SIZE', 200 * 1024 * 1024 * 1024)


