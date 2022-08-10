#
# Copyright (c) 2022 Resilinc, Inc., All Rights Reserved
#   Any use, modification, or distribution is prohibited
#   without prior written consent from Resilinc, Inc.
#

# stdlib imports
import time
from typing import Callable

# Third Party Imports.

# Local Imports.
from auth import AuthUser, AuthUtils, AuthConstants
from quart import request, g, current_app, jsonify
from Log import log_global

# This is Flask Specific Implementation for WAVZ Authentication.
# Import this file with app_context in flask projects __init__ file.

LOGGER = log_global()


@current_app.before_request
def authenticate_request() -> tuple:

    """
    Authenticate Every Request before processing. Abort with status 401, if authorization header missing or not present
    in Redis Session store. This also sets the AuthUser object in flask's g object in request's context.
    g is cleared at the end of request and is request specific as depicted here:
    https://stackoverflow.com/questions/19098295/flask-setting-application-and-request-specific-attributes

    :return: tuple
    """
    LOGGER.info("Authorizing User.")
    if request.endpoint in current_app.view_functions and request.endpoint != AuthConstants.HEALTHCHECK_ENDPOINT.value \
            and request.method != 'OPTIONS' and not hasattr(g, AuthConstants.LOGGED_IN_USER.value):
        view_func = current_app.view_functions[request.endpoint]
        if not hasattr(view_func, AuthConstants.EXCLUDE_FROM_AUTHENTICATION.value):
            access_token = request.headers.get(AuthConstants.AUTHORIZATION.value)
            if access_token:
                # Now get Authentication Details from redis.
                user: AuthUser = AuthUtils.get_user_info_from_redis(access_token)
                if user and user.user_anchor_id and user.tenant_id and user.current_organization:
                    setattr(g, AuthConstants.LOGGED_IN_USER.value, user)
                else:
                    LOGGER.info('Authorization Token: {} is Not Valid. User is accessing: {}'.format(access_token,
                                                                                                     request.path))
                    return jsonify({'timestamp': int(time.time()), 'status': 401, 'error': 'Unauthorized',
                                    'message': 'Authorization Token is Not Valid', 'path': request.path}), 401
            else:
                LOGGER.info('Access Token not provided in Request. User is accessing: {}'.format(
                    request.path))
                return jsonify({'timestamp': int(time.time()), 'status': 401, 'error': 'Unauthorized',
                                'message': 'Authorization Token Not Found', 'path': request.path}), 401
        else:
            LOGGER.warning('WARNING: Authentication skipped for Endpoint: {0} | Path: {1} '
                           '| Remote Addr: {2}'.format(request.endpoint, request.path, request.remote_addr))


def exclude_authentication(func: Callable) -> Callable:
    """
    WARNING: Use with utmost caution.
    Decorator for Excluding an Endpoint from getting authenticated. If excluded from authentication,
    logged in AuthUser object is not set in g before request (Automatically).
    :param func: View Function to exclude from authenticating.
    :type func: Callable
    :return: Callable
    """
    if isinstance(func, Callable):
        setattr(func, AuthConstants.EXCLUDE_FROM_AUTHENTICATION.value, True)
    return func

