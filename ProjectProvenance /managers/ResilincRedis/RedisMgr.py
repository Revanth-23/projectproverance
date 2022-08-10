#
# Copyright (c) 2019 Resilinc, Inc., All Rights Reserved.
#   Any use, modification, or distribution is prohibited
#   without prior written consent from Resilinc, Inc.
#

# stdlib Imports.

# Third Party Imports.
from flask import g, current_app

# Local Imports.
from managers.ResilincRedis.redis_client import RedisClient


# These functions are flask framework specific.


def redis_connection():
    if 'redis_client' not in g:
        g.redis_client = RedisClient.from_url(current_app.config.get('REDIS_AUTH_URI', ''), charset='utf-8',
                                              decode_responses=True)
    return g.redis_client


def redis_close(e=None):
    redis_client = g.pop('redis_client', None)

    if redis_client is not None:
        del redis_client


def init_app(app):
    app.teardown_appcontext(redis_close)
