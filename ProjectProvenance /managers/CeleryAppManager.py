#
# Copyright (c) 2022 Resilinc, Inc., All Rights Reserved
#   Any use, modification, or distribution is prohibited
#   without prior written consent from Resilinc, Inc.
#

from celery import Celery
from configuration import CELERY_REDIS_BACKEND_URI, CELERY_REDIS_BROKER_URI

celery_app = Celery(
    "celery_app",
    broker=CELERY_REDIS_BROKER_URI,
    backend=CELERY_REDIS_BACKEND_URI,
    include=["celery_tasks.cdif_tasks"],
)
