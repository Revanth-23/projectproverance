#
# Copyright (c) 2022 Resilinc, Inc., All Rights Reserved
#   Any use, modification, or distribution is prohibited
#   without prior written consent from Resilinc, Inc.
#

from quart import Quart, request, g
import os, asyncpg, sys, asyncio, uuid, shutil
import asyncio
import os
import sys

import asyncpg
from quart import Quart, request, g

from Log import log_global
from auth import AuthUser
from configuration import CDIF_FILE_MAX_UPLOAD_SIZE

parent_dir = os.path.abspath(os.pardir)
sys.path.insert(0, parent_dir)
cdif_dir = os.path.join(parent_dir, "cdif_files")
if not os.path.exists(cdif_dir):
    os.makedirs(cdif_dir)

from celery_tasks.cdif_tasks import process_excel_sheet
from celery import group

LOGGER = log_global()

from celery_tasks.cdif_data import sheet_names


class DataBase:
    # Create and return connect coroutine object
    async def connection(
        self,
        db_uri: str = None,
        db_port: str = None,
        db_host: str = None,
        db_name: str = None,
        db_username: str = None,
        db_password: str = None,
    ):
        if db_uri is not None:
            return await asyncpg.connect(db_uri)
        return await asyncpg.connect(
            port=db_port,
            host=db_host,
            database=db_name,
            user=db_username,
            password=db_password,
        )

    # Method to fetch data from the database for the query
    async def fetch_data(self, query, *args):
        db_data = await self.conn.fetch(query, *args)
        return db_data


app = Quart(__name__)


async def initialize():
    # This Function is to async-init all the things that we may need beforehand. Like DB Connections, Redis Connection.
    # This is also used to push somecode into Quart App Context.
    # with app.app_context() doesn't work without async.

    async with app.app_context():
        from auth import flask_auth
        pass
# TODO: this is for max content length at the application level, for individual file upload, check other options.

app.config["MAX_CONTENT_LENGTH"] = CDIF_FILE_MAX_UPLOAD_SIZE


@app.route("/upload", methods=["POST"])
async def upload_file():
    user: AuthUser = g.logged_in_user
    files = await request.files

    cdif_file = files.get("cdif_file", None)
    if not cdif_file:
        return {"server": "File not found"}, 400
    user_cdif_dir = os.path.join(cdif_dir, str(user.user_anchor_id))
    if os.path.exists(user_cdif_dir):
        for files in os.listdir(user_cdif_dir):
            file_path = os.path.join(user_cdif_dir, files)
            try:
                shutil.rmtree(file_path)
            except OSError:
                os.remove(file_path)
    else:
        os.makedirs(user_cdif_dir)
    await cdif_file.save(
        os.path.join(user_cdif_dir, f"{str(user.user_anchor_id)}.xlsx")
    )
    group_task_id = str(uuid.uuid4())
    celery_group_task = group(
        process_excel_sheet.s(
            user_id=str(user.user_anchor_id),
            sheet_name=sheet_name,
            group_task_id=group_task_id,
        )
        for sheet_name in sheet_names
    )
    group_result = celery_group_task.apply_async(task_id=group_task_id)
    group_result.save()

    return {"server": "File uploaded"}, 200


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(initialize())
    app.run(loop=loop, port=5001)

