#!/bin/sh
#
# Copyright (c) 2018 Resilinc, Inc., All Rights Reserved.
# Any use, modification, or distribution is prohibited
# without prior written consent from Resilinc, Inc.
#

# This makes setting APP_NAME mandatory.

process_count=`ps aux | grep $APP_NAME | grep -v grep | wc -l`
if [ "$process_count" -lt 1 ]
then
    exit 1
fi
