
#
# Copyright (c) 2022 Resilinc, Inc., All Rights Reserved.
#   Any use, modification, or distribution is prohibited
#   without prior written consent from Resilinc, Inc.
#

import inspect
import logging
import logging.handlers
import os
import platform
import re
import socket
import sys
import time
from datetime import datetime


class Log:
    LOG_FILE_EXTENSION = '.log'
    backup_log_pattern = re.compile(r'\.log\.\w$')

    def __init__(self, name: str = None, default_level: str = 'info', startup_message: str = None):
        fqdn = socket.getfqdn()
        
        self.name = name or inspect.currentframe().f_back.f_code.co_name
        self._initial_data = ('------------------- {} ----------------------'.format(startup_message),
                              'TIMEZONE: (offset=>{0}, dst=>{1}, zone=>{2})'.format(-(time.timezone/3600),
                                                                                    time.daylight, time.tzname),
                              # Setting hostname to 'hostname' if the platform is not Linux, as this doesn't work
                              # correctly on Mac.
                              'ENDPOINT: (ip=>{0}, name=>{1})'.format(
                                  socket.gethostbyname(fqdn) if platform.system() == 'Linux' else 'hostname', fqdn))
        self.logger = logging.getLogger(self.name)
        self.handlers = {'stdout': {'obj': None, 'set': False},
                         'stderr': {'obj': None, 'set': False},
                         'file': {'obj': None, 'set': False}}
        self.fmt = logging.Formatter('%(name)s:%(levelname)s:%(created)f (%(module)s:%(lineno)d:'
                                     '%(threadName)s) %(msg)s', datefmt='%Y%m%d_%H.%M.%S')
        if default_level:
            self.logger.setLevel(eval('logging.' + default_level.upper()))

        # These are aliases (user calls Log.Global.info(...), and it goes directly to the logger obj)
        self.critical = self.logger.critical
        self.error = self.logger.error
        self.warning = self.logger.warning
        self.info = self.logger.info
        self.debug = self.logger.debug
        self.log = self.logger.log
        self.warn = self.warning

        # Finally, Add the default handler
        self.activate_stdout_handler()

    def deactivate_stdout_handler(self):
        _dict = self.handlers['stdout']
        if _dict['set'] is True:
            self.logger.removeHandler(_dict['obj'])
            _dict['set'] = False

    def activate_stderr_handler(self):
        _dict = self.handlers['stderr']
        if _dict['obj'] is None:
            logging_stream_handler = logging.StreamHandler(stream=sys.stderr)
            logging_stream_handler.setLevel(logging.ERROR)
            logging_stream_handler.setFormatter(self.fmt)
            self.logger.addHandler(logging_stream_handler)
            _dict['obj'] = logging_stream_handler
            _dict['set'] = True
        elif _dict['set'] is False:
            self.logger.addHandler(_dict['obj'])
            _dict['set'] = True

    def activate_stdout_handler(self):
        _dict = self.handlers['stdout']
        if _dict['obj'] is None:
            logging_stream_handler = logging.StreamHandler(stream=sys.stdout)
            logging_stream_handler.setFormatter(self.fmt)
            self.logger.addHandler(logging_stream_handler)
            _dict['obj'] = logging_stream_handler
            _dict['set'] = True
            for line in self._initial_data:
                self.log(21, line)
        elif _dict['set'] is False:
            self.logger.addHandler(_dict['obj'])
            _dict['set'] = True

    def activate_file_handler(self, path=None, file_name='out.log', size=0, backup=0, deactivate_stdout=False,
                              log_rotation_type: str = 'timed', log_rotation_timed_interval_type='D',
                              log_rotation_timed_interval=1):
        _dict = self.handlers['file']
        if not path or not isinstance(file_name, str):
            path = './'

        if not file_name or not isinstance(file_name, str):
            file_name = 'out.log'

        if _dict['obj'] is None:
            size *= 1000000
            try:
                os.makedirs(os.path.abspath(path), mode=0o777, exist_ok=True)
                file_name = os.path.join(path, file_name)
                self.info('Redirecting Log output to ' + file_name)
                if log_rotation_type.lower() == 'timed':
                    logging_rotating_handler = logging.handlers.TimedRotatingFileHandler(
                        filename=file_name,
                        when=log_rotation_timed_interval_type,
                        interval=log_rotation_timed_interval, backupCount=backup)
                else:
                    logging_rotating_handler = logging.handlers.RotatingFileHandler(filename=file_name,
                                                                                    maxBytes=int(size),
                                                                                    backupCount=backup)
                logging_rotating_handler.namer = Log._rotating_file_handler_name
                logging_rotating_handler.setFormatter(self.fmt)

                self.logger.addHandler(logging_rotating_handler)
                if deactivate_stdout:
                    self.deactivate_stdout_handler()
                _dict['obj'] = logging_rotating_handler
                _dict['set'] = True
                for line in self._initial_data:
                    self.log(21, line)
            except PermissionError:
                self.logger.error('Could Not Create File because permission was denied, '
                                  'continuing with STDOUT Logging.')
                if deactivate_stdout:
                    self.logger.info('Reactivating STDOUT LOGGING.')
                    self.activate_file_handler()
        elif _dict['set'] is False:
            self.logger.addHandler(_dict['obj'])
            _dict['set'] = True
            self.activate_stdout_handler()

    def get_all_effective_handler(self):
        return self.logger.handlers or []

    @staticmethod
    def _rotating_file_handler_name(name):
        if name and isinstance(name, str):
            if Log.backup_log_pattern.search(name):
                return Log.backup_log_pattern.sub(datetime.now().strftime('-%d%m%Y-%H%M%S'), name) \
                       + Log.LOG_FILE_EXTENSION
            else:
                return name + datetime.now().strftime('-%d%m%Y-%H%M%S') + Log.LOG_FILE_EXTENSION
        return name

    def _may_level(self, check):
        lvl = self.logger.getEffectiveLevel()
        return (lvl > 0) and (lvl <= check)

    def may_info(self):
        return self._may_level(logging.INFO)

    def may_debug(self):
        return self._may_level(logging.DEBUG)

    def may_warning(self):
        return self._may_level(logging.WARNING)

    def may_error(self):
        return self._may_level(logging.ERROR)

    def may_critical(self):
        return self._may_level(logging.CRITICAL)
