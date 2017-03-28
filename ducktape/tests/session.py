# Copyright 2015 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os
import sys
import time

from ducktape.tests.loggermaker import LoggerMaker
from ducktape.command_line.defaults import ConsoleDefaults


class SessionContext(object):
    """Wrapper class for 'global' variables. A call to ducktape generates a single shared SessionContext object
    which helps route logging and reporting, etc.
    """

    def __init__(self, **kwargs):
        # session_id, results_dir, cluster, globals):
        self.session_id = kwargs["session_id"]
        self.results_dir = os.path.abspath(kwargs["results_dir"])

        self.debug = kwargs.get("debug", False)
        self.compress = kwargs.get("compress", False)
        self.exit_first = kwargs.get("exit_first", False)
        self.no_teardown = kwargs.get("no_teardown", False)
        self.max_parallel = kwargs.get("max_parallel", 1)
        self.default_expected_num_nodes = kwargs.get("default_num_nodes", None)
        self._globals = kwargs.get("globals")

    @property
    def globals(self):
        """None, or an immutable dictionary containing user-defined global variables."""
        return self._globals

    def to_json(self):
        return self.__dict__


class SessionLoggerMaker(LoggerMaker):
    def __init__(self, session_context):
        super(SessionLoggerMaker, self).__init__(session_context.session_id + ".session_logger")
        self.log_dir = session_context.results_dir
        self.debug = session_context.debug

    def configure_logger(self):
        """Set up the logger to log to stdout and files. This creates a few files as a side-effect. """
        if self.configured:
            return

        self._logger.setLevel(logging.DEBUG)

        fh_info = logging.FileHandler(os.path.join(self.log_dir, "session_log.info"))
        fh_debug = logging.FileHandler(os.path.join(self.log_dir, "session_log.debug"))
        fh_info.setLevel(logging.INFO)
        fh_debug.setLevel(logging.DEBUG)

        # create console handler with a higher log level
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.DEBUG if self.debug else logging.INFO)

        # create formatter and add it to the handlers
        formatter = logging.Formatter(ConsoleDefaults.SESSION_LOG_FORMATTER)
        fh_info.setFormatter(formatter)
        fh_debug.setFormatter(formatter)
        ch.setFormatter(formatter)

        # add the handlers to the logger
        self._logger.addHandler(fh_info)
        self._logger.addHandler(fh_debug)
        self._logger.addHandler(ch)


def generate_session_id(session_id_file):
    """Generate a new session id based on the previous session id found in session_id_file
    :type session_id_file: str  Last-used session_id is in this file
    :rtype str                  New session_id
    """

    def get_id(day, num):
        return day + "--%03d" % num

    def split_id(an_id):
        day = an_id[:10]
        num = int(an_id[12:])
        return day, num

    def today():
        return time.strftime("%Y-%m-%d")

    def next_id(prev_id):
        if prev_id is None:
            prev_day = today()
            prev_num = 0
        else:
            prev_day, prev_num = split_id(prev_id)

        if prev_day == today():
            next_day = prev_day
            next_num = prev_num + 1
        else:
            next_day = today()
            next_num = 1

        return get_id(next_day, next_num)

    if os.path.isfile(session_id_file):
        with open(session_id_file, "r") as fp:
            session_id = next_id(fp.read())
    else:
        session_id = next_id(None)

    with open(session_id_file, "w") as fp:
        fp.write(session_id)

    return session_id


def generate_results_dir(results_root, session_id):
    """Results from a single run of ducktape are assigned a session_id and put together in this directory.

    :type session_id: str
    :rtype: str
    """
    return os.path.join(os.path.abspath(results_root), session_id)
