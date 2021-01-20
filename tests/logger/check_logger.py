# Copyright 2016 Confluent Inc.
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
import psutil
import shutil
import tempfile

from ducktape.tests.loggermaker import LoggerMaker, close_logger


class DummyFileLoggerMaker(LoggerMaker):
    def __init__(self, log_dir, n_handles):
        """Create a logger with n_handles file handles, with files in log_dir"""
        self.log_dir = log_dir
        self.n_handles = n_handles

    @property
    def logger_name(self):
        return "a.b.c"

    def configure_logger(self):
        for i in range(self.n_handles):
            fh = logging.FileHandler(os.path.join(self.log_dir, "log-" + str(i)))
            self._logger.addHandler(fh)


def open_files():
    # current process
    p = psutil.Process()
    return p.open_files()


class CheckLogger(object):
    def setup_method(self, _):
        self.temp_dir = tempfile.mkdtemp()

    def check_close_logger(self):
        """Check that calling close_logger properly cleans up resources."""
        initial_open_files = open_files()

        n_handles = 100
        log_maker = DummyFileLoggerMaker(self.temp_dir, n_handles)
        # accessing logger attribute lazily triggers configuration of logger
        the_logger = log_maker.logger

        assert len(open_files()) == len(initial_open_files) + n_handles
        close_logger(the_logger)
        assert len(open_files()) == len(initial_open_files)

    def teardown_method(self, _):
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
