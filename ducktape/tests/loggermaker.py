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


class LoggerMaker(object):
    """This class helps ensure programmatically configured loggers are configured only once."""

    def __init__(self, logger_name):
        self.logger_name = logger_name

    @property
    def logger(self):
        """Read-only logger attribute."""
        if not hasattr(self, '_logger'):
            self._logger = logging.getLogger(self.logger_name)

        if not self.configured:
            self.configure_logger()

        return self._logger

    @property
    def configured(self):
        """Return True iff the logger has been configured.

        Since logging objects are global in the sense that logging.getLogger(self.logger_name) yields the same
        object, we assume it is already configured if it already has at least 1 handler.
        """
        return len(logging.getLogger(self.logger_name).handlers) > 0

    def configure_logger(self):
        raise NotImplementedError("configure_logger property must be implemented by a subclass")


def close_logger(logger):
    """Filehandles etc are not closed automatically, so close them here"""
    if logger is not None:
        handlers = logger.handlers[:]
        for handler in handlers:
            handler.close()
            logger.removeHandler(handler)
