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


class Logger(object):
    @property
    def logger_name(self):
        raise NotImplementedError("logger_name property must be implemented by a subclass")

    @property
    def logger(self):
        """Read-only logger attribute."""
        if not hasattr(self, '_logger'):
            self._logger = logging.getLogger(self.logger_name)

        if not hasattr(self, "_logger_configured"):
            self.__dict__["_logger_configured"] = False

        if not self._logger_configured:
            self.configure_logger()
            self._logger_configured = True

        return self._logger

    def configure_logger(self):
        raise NotImplementedError("configure_logger property must be implemented by a subclass")