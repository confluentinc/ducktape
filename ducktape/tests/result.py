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

import json
import time


class TestResult(object):
    """Wrapper class for a single result returned by a single test."""

    def __init__(self, test_context, success=True, summary="", data=None):
        """
        :type test_context: ducktape.tests.tests.TestContext
        :type test_name: str
        :type success: bool
        :type summary: str
        :type data: dict
        """

        self.test_context = test_context
        self.session_context = self.test_context.session_context
        self.success = success
        self.summary = summary
        self._data = None

        # For tracking run time
        self.start_time = -1
        self.stop_time = -1

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, d):
        try:
            json.dumps(d)  # Check that d is JSON-serializable
            self._data = d
        except TypeError as e:
            self.test_context.logger.error("Data returned from %s should be JSON-serializable but is not." %
                                           self.test_context.test_id)
            raise e

    @property
    def description(self):
        return self.test_context.description

    @property
    def run_time(self):
        if self.start_time < 0:
            return -1
        if self.stop_time < 0:
            return time.time() - self.start_time

        return self.stop_time - self.start_time
