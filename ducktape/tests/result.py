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


class TestStatus(object):
    def __init__(self, status):
        self._status = str(status).lower()

    def __eq__(self, other):
        return str(self).lower() == str(other).lower()

    def __str__(self):
        return self._status

PASS = TestStatus("pass")
FAIL = TestStatus("fail")
IGNORE = TestStatus("ignore")


class TestResult(object):
    """Wrapper class for a single result returned by a single test."""

    def __init__(self, test_context, test_status=PASS, summary="", data=None, start_time=-1, stop_time=-1):
        """
        @param test_context  standard test context object
        @param test_status   did the test pass or fail, etc?
        @param summary       summary information
        @param data          data returned by the test, e.g. throughput
        """

        self.test_context = test_context
        self.session_context = self.test_context.session_context
        self.test_status = test_status
        self.summary = summary
        self._data = data

        # For tracking run time
        self.start_time = start_time
        self.stop_time = stop_time

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
                                           self.test_context.test_name)
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


class TestResults(list):
    """Class used to aggregate individual TestResult objects from many tests."""
    # TODO make this tread safe - once tests are run in parallel, this will be shared by multiple threads

    def __init__(self, session_context):
        """
        :type session_context: ducktape.tests.session.SessionContext
        """
        super(list, self).__init__()

        self.session_context = session_context

        # For tracking total run time
        self.start_time = -1
        self.stop_time = -1

    @property
    def num_passed(self):
        return len([r for r in self if r.test_status == PASS])

    @property
    def num_failed(self):
        return len([r for r in self if r.test_status == FAIL])

    @property
    def num_ignored(self):
        return len([r for r in self if r.test_status == IGNORE])

    @property
    def run_time(self):
        if self.start_time < 0:
            return -1
        if self.stop_time < 0:
            return time.time() - self.start_time

        return self.stop_time - self.start_time

    def get_aggregate_success(self):
        """Check cumulative success of all tests run so far
        :rtype: bool
        """
        for result in self:
            if result.test_status == FAIL:
                return False
        return True


