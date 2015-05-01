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

import time


class TestResult(object):
    """Wrapper class for a single result returned by a single test."""

    def __init__(self, session_context, test_name, success=True, summary="", data=None):
        """
        :type session_context: ducktape.tests.session.SessionContext
        :type test_name: str
        :type success: bool
        :type summary: str
        :type data: dict
        """

        self.session_context = session_context
        self.test_name = test_name
        self.success = success
        self.summary = summary
        self.data = data

        # For tracking run time
        self.start_time = -1
        self.stop_time = -1

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

    def num_passed(self):
        return sum([1 for r in self if r.success])

    def num_failed(self):
        return sum([1 for r in self if not r.success])

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
            if not result.success:
                return False
        return True


