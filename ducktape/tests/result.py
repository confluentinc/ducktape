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


class TestResult(object):
    """Wrapper class for a single result returned by a single test."""

    def __init__(self, session_context, test_name, success=True, summary="", data=None):
        """
        :type session_context: ducktape.tests.session_context.TestSessionContext
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


class TestResults(object):
    """Class used to aggregate individual TestResult objects from many tests."""
    # TODO make this tread safe - once tests are run in parallel, this will be shared by multiple threads

    def __init__(self, session_context):
        """
        :type session_context: ducktape.tests.session_context.TestSessionContext
        """
        self.session_context = session_context

        # Mapping from test_name -> test_result
        self.results_map = {}

        # maintains an ordering of test_results
        self.results_list = []

        # Aggregate success of all results
        self.success = True

    def add_result(self, test_result):
        """Add a TestResult to this collection.
        :type test_result: TestResult
        """
        assert test_result.__class__ == TestResult
        self.results_map[test_result.test_name] = test_result
        self.results_list.append(test_result)
        self.success = self.success and test_result.success

    def get_result(self, test_name):
        """Get the TestResult associated with given test_name
        :type test_name: str
        :rtype: TestResult
        """
        return self.results_map.get(test_name)

    def get_aggregate_success(self):
        """Check cumulative success of all tests run so far
        :rtype: bool
        """
        if not self.success:
            return False

        for result in self:
            if not result.success:
                return False

        return True

    def __iter__(self):
        for item in self.results_list:
            yield item

