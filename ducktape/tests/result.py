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

from json import JSONEncoder
import os
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

    def __init__(self,
                 test_context,
                 session_context,
                 test_status=PASS,
                 summary="",
                 data=None,
                 start_time=-1,
                 stop_time=-1):
        """
        @param test_context  standard test context object
        @param test_status   did the test pass or fail, etc?
        @param summary       summary information
        @param data          data returned by the test, e.g. throughput
        """
        self.test_id = test_context.test_id
        self.module_name = test_context.module_name
        self.cls_name = test_context.cls_name
        self.function_name = test_context.function_name
        self.injected_args = test_context.injected_args
        self.description = test_context.description

        self.session_context = session_context
        self.test_status = test_status
        self.summary = summary
        self.data = data

        self.base_results_dir = session_context.results_dir
        self.results_dir = test_context.results_dir
        if not self.results_dir.endswith(os.path.sep):
            self.results_dir += os.path.sep
        if not self.base_results_dir.endswith(os.path.sep):
            self.base_results_dir += os.path.sep
        assert self.results_dir.startswith(self.base_results_dir)
        self.relative_results_dir = self.results_dir[len(self.base_results_dir):]

        # For tracking run time
        self.start_time = start_time
        self.stop_time = stop_time

    def __repr__(self):
        return "<%s - test_status:%s, data:%s>" % (self.__class__.__name__, self.test_status, str(self.data))

    @property
    def run_time(self):
        if self.start_time < 0:
            return -1
        if self.stop_time < 0:
            return time.time() - self.start_time

        return self.stop_time - self.start_time


class JSONResultEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, TestResult):
            return {
                "test_id": obj.test_id,
                "module_name": obj.module_name,
                "cls_name": obj.cls_name,
                "function_name": obj.function_name,
                "injected_args": obj.injected_args,
                "description": obj.description,
                "results_dir": obj.results_dir,
                "relative_results_dir": obj.relative_results_dir,
                "base_results_dir": obj.base_results_dir,
                "test_status": obj.test_status,
                "summary": obj.summary,
                "data": obj.data,
                "start_time": obj.start_time,
                "stop_time": obj.stop_time,
                "run_time": obj.run_time
            }
        elif isinstance(obj, TestStatus):
            return str(obj).upper()
        else:
            # Let the base class default method raise the TypeError
            return JSONEncoder.default(self, obj)


class TestResults(list):
    """Class used to aggregate individual TestResult objects from many tests."""
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


