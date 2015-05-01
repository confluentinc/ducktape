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


from ducktape.tests.result import TestResult, TestResults
from ducktape.tests.test import TestContext

import time
import traceback


class TestRunner(object):
    """Abstract class responsible for running one or more tests."""
    def __init__(self, session_context, test_classes):
        self.tests = test_classes
        self.session_context = session_context
        self.results = TestResults(self.session_context)
        self.cluster = session_context.cluster
        self.logger = session_context.logger

        self.logger.debug("Instantiating " + self.who_am_i())

    def who_am_i(self):
        """Human-readable name helpful for logging."""
        return self.__class__.__name__

    def run_all_tests(self):
        raise NotImplementedError()


def create_test_case(test_class, session_context):
    """Create test context object and instantiate test class.

    :type test_class: ducktape.tests.test.Test.__class__
    :type session_context: ducktape.tests.session.SessionContext
    :rtype test_class
    """

    test_context = TestContext(session_context, test_class.__module__, test_class, test_class.run, config=None)
    return test_class(test_context)


class SerialTestRunner(TestRunner):
    """Runs tests serially."""

    def run_all_tests(self):

        self.results.start_time = time.time()
        for test in self.tests:
            # Create single testable unit and corresponding test result object
            test_case = create_test_case(test, self.session_context)
            result = TestResult(self.session_context, test_case.who_am_i())

            # Run the test unit
            try:
                result.start_time = time.time()
                result.data = self.run_single_test(test_case)
            except BaseException as e:
                result.success = False
                result.summary += e.message + "\n" + traceback.format_exc(limit=16)

                if self.session_context.exit_first or isinstance(e, KeyboardInterrupt):
                    # Don't run any more tests
                    break
            finally:
                result.stop_time = time.time()
                self.results.append(result)

        self.results.stop_time = time.time()
        return self.results

    def run_single_test(self, test):
        """Setup, run, and tear down one testable unit."""

        self.logger.debug("Checking if there are enough nodes...")
        if test.min_cluster_size() > self.cluster.num_available_nodes():
            raise RuntimeError(
                "There are not enough nodes available in the cluster to run this test. Needed: %d, Available: %d" %
                (test.min_cluster_size(), self.cluster.num_available_nodes()))

        try:
            # start services etc
            self.logger.info("%s: %s: setting up" % (self.who_am_i(), test.who_am_i()))
            test.setUp()

            # run the test!
            self.logger.info("%s: %s: running" % (self.who_am_i(), test.who_am_i()))
            data = test.run()
            self.logger.info("%s: %s: PASS" % (self.who_am_i(), test.who_am_i()))
        except BaseException as e:
            self.logger.info("%s: %s: FAIL" % (self.who_am_i(), test.who_am_i()))
            raise
        finally:
            # clean up no matter what the exception is
            self.logger.info("%s: %s: tearing down" % (self.who_am_i(), test.who_am_i()))
            test.tearDown()
            test.free_nodes()

        return data



