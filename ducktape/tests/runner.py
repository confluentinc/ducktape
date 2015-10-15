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


from ducktape.tests.result import TestResult
from ducktape.tests.reporter import SingleResultFileReporter
from ducktape.tests.reporter import SingleResultStdoutReporter
from ducktape.tests.result_store import create_test_datum, TestKey
from ducktape.tests.regression_test import RegressionTest

import logging
import time
import traceback


class TestRunner(object):
    """Abstract class responsible for running one or more tests."""
    def __init__(self, session_context, tests):
        self.tests = tests
        self.session_context = session_context
        # self.session_test_data = object()
        self.result_store = self.session_context.result_store

        self.logger.debug("Instantiating " + self.who_am_i())

    @property
    def cluster(self):
        return self.session_context.cluster

    @property
    def logger(self):
        return self.session_context.logger

    def who_am_i(self):
        """Human-readable name helpful for logging."""
        return self.__class__.__name__

    def run_all_tests(self):
        raise NotImplementedError()


class SerialTestRunner(TestRunner):
    """Runs tests serially."""

    # When set to True, the test runner will finish running/cleaning the current test, but it will not run any more
    stop_testing = False

    def __init__(self, *args, **kwargs):
        super(SerialTestRunner, self).__init__(*args, **kwargs)
        self.current_test = None
        self.current_test_context = None

        # Move regression tests to the end of the queue
        regression_tests = [t for t in self.tests if isinstance(t.cls, RegressionTest)]
        normal_tests = [t for t in self.tests if not isinstance(t.cls, RegressionTest)]
        self.tests = normal_tests
        self.tests.extend(regression_tests)

    def run_all_tests(self):
        self.session_context.start_time = time.time()

        self.log(logging.INFO, "starting test run with session id %s..." % self.session_context.session_id)
        self.log(logging.INFO, "running %d tests..." % len(self.tests))

        for test_num, test_context in enumerate(self.tests, 1):
            if len(self.cluster) != self.cluster.num_available_nodes():
                # Sanity check - are we leaking cluster nodes?
                raise RuntimeError(
                    "Expected all nodes to be available. Instead, %d of %d are available" %
                    (self.cluster.num_available_nodes(), len(self.cluster)))

            # Create single testable unit and corresponding test result object
            self.current_test_context = test_context

            # Instantiate test
            self.current_test = test_context.cls(test_context)
            result = TestResult(self.current_test_context)

            # Run the test unit
            result.start_time = time.time()
            self.log(logging.INFO, "test %d of %d" % (test_num, len(self.tests)))

            try:
                self.log(logging.INFO, "setting up")
                self.setup_single_test()

                self.log(logging.INFO, "running")
                result.data = self.run_single_test()
                self.log(logging.INFO, "PASS")

            except BaseException as e:
                self.log(logging.INFO, "FAIL")
                result.success = False
                result.summary += str(e.message) + "\n" + traceback.format_exc(limit=16)

                self.stop_testing = self.session_context.exit_first or isinstance(e, KeyboardInterrupt)

            finally:
                if not self.session_context.no_teardown:
                    self.log(logging.INFO, "tearing down")
                    self.teardown_single_test()

                result.stop_time = time.time()
                test_key = TestKey.from_test_context(self.current_test_context)
                datum = create_test_datum(result)
                self.result_store.put(self.session_context.session_id, test_key, datum)

                test_reporter = SingleResultFileReporter(datum, self.current_test_context.results_dir)
                test_reporter.report()
                test_reporter = SingleResultStdoutReporter(datum)
                test_reporter.report()

                self.current_test_context, self.current_test = None, None

            if self.stop_testing:
                break

        self.session_context.stop_time = time.time()

    def setup_single_test(self):
        """start services etc"""

        self.log(logging.DEBUG, "Checking if there are enough nodes...")
        if self.current_test.min_cluster_size() > len(self.cluster):
            raise RuntimeError(
                "There are not enough nodes available in the cluster to run this test. "
                "Cluster size: %d, Need at least: %d. Services currently registered: %s" %
                (len(self.cluster), self.current_test.min_cluster_size(), self.current_test_context.services))

        self.current_test.setUp()

    def run_single_test(self):
        """Run the test!

        We expect current_test_context.function to be a function or unbound method which takes an
        instantiated test object as its argument.
        """
        return self.current_test_context.function(self.current_test)

    def teardown_single_test(self):
        """teardown method which stops services, gathers log data, removes persistent state, and releases cluster nodes.

        Catch all exceptions so that every step in the teardown process is tried, but signal that the test runner
        should stop if a keyboard interrupt is caught.
        """
        exceptions = []
        if hasattr(self.current_test_context, 'services'):
            services = self.current_test_context.services
            try:
                services.stop_all()
            except BaseException as e:
                exceptions.append(e)
                self.log(logging.WARN, "Error stopping services: %s" % e.message + "\n" + traceback.format_exc(limit=16))

            try:
                self.current_test.copy_service_logs()
            except BaseException as e:
                exceptions.append(e)
                self.log(logging.WARN, "Error copying service logs: %s" % e.message + "\n" + traceback.format_exc(limit=16))

            try:
                services.clean_all()
            except BaseException as e:
                exceptions.append(e)
                self.log(logging.WARN, "Error cleaning services: %s" % e.message + "\n" + traceback.format_exc(limit=16))

        try:
            self.current_test.free_nodes()
        except BaseException as e:
            exceptions.append(e)
            self.log(logging.WARN, "Error freeing nodes: %s" % e.message + "\n" + traceback.format_exc(limit=16))

        if len([e for e in exceptions if isinstance(e, KeyboardInterrupt)]) > 0:
            # Signal no more tests if we caught a keyboard interrupt
            self.stop_testing = True

    def log(self, log_level, msg):
        """Log to the service log and the test log of the given test."""
        if self.current_test is None:
            msg = "%s: %s" % (self.who_am_i(), msg)
            self.logger.log(log_level, msg)
        else:
            msg = "%s: %s: %s" % (self.who_am_i(), self.current_test_context.test_id, msg)
            self.logger.log(log_level, msg)
            self.current_test.logger.log(log_level, msg)






