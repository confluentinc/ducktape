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

import traceback


class TestRunner(object):
    def __init__(self, session_context, test_classes, cluster):
        self.tests = test_classes
        self.cluster = cluster
        self.session_context = session_context
        self.results = TestResults(self.session_context)
        self.logger = session_context.logger

        self.logger.debug("Instantiating " + self.__class__.__name__)

    def run_all_tests(self):
        raise NotImplementedError()


def get_test_case(test_class, session_context):
    """Create test context object and instantiate test class.
    :type test_class: ducktape.tests.test.Test.__class__
    :type session_context: ducktape.tests.session.SessionContext
    :rtype test_class
    """

    test_context = TestContext(session_context, test_class.__module__, test_class, test_class.run, config=None)
    return test_class(test_context)


class SerialTestRunner(TestRunner):
    def run_all_tests(self):
        for test in self.tests:
            result = TestResult(self.session_context, str(test))
            try:
                self.run_single_test(test)
            except Exception as e:
                result.success = False
                result.summary += e.message + "\n" + traceback.format_exc(limit=16) + "\n"
            finally:
                self.results.add_result(result)

        return self.results

    def run_single_test(self, test_class):
        test = get_test_case(test_class, self.session_context)
        self.logger.debug("Instantiated test class: " + str(test))

        self.logger.debug("Checking if there are enough nodes...")
        if test.min_cluster_size() > self.cluster.num_available_nodes():
            raise RuntimeError(
                "There are not enough nodes available in the cluster to run this test. Needed: %d, Available: %d" %
                (test.min_cluster_size(), self.cluster.num_available_nodes()))

        try:
            # Obtain nodes from the cluster
            self.logger.info(self.__class__.__name__ + ": allocating nodes for " + test.__class__.__name__)
            test.services.allocate_nodes()
            self.logger.info((self.__class__.__name__ + ": allocated %d nodes for %s " %
                              (test.services.num_nodes(), test.__class__.__name__)))

            if hasattr(test, 'setUp'):
                self.logger.info(self.__class__.__name__ + ": setting up " + test.__class__.__name__)
                # start services etc
                test.setUp()

            self.logger.info(self.__class__.__name__ + ": running " + test.__class__.__name__)
            test.run()
            self.logger.info(self.__class__.__name__ + ": successfully ran " + test.__class__.__name__)
        except Exception as e:
            raise e
        finally:
            if hasattr(test, 'tearDown'):
                self.logger.info(self.__class__.__name__ + ": tearing down " + test.__class__.__name__)
                test.tearDown()



