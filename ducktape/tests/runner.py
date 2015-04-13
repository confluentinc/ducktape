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


from ducktape.logger import Logger
from ducktape.tests.result import TestResult, TestResults

import logging


class TestRunner(Logger):
    def __init__(self, session_context, test_classes, cluster):
        self.tests = test_classes
        self.cluster = cluster
        self.session_context = session_context
        self.results = TestResults(self.session_context)
        logging.basicConfig(level=logging.INFO)

    def run_all_tests(self):
        raise NotImplementedError()


class SerialTestRunner(TestRunner):
    def run_all_tests(self):
        for test in self.tests:
            result = TestResult(self.session_context, str(test))
            try:
                self.run_test(test)

                # some mechanism for collecting summary and/or test data (json?)
            except Exception as e:
                result.success = False
                result.summary += e.message + "\n"
            finally:
                self.results.add_result(result)

        return self.results

    def run_test(self, test_class):
        test = test_class(self.cluster)

        if test.min_cluster_size() > self.cluster.num_available_nodes():
            raise RuntimeError(
                "There are not enough nodes available in the cluster to run this test. Needed: %d, Available: %d" %
                (test.min_cluster_size(), self.cluster.num_available_nodes()))

        test.log_start()

        try:
            if hasattr(test, 'setUp'):
                print self.__class__.__name__ + ": setting up " + test.__class__.__name__
                test.setUp()

            print self.__class__.__name__ + ": running " + test.__class__.__name__
            test.run()
        except Exception as e:
            raise e
        finally:
            if hasattr(test, 'tearDown'):
                print self.__class__.__name__ + ": tearing down " + test.__class__.__name__
                test.tearDown()



