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
from ducktape.tests.session_context import TestContext

import errno
import logging
import os
import traceback


class TestRunner(Logger):
    def __init__(self, session_context, test_classes, cluster):
        self.tests = test_classes
        self.cluster = cluster
        self.session_context = session_context
        self.results = TestResults(self.session_context)
        logging.basicConfig(level=logging.INFO)

    def run_all_tests(self):
        raise NotImplementedError()


def mkdir_p(path):
    """mkdir -p functionality.
    :type path: str
    """
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def get_test_case(test_class, session_context):
    """Create test context object and instantiate test class.
    :type test_class: ducktape.tests.test.Test.__class__
    :type session_context: ducktape.tests.session_context.SessionContext
    :rtype test_class
    """
    print str(session_context)
    print test_class.__module__
    print test_class
    print test_class.run.__name__

    test_context = TestContext(session_context, test_class.__module__, test_class, test_class.run, config=None)

    mkdir_p(test_context.get_log_dir())
    fh = logging.FileHandler(os.path.join(test_context.get_log_dir(), "test_log"))
    fh.setLevel(logging.DEBUG)
    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.ERROR)
    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    # add the handlers to the logger
    test_context.logger.addHandler(fh)
    test_context.logger.addHandler(ch)

    return test_class(test_context)


class SerialTestRunner(TestRunner):
    def run_all_tests(self):
        for test in self.tests:
            result = TestResult(self.session_context, str(test))
            try:
                self.run_test(test)

                # some mechanism for collecting summary and/or test data (json?)
            except Exception as e:
                result.success = False
                result.summary += e.message + "\n" + traceback.format_exc(limit=16) + "\n"
            finally:
                self.results.add_result(result)

        return self.results

    def run_test(self, test_class):
        test = get_test_case(test_class, self.session_context)
        print "Instantiated test class:", str(test)

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
            print self.__class__.__name__ + ": successfully run " + test.__class__.__name__
        except Exception as e:
            raise e
        finally:
            if hasattr(test, 'tearDown'):
                print self.__class__.__name__ + ": tearing down " + test.__class__.__name__
                test.tearDown()



