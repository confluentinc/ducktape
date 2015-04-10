# Copyright 2014 Confluent Inc.
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
import importlib
import os
import inspect
import re
import json
import logging


class Test(Logger):
    """
    Base class for tests that provides some minimal helper utilities'
    """

    def __init__(self, cluster):
        self.cluster = cluster

    def log_start(self):
        self.logger.info("Running test %s", self._short_class_name())

    def min_cluster_size(self):
        """
        Subclasses implement this to provide a helpful heuristic to prevent trying to run a test on a cluster
        with too few nodes.
        """
        raise NotImplementedError("All tests must implement this method.")


class TestLoader(Logger):
    DEFAULT_TEST_FILE_PATTERN = "(^test_.*\.py$)|(^.*_test\.py$)"

    def discover(self, base_dir, pattern=DEFAULT_TEST_FILE_PATTERN):
        """Recurse through file hierarchy beginning at base_dir and return a list of all found test classes.

        - Discover modules that 'look like' a test. By default, this means the filename is "test_*" or "*_test.py"
        - Discover test classes within each test module. A test class is a subclass of Test which is a leaf
          (i.e. it has no subclasses).
        """
        if os.path.isfile(base_dir):
            test_files = [os.path.abspath(base_dir)]
        else:
            test_files = self.find_test_files(base_dir, pattern)
        test_modules = self.import_modules(test_files)

        # pull test_classes out of test_modules
        test_classes = []
        for module in test_modules:
            try:
                test_classes.extend(self.get_test_classes(module))
            except Exception as e:
                self.logger.debug("Error getting test classes from module: " + e.message)

        self.logger.debug("Discovered these test classes: " + str(test_classes))
        return test_classes

    def find_test_files(self, base_dir, pattern=DEFAULT_TEST_FILE_PATTERN):
        """Return a list of files underneath base_dir that look like test files.

        The returned file names are absolute paths to the files in question.
        """
        test_files = []

        for pwd, dirs, files in os.walk(base_dir):
            for f in files:
                file_path = os.path.abspath(os.path.join(pwd, f))
                if self.is_test_file(file_path, pattern):
                    test_files.append(file_path)

        return test_files

    def import_modules(self, file_list):
        """Attempt to import modules in the file list.
        Assume all files in the list are absolute paths ending in '.py'

        Return all imported modules.
        """
        module_list = []

        for f in file_list:
            if f[-3:] != ".py" or not os.path.isabs(f):
                raise Exception("Expected absolute path ending in '.py' but got " + f)

            # Try all possible module imports for given file
            path_pieces = f[:-3].split("/")  # Strip off '.py' before splitting
            while len(path_pieces) > 0:
                module_name = '.'.join(path_pieces)
                # Try to import the current file as a module
                try:
                    module_list.append(importlib.import_module(module_name))
                    self.logger.debug("Successfully imported " + module_name)
                    break  # no need to keep trying
                except Exception as e:
                    self.logger.debug("Could not import " + module_name + ": " + e.message)
                    continue
                finally:
                    path_pieces = path_pieces[1:]

        return module_list

    def get_test_classes(self, module):
        """Return list of any all classes in the module object."""
        module_objects = module.__dict__.values()
        return filter(lambda x: self.is_test_class(x), module_objects)

    def is_test_file(self, file_name, pattern=DEFAULT_TEST_FILE_PATTERN):
        """By default, a test file looks like test_*.py or *_test.py"""
        return re.match(pattern, os.path.basename(file_name)) is not None

    def is_test_class(self, obj):
        """An object is a test class if it's a leafy subclass of Test.
        """
        return inspect.isclass(obj) and issubclass(obj, Test) and len(obj.__subclasses__()) == 0


class TestReporter(object):
    def __init__(self, results):
        self.results = results

    def pass_fail(self, success):
        return "PASS" if success else "FAIL"

    def report(self):
        raise NotImplementedError("method report must be implemented by subclasses of TestReporter")


class SimpleReporter(TestReporter):
    def header_string(self):
        header = ""
        header += "Test run with session_id " + self.results.test_session_context.test_session_id + "\n"
        header += self.pass_fail(self.results.get_aggregate_success()) + "\n"
        header += "------------------------------------------------------------\n"

        return header

    def result_string(self, result):
        result_str = ""
        result_str += self.pass_fail(result.success) + ": " + result.test_name + "\n"
        if not result.success:
            result_str += "    " + result.summary + "\n"
        if result.data is not None:
            result_str += json.dumps(result.data) + "\n"

        return result_str

    def report_string(self):
        report_str = ""
        report_str += self.header_string()

        for result in self.results:
            report_str += self.result_string(result)

        return report_str


class SimpleFileReporter(SimpleReporter):
    def report(self):
        report_file = os.path.join(self.results.test_session_context.test_session_report_dir, "summary")
        with open(report_file, "w") as fp:
            fp.write(self.report_string())


class SimpleStdoutReporter(SimpleReporter):
    def report(self):
        print self.report_string()


class TestSessionContext(object):
    def __init__(self, test_session_report_dir, test_session_id):
        self.test_session_report_dir = test_session_report_dir
        self.test_session_id = test_session_id


class TestResult(object):
    def __init__(self, test_session_context, test_name, success=True, summary="", data=None):
        self.test_session_context = test_session_context
        self.test_name = test_name
        self.success = success
        self.summary = summary
        self.data = data


class TestResults(object):
    # TODO make this tread safe - once tests are run in parallel, this will be shared by multiple threads

    def __init__(self, test_session_context):
        # test_name -> test_result
        self.results_map = {}

        # maintains an ordering of test_results
        self.results_list = []

        # Aggregate success of all results
        self.success = True

        self.test_session_context = test_session_context

    def add_result(self, test_result):
        assert test_result.__class__ == TestResult
        self.results_map[test_result.test_name] = test_result
        self.results_list.append(test_result)
        self.success = self.success and test_result.success

    def get_result(self, result_name):
        return self.results_map.get(result_name)

    def get_aggregate_success(self):
        """Check cumulative success of all tests run so far"""
        if not self.success:
            return False

        for result in self:
            if not result.success:
                return False

        return True

    def __iter__(self):
        for item in self.results_list:
            yield item


class TestRunner(Logger):
    DEFAULT_TEST_RUN_ID_FILE = ".ducktape/test_run_id"

    def __init__(self, test_session_context, test_classes, cluster):
        self.tests = test_classes
        self.cluster = cluster
        self.test_session_context = test_session_context
        self.results = TestResults(self.test_session_context)
        logging.basicConfig(level=logging.INFO)

    def run_all_tests(self):
        raise NotImplementedError()


class SerialTestRunner(TestRunner):
    def run_all_tests(self):
        for test in self.tests:
            result = TestResult(self.test_session_context, str(test))
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





