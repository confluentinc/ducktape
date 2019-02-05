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

from operator import attrgetter

import collections
import importlib
import inspect
import itertools
import os
import re
import requests

from ducktape.tests.test import Test, TestContext
from ducktape.mark import parametrized
from ducktape.mark.mark_expander import MarkedFunctionExpander


class LoaderException(Exception):
    pass


# A helper container class
ModuleAndFile = collections.namedtuple('ModuleAndFile', ['module', 'file'])


DEFAULT_TEST_FILE_PATTERN = r"(^test_.*\.py$)|(^.*_test\.py$)"
DEFAULT_TEST_FUNCTION_PATTERN = "(^test.*)|(.*test$)"

# Included for unit tests to be able to add support for loading local file:/// URLs.
_requests_session = requests.session()


class TestLoader(object):
    """Class used to discover and load tests."""

    def __init__(self, session_context, logger, repeat=1, injected_args=None, cluster=None, subset=0, subsets=1,
                 historical_report=None):
        self.session_context = session_context
        self.cluster = cluster
        assert logger is not None
        self.logger = logger

        assert repeat >= 1
        self.repeat = repeat

        if subset >= subsets:
            raise ValueError("The subset to execute must be in the range [0, subsets-1]")
        self.subset = subset
        self.subsets = subsets

        self.historical_report = historical_report

        self.test_file_pattern = DEFAULT_TEST_FILE_PATTERN
        self.test_function_pattern = DEFAULT_TEST_FUNCTION_PATTERN

        # A non-None value here means the loader will override the injected_args
        # in any discovered test, whether or not it is parametrized
        self.injected_args = injected_args

    def load(self, test_discovery_symbols):
        """Recurse through packages in file hierarchy starting at base_dir, and return a list of test_context objects
        for all discovered tests.

        - Discover modules that 'look like' a test. By default, this means the filename is "test_*" or "*_test.py"
        - Discover test classes within each test module. A test class is a subclass of Test which is a leaf
          (i.e. it has no subclasses).
        - Discover test methods within each test class. A test method is a method containing 'test' in its name

        :param test_discovery_symbols: list of file paths
        :return list of test context objects found during discovery. Note: if self.repeat is set to n, each test_context
            will appear in the list n times.
        """
        assert type(test_discovery_symbols) == list, "Expected test_discovery_symbols to be a list."
        all_test_context_list = []
        for symbol in test_discovery_symbols:
            directory, module_name, cls_name, method_name = self._parse_discovery_symbol(symbol)
            directory = os.path.abspath(directory)

            test_context_list_for_symbol = self.discover(directory, module_name, cls_name, method_name)
            all_test_context_list.extend(test_context_list_for_symbol)

            if len(test_context_list_for_symbol) == 0:
                raise LoaderException("Didn't find any tests for symbol %s." % symbol)

        self.logger.debug("Discovered these tests: " + str(all_test_context_list))

        # Sort to make sure we get a consistent order for when we create subsets
        all_test_context_list = sorted(all_test_context_list, key=attrgetter("test_id"))

        # Select the subset of tests.
        if self.historical_report:
            # With timing info, try to pack the subsets reasonably evenly based on timing. To do so, get timing info
            # for each test (using avg as a fallback for missing data), sort in descending order, then start greedily
            # packing tests into bins based on the least full bin at the time.
            raw_results = _requests_session.get(self.historical_report).json()["results"]
            time_results = {r['test_id']: r['run_time_seconds'] for r in raw_results}
            avg_result_time = sum(time_results.itervalues()) / len(time_results)
            time_results = {tc.test_id: time_results.get(tc.test_id, avg_result_time) for tc in all_test_context_list}
            all_test_context_list = sorted(all_test_context_list, key=lambda x: time_results[x.test_id], reverse=True)

            subsets = [[] for _ in range(self.subsets)]
            subsets_accumulated_time = [0] * self.subsets

            for tc in all_test_context_list:
                min_subset_idx = min(range(len(subsets_accumulated_time)), key=lambda i: subsets_accumulated_time[i])
                subsets[min_subset_idx].append(tc.test_id)
                subsets_accumulated_time[min_subset_idx] += time_results[tc.test_id]

            subset_test_context_list = subsets[self.subset]
        else:
            # Without timing info, select every nth test instead of blocks of n to avoid groups of tests that are
            # parametrizations of the same test being grouped together since that can lead to a single, parameterized,
            # long-running test causing a very imbalanced workload across different subsets. Imbalance is still
            # possible, but much less likely using this heuristic.
            subset_test_context_list = list(itertools.islice(all_test_context_list, self.subset, None, self.subsets))

        self.logger.debug("Selected this subset of tests: " + str(subset_test_context_list))
        return subset_test_context_list * self.repeat

    def discover(self, directory, module_name, cls_name, method_name):
        """Discover and unpack parametrized tests tied to the given module/class/method

        :param directory: path to the module containing the test method
        :param module_name: name of the module containing the test method
        :param cls_name: name of the class containing the test method
        :param method_name: name of the targeted test method
        :return list of test_context objects
        """
        # Check validity of path
        path = os.path.join(directory, module_name)
        if not os.path.exists(path):
            raise LoaderException("Path {} does not exist".format(path))

        # Recursively search path for test modules
        test_context_list = []
        if os.path.isfile(path):
            test_files = [os.path.abspath(path)]
        else:
            test_files = self._find_test_files(path)
        modules_and_files = self._import_modules(test_files)

        # Find all tests in discovered modules and filter out any that don't match the discovery symbol
        for mf in modules_and_files:
            test_context_list.extend(self._expand_module(mf))
        if len(cls_name) > 0:
            test_context_list = filter(lambda t: t.cls_name == cls_name, test_context_list)
        if len(method_name) > 0:
            test_context_list = filter(lambda t: t.function_name == method_name, test_context_list)

        return test_context_list

    def _parse_discovery_symbol(self, discovery_symbol):
        """Parse a single 'discovery symbol'

        :param discovery_symbol: a symbol used to target test(s).
            Looks like: <path/to/file_or_directory>[::<ClassName>[.method_name]]
        :return tuple of form (directory, module.py, cls_name, function_name)

        :raise LoaderException if it can't be parsed

        Examples:
            "path/to/directory" -> ("path/to/directory", "", "", "")
            "path/to/test_file.py" -> ("path/to", "test_file.py", "", "")
            "path/to/test_file.py::ClassName.method" -> ("path/to", "test_file.py", "ClassName", "method")
        """
        directory = os.path.dirname(discovery_symbol)
        base = os.path.basename(discovery_symbol)

        if base.find("::") >= 0:
            parts = base.split("::")
            if len(parts) == 1:
                module, cls_name = parts[0], ""
            elif len(parts) == 2:
                module, cls_name = parts
            else:
                raise LoaderException("Invalid discovery symbol: " + discovery_symbol)

            # If the part after :: contains a dot, use it to split into class + method
            parts = cls_name.split('.')
            if len(parts) == 1:
                method_name = ""
            elif len(parts) == 2:
                cls_name, method_name = parts
            else:
                raise LoaderException("Invalid discovery symbol: " + discovery_symbol)
        else:
            # No "::" present in symbol
            module, cls_name, method_name = base, "", ""

        if not module.endswith(".py"):
            directory = os.path.join(directory, module)
            module = ""
        return directory, module, cls_name, method_name

    def _import_modules(self, file_list):
        """Attempt to import modules in the file list.
        Assume all files in the list are absolute paths ending in '.py'

        Return all imported modules.

        :param file_list: list of files which we will try to import
        :return list of ModuleAndFile objects; each object contains the successfully imported module and
            the file from which it was imported
        """
        module_and_file_list = []

        for f in file_list:
            if f[-3:] != ".py" or not os.path.isabs(f):
                raise Exception("Expected absolute path ending in '.py' but got " + f)

            # Try all possible module imports for given file
            path_pieces = filter(lambda x: len(x) > 0, f[:-3].split("/"))  # Strip off '.py' before splitting
            successful_import = False
            while len(path_pieces) > 0:
                module_name = '.'.join(path_pieces)
                # Try to import the current file as a module
                try:
                    module_and_file_list.append(
                        ModuleAndFile(module=importlib.import_module(module_name), file=f))
                    self.logger.debug("Successfully imported " + module_name)
                    successful_import = True
                    break  # no need to keep trying
                except Exception as e:
                    # Because of the way we are searching for
                    # valid modules in this loop, we expect some of the
                    # module names we construct to fail to import.
                    #
                    # Therefore we check if the failure "looks normal", and log
                    # expected failures only at debug level.
                    #
                    # Unexpected errors are aggressively logged, e.g. if the module
                    # is valid but itself triggers an ImportError (e.g. typo in an
                    # import line), or a SyntaxError.

                    expected_error = False
                    if isinstance(e, ImportError):
                        match = re.search(r"No module named ([^\s]+)", e.message)

                        if match is not None:
                            missing_module = match.groups()[0]

                            if missing_module == module_name:
                                expected_error = True
                            else:
                                # The error is still an expected error if missing_module is a suffix of module_name.
                                # This is because the error message may contain only a suffix
                                # of the original module_name if leftmost chunk of module_name is a legitimate
                                # module name, but the rightmost part doesn't exist.
                                #
                                # Check this by seeing if it is a "piecewise suffix" of module_name - i.e. if the parts
                                # delimited by dots match. This is a little bit stricter than just checking for a suffix
                                #
                                # E.g. "fancy.cool_module" is a piecewise suffix of "my.fancy.cool_module",
                                # but  "module" is not a piecewise suffix of "my.fancy.cool_module"
                                missing_module_pieces = missing_module.split(".")
                                expected_error = (missing_module_pieces == path_pieces[-len(missing_module_pieces):])

                    if expected_error:
                        self.logger.debug(
                            "Failed to import %s. This is likely an artifact of the "
                            "ducktape module loading process: %s: %s", module_name, e.__class__.__name__, e)
                    else:
                        self.logger.error(
                            "Failed to import %s, which may indicate a "
                            "broken test that cannot be loaded: %s: %s", module_name, e.__class__.__name__, e)
                finally:
                    path_pieces = path_pieces[1:]

            if not successful_import:
                self.logger.debug("Unable to import %s" % f)

        return module_and_file_list

    def _expand_module(self, module_and_file):
        """Return a list of TestContext objects, one object for every 'testable unit' in module"""

        test_context_list = []
        module = module_and_file.module
        file_name = module_and_file.file
        module_objects = module.__dict__.values()
        test_classes = [c for c in module_objects if self._is_test_class(c)]

        for cls in test_classes:
            test_context_list.extend(self._expand_class(
                TestContext(
                    session_context=self.session_context,
                    cluster=self.cluster,
                    module=module.__name__,
                    cls=cls,
                    file=file_name)))

        return test_context_list

    def _expand_class(self, t_ctx):
        """Return a list of TestContext objects, one object for each method in t_ctx.cls"""
        test_methods = []
        for f_name in dir(t_ctx.cls):
            f = getattr(t_ctx.cls, f_name)
            if self._is_test_function(f):
                test_methods.append(f)

        test_context_list = []
        for f in test_methods:
            t = t_ctx.copy(function=f)
            test_context_list.extend(self._expand_function(t))
        return test_context_list

    def _expand_function(self, t_ctx):
        expander = MarkedFunctionExpander(
            t_ctx.session_context,
            t_ctx.module,
            t_ctx.cls,
            t_ctx.function,
            t_ctx.file,
            t_ctx.cluster)
        return expander.expand(self.injected_args)

    def _find_test_files(self, base_dir):
        """Return a list of files underneath base_dir that look like test files.

        :param base_dir: the base directory from which to search recursively for test files.
        :return: list of absolute paths to test files
        """
        test_files = []

        for pwd, dirs, files in os.walk(base_dir):
            if "__init__.py" not in files:
                # Not a package - ignore this directory
                continue
            for f in files:
                file_path = os.path.abspath(os.path.join(pwd, f))
                if self._is_test_file(file_path):
                    test_files.append(file_path)

        return test_files

    def _is_test_file(self, file_name):
        """By default, a test file looks like test_*.py or *_test.py"""
        return re.match(self.test_file_pattern, os.path.basename(file_name)) is not None

    def _is_test_class(self, obj):
        """An object is a test class if it's a leafy subclass of Test."""
        return inspect.isclass(obj) and issubclass(obj, Test) and len(obj.__subclasses__()) == 0

    def _is_test_function(self, function):
        """A test function looks like a test and is callable (or expandable)."""
        if function is None:
            return False

        if not parametrized(function) and not callable(function):
            return False

        return re.match(self.test_function_pattern, function.__name__) is not None
