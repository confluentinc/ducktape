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

import importlib
import inspect
import os
import re

from ducktape.tests.test import Test, TestContext
from ducktape.mark import parametrized, MarkedFunctionExpander


class LoaderException(Exception):
    pass


DEFAULT_TEST_FILE_PATTERN = "(^test_.*\.py$)|(^.*_test\.py$)"
DEFAULT_TEST_FUNCTION_PATTERN = "(^test.*)|(.*test$)"


class TestLoader(object):
    """Class used to discover and load tests."""

    def __init__(self, session_context, test_parameters=None):
        self.session_context = session_context
        self.test_file_pattern = DEFAULT_TEST_FILE_PATTERN
        self.test_function_pattern = DEFAULT_TEST_FUNCTION_PATTERN

        # A non-None value here means the loader will override the injected_args
        # in any discovered test, whether or not it is parametrized
        self.test_parameters = test_parameters

    @property
    def logger(self):
        return self.session_context.logger

    def parse_discovery_symbol(self, discovery_symbol):
        """Parse command-line argument into a tuple (directory, module.py, cls_name, function_name).

        :raise LoaderException if it can't be parsed
        """
        directory = os.path.abspath(os.path.dirname(discovery_symbol))
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
            module, cls_name, method_name = base, "", ""

        return directory, module, cls_name, method_name

    def discover(self, test_discovery_symbols):
        """Recurse through packages in file hierarchy starting at base_dir, and return a list of all found test methods
        in test classes.

        - Discover modules that 'look like' a test. By default, this means the filename is "test_*" or "*_test.py"
        - Discover test classes within each test module. A test class is a subclass of Test which is a leaf
          (i.e. it has no subclasses).
        - Discover test methods within each test class. A test method is a method containing 'test' in its name

        :type test_discovery_symbols: list
        :rtype: list
        """
        assert type(test_discovery_symbols) == list, "Expected test_discovery_symbols to be a list."
        test_context_list = []
        for symbol in test_discovery_symbols:
            directory, module_name, cls_name, method_name = self.parse_discovery_symbol(symbol)

            # Check validity of path
            path = os.path.join(directory, module_name)
            if not os.path.exists(path):
                raise LoaderException("Path {} does not exist".format(path))

            # Recursively search path for test modules
            if os.path.isfile(path):
                test_files = [os.path.abspath(path)]
            else:
                test_files = self.find_test_files(path)
            modules = self.import_modules(test_files)

            # Find all tests in discovered modules and filter out any that don't match the discovery symbol
            for m in modules:
                test_context_list.extend(self.expand_module(m))
            if len(cls_name) > 0:
                test_context_list = filter(lambda t: t.cls_name == cls_name, test_context_list)
            if len(method_name) > 0:
                test_context_list = filter(lambda t: t.function_name == method_name, test_context_list)

            if len(test_context_list) == 0:
                raise LoaderException("Didn't find any tests for symbol %s." % symbol)

        self.logger.debug("Discovered these tests: " + str(test_context_list))
        return test_context_list

    def import_modules(self, file_list):
        """Attempt to import modules in the file list.
        Assume all files in the list are absolute paths ending in '.py'

        Return all imported modules.

        :type file_list: list
        :return list of imported modules
        """
        module_list = []

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
                    module_list.append(importlib.import_module(module_name))
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
                        match = re.search("No module named ([^\s]+)", e.message)

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
                        self.logger.debug("Failed to import %s. This is likely an artifact of the ducktape module loading process: %s: %s", module_name, e.__class__.__name__, e)
                    else:
                        self.logger.error("Failed to import %s, which may indicate a broken test that cannot be loaded: %s: %s", module_name, e.__class__.__name__, e)
                finally:
                    path_pieces = path_pieces[1:]

            if not successful_import:
                self.logger.debug("Unable to import %s" % f)

        return module_list

    def expand_module(self, module):
        """Return a list of TestContext objects, one object for every 'testable unit' in module"""

        test_context_list = []
        module_objects = module.__dict__.values()
        test_classes = [c for c in module_objects if self.is_test_class(c)]

        for cls in test_classes:
            test_context_list.extend(
                self.expand_class(TestContext(session_context=self.session_context, module=module.__name__, cls=cls)))

        return test_context_list

    def expand_class(self, t_ctx):
        """Return a list of TestContext objects, one object for each method in t_ctx.cls"""
        test_methods = []
        for f_name in dir(t_ctx.cls):
            f = getattr(t_ctx.cls, f_name)
            if self.is_test_function(f):
                test_methods.append(f)

        test_context_list = []
        for f in test_methods:
            t = t_ctx.copy(function=f)
            test_context_list.extend(self.expand_function(t))
        return test_context_list

    def expand_function(self, t_ctx):
        expander = MarkedFunctionExpander(t_ctx.session_context, t_ctx.module, t_ctx.cls, t_ctx.function)
        return expander.expand(self.test_parameters)

    def find_test_files(self, base_dir):
        """Return a list of files underneath base_dir that look like test files.
        The returned file names are absolute paths to the files in question.

        :type base_dir: str
        :type pattern: str
        :rtype: list
        """
        test_files = []

        for pwd, dirs, files in os.walk(base_dir):
            if "__init__.py" not in files:
                # Not a package - ignore this directory
                continue
            for f in files:
                file_path = os.path.abspath(os.path.join(pwd, f))
                if self.is_test_file(file_path):
                    test_files.append(file_path)

        return test_files

    def is_test_file(self, file_name):
        """By default, a test file looks like test_*.py or *_test.py"""
        return re.match(self.test_file_pattern, os.path.basename(file_name)) is not None

    def is_test_class(self, obj):
        """An object is a test class if it's a leafy subclass of Test."""
        return inspect.isclass(obj) and issubclass(obj, Test) and len(obj.__subclasses__()) == 0

    def is_test_function(self, function):
        """A test function looks like a test and is callable (or expandable)."""
        if function is None:
            return False

        if not parametrized(function) and not callable(function):
            return False

        return re.match(self.test_function_pattern, function.__name__) is not None
