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

from ducktape.tests.test import Test, TestContext
from ducktape.mark import parametrized

import importlib
import inspect
import os
import re


class LoaderException(Exception):
    pass


DEFAULT_TEST_FILE_PATTERN = "(^test_.*\.py$)|(^.*_test\.py$)"
DEFAULT_TEST_FUNCTION_PATTERN = "(^test.*)|(.*test$)"


class TestInfo(object):
    """Helper class used to wrap discovered test information"""
    def __init__(self, module=None, cls=None, function=None, injected_args=None):
        self.module = module
        self.cls = cls
        self.function = function
        self.injected_args = injected_args

    @property
    def module_name(self):
        return "" if self.module is None else self.module.__name__

    @property
    def cls_name(self):
        return "" if self.cls is None else self.cls.__name__

    @property
    def function_name(self):
        return "" if self.function is None else self.function.__name__


class TestLoader(object):
    """Class used to discover and load tests."""

    def __init__(self, session_context):
        self.session_context = session_context
        self.test_file_pattern = DEFAULT_TEST_FILE_PATTERN
        self.test_function_pattern = DEFAULT_TEST_FUNCTION_PATTERN

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
        :type pattern: str
        :rtype: list
        """
        assert type(test_discovery_symbols) == list, "Expected test_discovery_symbols to be a list."
        test_info_list = []
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
                test_info_list.extend(self.expand_module(TestInfo(module=m)))
            if len(cls_name) > 0:
                test_info_list = filter(lambda t: t.cls_name == cls_name, test_info_list)
            if len(method_name) > 0:
                test_info_list = filter(lambda t: t.function_name == method_name, test_info_list)

            if len(test_info_list) == 0:
                raise LoaderException("Didn't find any tests for symbol %s." % symbol)

        test_list = [TestContext(self.session_context, t.module_name, t.cls, t.function, t.injected_args)
                     for t in test_info_list]
        self.logger.debug("Discovered these tests: " + str(test_list))
        return test_list

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
            success = False
            while len(path_pieces) > 0:
                module_name = '.'.join(path_pieces)
                # Try to import the current file as a module
                try:
                    module_list.append(importlib.import_module(module_name))
                    self.logger.debug("Successfully imported " + module_name)
                    success = True
                    break  # no need to keep trying
                except Exception as e:
                    # When importing, exceptions can occur if a) the module
                    # doesn't exist, e.g. we have the wrong path to the
                    # module/there aren't __init__.py files, etc, resulting in
                    # an ImportError or b) the module is valid, but there was
                    # some other error when parsing/executing the module, which
                    # can result in a variety of errors (e.g. IndentationError,
                    # TypeError, etc). Because of the way we are searching for
                    # valid modules in this loop, where we expect some of the
                    # module names we construct to fail to import, we ignore
                    # ImportError here, but log everything else as an
                    # error. This does mean we fail to log a subset of import
                    # errors that we should: if the module is valid but itself
                    # triggers an ImportError (e.g. typo in an import line),
                    # that error will be swallowed.
                    if not isinstance(e, ImportError):
                        self.logger.error("Failed to import %s, which may indicate a broken test that cannot be loaded: %s: %s", module_name, e.__class__.__name__, e)
                finally:
                    path_pieces = path_pieces[1:]

            if not success:
                self.logger.debug("Unable to import %s" % f)

        return module_list

    def expand_module(self, t_info):
        """Return a list of TestInfo objects, one object for every 'testable unit' in t_info.module"""

        test_info_list = []
        module = t_info.module
        module_objects = module.__dict__.values()
        test_classes = [c for c in module_objects if self.is_test_class(c)]

        for cls in test_classes:
            test_info_list.extend(self.expand_class(TestInfo(module=module, cls=cls)))

        return test_info_list

    def expand_class(self, t_info):
        """Return a list of TestInfo objects, one object for each method in t_info.cls"""
        test_methods = []
        for f_name in dir(t_info.cls):
            f = getattr(t_info.cls, f_name)
            if self.is_test_function(f):
                test_methods.append(f)

        test_info_list = []
        for f in test_methods:
            t = TestInfo(module=t_info.module, cls=t_info.cls, function=f)

            if parametrized(f):
                test_info_list.extend(self.expand_function(t))
            else:
                test_info_list.append(t)

        return test_info_list

    def expand_function(self, t_info):
        """Assume t_info.function is marked with @parametrize etc."""
        assert parametrized(t_info.function)

        test_info_list = []
        for f in t_info.function:
            test_info_list.append(
                TestInfo(module=t_info.module, cls=t_info.cls, function=f, injected_args=f.kwargs))

        return test_info_list

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
