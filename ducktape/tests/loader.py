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
import glob
import json
import sys
from operator import attrgetter

import collections
import importlib
import inspect
import itertools
import os
import re
from typing import List

import requests
import yaml

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

    def load(self, symbols, excluded_test_symbols=None):
        """
        Discover tests specified by the symbols parameter (iterable of test symbols and/or test suite file paths).
        Skip any tests specified by excluded_test_symbols (iterable of test symbols).

        *Test symbol* is a pointer to the test or a group of tests.
        It is specified by the file/folder path or glob, optionally with Class.method after `::` :
        - `test-dir/`  - loads all tests under `test-dir`  but does NOT load test suites found under `test-dir`
        - `test-dir/prefix_*.py` - loads all files with a specified prefix
        - `/path/to/test/file.py`
        - `test/file.py::TestClass`
        - `test/file.py::TestClass.test_method`

        *Test suite* is a yaml file with the following format:
        ```
            # multiple test suites can be included:
            test_suite_name:
                # list included test symbols
                - path/to/test.py
            # optionally test suite can have included and excluded sections:
            # you may also specify a list of other suite's you wish to import
            # that will also be loaded when loading this file by using the
            # import tag.
            import:
                # list of yaml files whose suites will also run:
                - path/to/suite.yml
            another_test_suite:
                included:
                    # list of included test symbols:
                    - path/to/test-dir/prefix_*.py
                excluded:
                    # list of excluded test symbols:
                    - path/to/test-dir/prefix_excluded.py
        ```
        Each file found after parsing a symbol is checked to see if it contains a test:
        - Discover modules that 'look like' a test. By default, this means the filename is "test_*" or "*_test.py"
        - Discover test classes within each test module. A test class is a subclass of Test which is a leaf
          (i.e. it has no subclasses).
        - Discover test methods within each test class. A test method is a method containing 'test' in its name

        :param symbols: iterable that contains test symbols and/or test suite file paths.
        :param excluded_test_symbols: iterable that contains test symbols only.
        :return list of test context objects found during discovery. Note: if self.repeat is set to n, each test_context
            will appear in the list n times.
        """

        test_symbols = []
        test_suites = []
        # symbol can point to a test or a test suite
        for symbol in symbols:
            if symbol.endswith('.yml'):
                # if it ends with .yml, its a test suite, read included and excluded paths from the file
                test_suites.append(symbol)
            else:
                # otherwise add it to default suite's included list
                test_symbols.append(symbol)

        contexts_from_suites = self._load_test_suite_files(test_suites)
        contexts_from_symbols = self._load_test_contexts(test_symbols)
        all_included = contexts_from_suites.union(contexts_from_symbols)

        # excluded_test_symbols apply to both tests from suites and tests from symbols
        global_excluded = self._load_test_contexts(excluded_test_symbols)
        all_test_context_list = self._filter_excluded_test_contexts(all_included, global_excluded)

        # make sure no test is loaded twice
        all_test_context_list = self._filter_by_unique_test_id(all_test_context_list)

        # Sort to make sure we get a consistent order for when we create subsets
        all_test_context_list = sorted(all_test_context_list, key=attrgetter("test_id"))
        if not all_test_context_list:
            raise LoaderException("No tests to run!")
        self.logger.debug("Discovered these tests: " + str(all_test_context_list))
        # Select the subset of tests.
        if self.historical_report:
            # With timing info, try to pack the subsets reasonably evenly based on timing. To do so, get timing info
            # for each test (using avg as a fallback for missing data), sort in descending order, then start greedily
            # packing tests into bins based on the least full bin at the time.
            raw_results = _requests_session.get(self.historical_report).json()["results"]
            time_results = {r['test_id']: r['run_time_seconds'] for r in raw_results}
            avg_result_time = sum(time_results.values()) / len(time_results)
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

    def discover(self, directory, module_name, cls_name, method_name, injected_args=None):
        """Discover and unpack parametrized tests tied to the given module/class/method

        :return list of test_context objects
        """
        self.logger.debug("Discovering tests at {} - {} - {} - {} - {}"
                          .format(directory, module_name, cls_name, method_name, injected_args))
        # Check validity of path
        path = os.path.join(directory, module_name)
        if not os.path.exists(path):
            raise LoaderException("Path {} does not exist".format(path))

        # Recursively search path for test modules
        module_and_file = self._import_module(path)
        if module_and_file:
            # Find all tests in discovered modules and filter out any that don't match the discovery symbol
            test_context_list = self._expand_module(module_and_file)
            if len(cls_name) > 0:
                test_context_list = filter(lambda t: t.cls_name == cls_name, test_context_list)
            if len(method_name) > 0:
                test_context_list = filter(lambda t: t.function_name == method_name, test_context_list)
            if injected_args is not None:
                if isinstance(injected_args, List):
                    def condition(t):
                        return t.injected_args in injected_args
                else:
                    def condition(t):
                        return t.injected_args == injected_args
                test_context_list = filter(condition, test_context_list)

            listed = list(test_context_list)
            if not listed:
                self.logger.warn("No tests loaded for {} - {} - {} - {} - {}"
                                 .format(directory, module_name, cls_name, method_name, injected_args))
            return listed
        else:
            return []

    def _parse_discovery_symbol(self, discovery_symbol, base_dir=None):
        """Parse a single 'discovery symbol'

        :param discovery_symbol: a symbol used to target test(s).
            Looks like: <path/to/file_or_directory>[::<ClassName>[.method_name]]
        :return tuple of form (directory, module.py, cls_name, function_name)

        :raise LoaderException if it can't be parsed

        Examples:
            "path/to/directory" -> ("path/to/directory", "", "")
            "path/to/test_file.py" -> ("path/to/test_file.py", "", "")
            "path/to/test_file.py::ClassName.method" -> ("path/to/test_file.py", "ClassName", "method")
        """
        def divide_by_symbol(ds, symbol):
            if symbol not in ds:
                return ds, ""
            return ds.split(symbol, maxsplit=1)

        self.logger.debug('Trying to parse discovery symbol {}'.format(discovery_symbol))
        if base_dir:
            discovery_symbol = os.path.join(base_dir, discovery_symbol)
        if discovery_symbol.find("::") >= 0:
            path, cls_name = divide_by_symbol(discovery_symbol, "::")
            # If the part after :: contains a dot, use it to split into class + method
            cls_name, method_name = divide_by_symbol(cls_name, ".")
            method_name, injected_args_str = divide_by_symbol(method_name, "@")

            if injected_args_str:
                if self.injected_args:
                    raise LoaderException("Cannot use both global and per-method test parameters")
                try:
                    injected_args = json.loads(injected_args_str)
                except Exception as e:
                    raise LoaderException("Invalid discovery symbol: cannot parse params: " + injected_args_str) from e
            else:
                injected_args = None
        else:
            # No "::" present in symbol
            path, cls_name, method_name, injected_args = discovery_symbol, "", "", None

        return path, cls_name, method_name, injected_args

    def _import_module(self, file_path):
        """Attempt to import a python module from the file path.
        Assume file_path is an absolute path ending in '.py'

        Return the imported module..

        :param file_path: file to import module from.
        :return ModuleAndFile object that contains the successfully imported module and
            the file from which it was imported
        """
        self.logger.debug("Trying to import module at path {}".format(file_path))
        if file_path[-3:] != ".py" or not os.path.isabs(file_path):
            raise Exception("Expected absolute path ending in '.py' but got " + file_path)

        # Try all possible module imports for given file
        # Strip off '.py' before splitting
        path_pieces = [piece for piece in file_path[:-3].split("/") if len(piece) > 0]
        while len(path_pieces) > 0:
            module_name = '.'.join(path_pieces)
            # Try to import the current file as a module
            self.logger.debug("Trying to import module {}".format(module_name))
            try:
                module_and_file = ModuleAndFile(module=importlib.import_module(module_name), file=file_path)
                self.logger.debug("Successfully imported " + module_name)
                return module_and_file
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
                    match = re.search(r"No module named '?([^\s\']+)'?", str(e))

                    if match is not None:
                        missing_module = match.groups()[0]

                        if missing_module in module_name:
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

        self.logger.debug("Unable to import %s" % file_path)
        return None

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

    def _find_test_files(self, path_or_glob):
        """
        Return a list of files at the specified path (or glob) that look like test files.

        - Globs are not recursive, so ** is not supported.
        - However, if the glob matches a folder (or is not a glob but simply a folder path),
            we will load all tests in that folder and recursively search the sub folders.

        :param path_or_glob: path to a test file, folder with test files or a glob that expands to folders and files
        :return: list of absolute paths to test files
        """
        test_files = []
        self.logger.debug('Looking for test files in {}'.format(path_or_glob))
        # glob is safe to be called on non-glob path - it would just return that same path wrapped in a list
        expanded_glob = glob.glob(path_or_glob)
        self.logger.debug('Expanded {} into {}'.format(path_or_glob, expanded_glob))

        def maybe_add_test_file(f):
            if self._is_test_file(f):
                test_files.append(f)
            else:
                self.logger.debug("Skipping {} because it isn't a test file".format(f))

        for path in expanded_glob:
            if not os.path.exists(path):
                raise LoaderException('Path {} does not exist'.format(path))
            self.logger.debug('Checking {}'.format(path))
            if os.path.isfile(path):
                maybe_add_test_file(path)
            elif os.path.isdir(path):
                for pwd, dirs, files in os.walk(path):
                    if "__init__.py" not in files:
                        # Not a package - ignore this directory
                        continue
                    for f in files:
                        file_path = os.path.abspath(os.path.join(pwd, f))
                        maybe_add_test_file(file_path)
            else:
                raise LoaderException("Got a path that we don't understand: " + path)

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

    def _load_test_suite_files(self, test_suite_files):
        suites = list()

        suites.extend(self._read_test_suite_from_file(test_suite_files))

        all_contexts = set()
        for suite in suites:
            all_contexts.update(self._load_test_suite(**suite))
        return all_contexts

    def _load_file(self, suite_file_path):
        if not os.path.exists(suite_file_path):
            raise LoaderException(f'Path {suite_file_path} does not exist')
        if not os.path.isfile(suite_file_path):
            raise LoaderException(f'{suite_file_path} is not a file, so it cannot be a test suite')

        with open(suite_file_path) as fp:
            try:
                file_content = yaml.load(fp, Loader=yaml.FullLoader)
            except Exception as e:
                raise LoaderException("Failed to load test suite from file: " + suite_file_path, e)

        if not file_content:
            raise LoaderException("Test suite file is empty: " + suite_file_path)
        if not isinstance(file_content, dict):
            raise LoaderException("Malformed test suite file: " + suite_file_path)

        for suite_name, suite_content in file_content.items():
            if not suite_content:
                raise LoaderException("Empty test suite " + suite_name + " in " + suite_file_path)
        return file_content

    def _load_suites(self, file_path, file_content):
        suites = []
        for suite_name, suite_content in file_content.items():
            if not suite_content:
                raise LoaderException(f"Empty test suite {suite_name} in {file_path}")

            # if test suite is just a list of paths, those are included paths
            # otherwise, expect separate sections for included and excluded
            if isinstance(suite_content, list):
                included_paths = suite_content
                excluded_paths = None
            elif isinstance(suite_content, dict):
                included_paths = suite_content.get('included')
                excluded_paths = suite_content.get('excluded')
            else:
                raise LoaderException(f"Malformed test suite {suite_name} in {file_path}")
            suites.append({
                'name': suite_name,
                'included': included_paths,
                'excluded': excluded_paths,
                'base_dir': os.path.dirname(file_path)
            })
        return suites

    def _read_test_suite_from_file(self, root_suite_file_paths):
        root_suite_file_paths = [os.path.abspath(file_path) for file_path in root_suite_file_paths]
        files = {file: self._load_file(file) for file in root_suite_file_paths}
        stack = root_suite_file_paths

        # load all files
        while len(stack) != 0:
            curr = stack.pop()
            loaded = files[curr]
            if 'import' in loaded:
                if isinstance(loaded['import'], str):
                    loaded['import'] = [loaded['import']]
                directory = os.path.dirname(curr)
                # apply path of current file to the files inside
                abs_file_iter = (os.path.abspath(os.path.join(directory, file))
                                 for file in loaded.get('import', []))
                imported = [file for file in abs_file_iter if file not in files]
                for file in imported:
                    files[file] = self._load_file(file)
                stack.extend(imported)
                del files[curr]['import']

        # load all suites from all loaded files
        suites = []
        for file_name, file_context in files.items():
            suites.extend(self._load_suites(file_name, file_context))

        return suites

    def _load_test_suite(self, **kwargs):
        name = kwargs['name']
        included = kwargs['included']
        excluded = kwargs.get('excluded')
        base_dir = kwargs.get('base_dir')
        excluded_contexts = self._load_test_contexts(excluded, base_dir=base_dir)
        included_contexts = self._load_test_contexts(included, base_dir=base_dir)

        self.logger.debug("Including tests: " + str(included_contexts))
        self.logger.debug("Excluding tests: " + str(excluded_contexts))

        # filter out any excluded test from the included tests set
        all_test_context_list = self._filter_excluded_test_contexts(included_contexts, excluded_contexts)
        if not all_test_context_list:
            raise LoaderException("No tests found in  " + name)

        return all_test_context_list

    def _load_test_contexts(self, test_discovery_symbols, base_dir=None):
        """
        Load all test_context objects found in test_discovery_symbols.
        Each test discovery symbol is a dir or file path, optionally with with a ::Class or ::Class.method specified.

        :param test_discovery_symbols: list of test symbols to look into
        :return: List of test_context objects discovered by checking test_discovery_symbols (may be empty if none were
            discovered)
        """
        if not test_discovery_symbols:
            return set()
        if not isinstance(test_discovery_symbols, list):
            raise LoaderException("Expected test_discovery_symbols to be a list.")
        all_test_context_list = set()
        for symbol in test_discovery_symbols:
            path_or_glob, cls_name, method, injected_args = self._parse_discovery_symbol(symbol, base_dir)
            self.logger.debug('Parsed symbol into {} - {} - {} - {}'
                              .format(path_or_glob, cls_name, method, injected_args))
            path_or_glob = os.path.abspath(path_or_glob)

            # TODO: consider adding a check to ensure glob or dir is not used together with cls_name and method
            test_files = []
            if os.path.isfile(path_or_glob):
                # if it is a single file, just add it directly - https://github.com/confluentinc/ducktape/issues/284
                test_files = [path_or_glob]
            else:
                # otherwise, when dealing with a dir or a glob, apply pattern matching rules
                test_files = self._find_test_files(path_or_glob)

            self._add_top_level_dirs_to_sys_path(test_files)

            for test_file in test_files:
                directory = os.path.dirname(test_file)
                module_name = os.path.basename(test_file)
                test_context_list_for_file = self.discover(
                    directory, module_name, cls_name, method, injected_args=injected_args)
                all_test_context_list.update(test_context_list_for_file)
                if len(test_context_list_for_file) == 0:
                    self.logger.warn("Didn't find any tests in %s " % test_file)

        return all_test_context_list

    def _filter_by_unique_test_id(self, contexts):
        contexts_dict = dict()
        for context in contexts:
            if context.test_id not in contexts_dict:
                contexts_dict[context.test_id] = context
        return contexts_dict.values()

    def _filter_excluded_test_contexts(self, included_contexts, excluded_contexts):
        excluded_test_ids = set(map(lambda ctx: ctx.test_id, excluded_contexts))
        return set(filter(lambda ctx: ctx.test_id not in excluded_test_ids, included_contexts))

    def _add_top_level_dirs_to_sys_path(self, test_files):
        seen_dirs = set()
        for path in test_files:
            dir = os.path.dirname(path)
            while os.path.exists(os.path.join(dir, '__init__.py')):
                dir = os.path.dirname(dir)
            if dir not in seen_dirs:
                sys.path.append(dir)
                seen_dirs.add(dir)
