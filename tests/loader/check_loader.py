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

from ducktape.tests.loader import TestLoader, LoaderException, _requests_session

import tests.ducktape_mock

import os
import os.path
import pytest
import re
import requests

from mock import Mock
from requests_testadapter import Resp


class LocalFileAdapter(requests.adapters.HTTPAdapter):
    def build_response_from_file(self, request):
        file_path = request.url[7:]
        with open(file_path, 'rb') as file:
            buff = bytearray(os.path.getsize(file_path))
            file.readinto(buff)
            resp = Resp(buff)
            r = self.build_response(request, resp)

            return r

    def send(self, request, stream=False, timeout=None,
             verify=True, cert=None, proxies=None):

        return self.build_response_from_file(request)


def resources_dir():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "resources")


def discover_dir():
    """Return the absolute path to the directory to use with discovery tests."""
    return os.path.join(resources_dir(), "loader_test_directory")


def sub_dir_a():
    return os.path.join(discover_dir(), "sub_dir_a")


def num_tests_in_file(fpath):
    """Count expected number of tests in the file.
    Search for NUM_TESTS = N

    return N if pattern is present else 0
    """
    with open(fpath, 'r') as fd:
        match = re.search(r'^NUM_TESTS\s*=\s*(\d+)', fd.read(), re.MULTILINE)

        if not match:
            return 0
        return int(match.group(1))


def num_tests_in_dir(dpath):
    """Walk through directory subtree and count up expected number of tests that TestLoader should find."""
    assert os.path.exists(dpath)
    assert os.path.isdir(dpath)

    num_tests = 0
    for pwd, dirs, files in os.walk(dpath):
        for f in files:
            if not f.endswith('.py'):
                continue
            file_path = os.path.abspath(os.path.join(pwd, f))
            num_tests += num_tests_in_file(file_path)
    return num_tests


def invalid_test_suites():
    dpath = os.path.join(discover_dir(), 'invalid_test_suites')
    params = []
    for pwd, dirs, files in os.walk(dpath):
        for f in files:
            if not f.endswith('.yml'):
                continue
            file_path = os.path.abspath(os.path.join(pwd, f))
            params.append(pytest.param(file_path, id=os.path.basename(file_path)))
    return params


class CheckTestLoader(object):
    def setup_method(self, method):
        self.SESSION_CONTEXT = tests.ducktape_mock.session_context()
        # To simplify unit tests, add file:// support to the test loader's functionality for loading previous
        # report.json files
        _requests_session.mount('file://', LocalFileAdapter())

    @pytest.mark.parametrize('suite_file_path', invalid_test_suites())
    def check_test_loader_raises_on_invalid_test_suite(self, suite_file_path):
        loader = TestLoader(self.SESSION_CONTEXT, logger=Mock())
        with pytest.raises(LoaderException):
            loader.load([suite_file_path])

    @pytest.mark.parametrize(['expected_count', 'input_symbols', 'excluded_symbols'], [
        pytest.param(8, [
            # see test suite files for number of tests in it.
            # decorated test suite includes 2 tests;
            # single includes 4 tests; multiple includes 3 tests;
            # however both single and multiple test suites include one test method of test_by.py,
            # so total -= 1
            os.path.join(discover_dir(), 'test_suite_single.yml'),
            os.path.join(discover_dir(), 'test_suite_multiple.yml'),
            os.path.join(discover_dir(), 'test_suite_decorated.yml'),
        ], None, id='load multiple test suite files'),
        pytest.param(5, [
            # see test suite file for number of tests in it
            os.path.join(discover_dir(), 'test_suites', 'test_suite_glob.yml')
        ], None, id='load test suite with globs'),
        pytest.param(2, [
            # test suite that includes sub_dir_a/test_c.py (1 test total):
            os.path.join(discover_dir(), 'test_suites', 'sub_dir_a_test_c.yml'),
            # explicitly include test_a.yml (1 test total)
            os.path.join(discover_dir(), 'test_a.py')
        ], None, id='load both file and suite'),
        pytest.param(1, [
            # test suite that includes sub_dir_a/test_c.py (1 test total):
            os.path.join(discover_dir(), 'test_suites', 'sub_dir_a_test_c.yml'),
            # explicitly include test_a.yml (1 test total)
            os.path.join(discover_dir(), 'test_a.py')
        ], [
            # explicitly exclude the sub_dir_a/test_c.py (included with test suite):
            os.path.join(sub_dir_a(), 'test_c.py'),
        ], id='global exclude overrides test suite include'),
        pytest.param(4, [
            # sub_dir_a contains 4 total tests
            # test suite that includes sub_dir_a/*.py but excludes sub_dir_a/test_d.py:
            os.path.join(discover_dir(), 'test_suites', 'sub_dir_a_with_exclude.yml'),
            # explicitly include sub_dir_a/test_d.py to override exclusion from test suite:
            os.path.join(sub_dir_a(), 'test_d.py')
        ], None, id='global include overrides test suite exclude'),
        pytest.param(1, [
            # load two test suites and two files that all point to the same actual test
            # and verify that in the end only 1 test has been loaded
            os.path.join(discover_dir(), 'test_suites', 'sub_dir_a_test_c.yml'),
            os.path.join(discover_dir(), 'test_suites', 'sub_dir_a_test_c_via_class.yml'),
            os.path.join(sub_dir_a(), 'test_c.py'),
            os.path.join(sub_dir_a(), 'test_c.py::TestC')
        ], None, id='same test in test suites and test files')
    ])
    def check_test_loader_with_test_suites_and_files(self, expected_count, input_symbols, excluded_symbols):
        """
        When both files and test suites are loaded, files (both included and excluded) are
        loaded after and separately from the test suites, so even if a test suite excludes file A,
        it will be included if it's passed directly. And if file A is excluded directly, even if any of
        the test suites includes it, it will still be excluded.
        """
        loader = TestLoader(self.SESSION_CONTEXT, logger=Mock())
        tests = loader.load(input_symbols, excluded_test_symbols=excluded_symbols)
        assert len(tests) == expected_count

    def check_test_loader_with_directory(self):
        """Check discovery on a directory."""
        loader = TestLoader(self.SESSION_CONTEXT, logger=Mock())
        tests = loader.load([discover_dir()])
        assert len(tests) == num_tests_in_dir(discover_dir())

    def check_test_loader_with_file(self):
        """Check discovery on a file. """
        loader = TestLoader(self.SESSION_CONTEXT, logger=Mock())
        module_path = os.path.join(discover_dir(), "test_a.py")

        tests = loader.load([module_path])
        assert len(tests) == num_tests_in_file(module_path)

    def check_test_loader_with_glob(self):
        loader = TestLoader(self.SESSION_CONTEXT, logger=Mock())
        file_glob = os.path.join(discover_dir(), "*_a.py")  # should resolve to test_a.py only
        tests = loader.load([file_glob])
        assert len(tests) == 1

    def check_test_loader_multiple_files(self):
        loader = TestLoader(self.SESSION_CONTEXT, logger=Mock())
        file_a = os.path.join(discover_dir(), "test_a.py")
        file_b = os.path.join(discover_dir(), "test_b.py")

        tests = loader.load([file_a, file_b])
        assert len(tests) == num_tests_in_file(file_a) + num_tests_in_file(file_b)

    def check_test_loader_include_dir_exclude_file(self):
        loader = TestLoader(self.SESSION_CONTEXT, logger=Mock())
        excluded_file_a = os.path.join(discover_dir(), "test_a.py")
        excluded_file_b = os.path.join(discover_dir(), "test_b.py")
        num_excluded = num_tests_in_file(excluded_file_a) + num_tests_in_file(excluded_file_b)
        tests = loader.load([discover_dir()], [excluded_file_a, excluded_file_b])
        assert len(tests) == num_tests_in_dir(discover_dir()) - num_excluded

    def check_test_loader_exclude_subdir(self):
        loader = TestLoader(self.SESSION_CONTEXT, logger=Mock())
        included_dir = discover_dir()
        excluded_dir = sub_dir_a()
        tests = loader.load([included_dir], [excluded_dir])
        assert len(tests) == num_tests_in_dir(included_dir) - num_tests_in_dir(excluded_dir)

    def check_test_loader_exclude_subdir_glob(self):
        loader = TestLoader(self.SESSION_CONTEXT, logger=Mock())
        included_dir = discover_dir()
        excluded_dir = sub_dir_a()
        excluded_dir_glob = os.path.join(sub_dir_a(), "*.py")
        tests = loader.load([included_dir], [excluded_dir_glob])
        assert len(tests) == num_tests_in_dir(included_dir) - num_tests_in_dir(excluded_dir)

    def check_test_loader_raises_when_nothing_is_included(self):
        loader = TestLoader(self.SESSION_CONTEXT, logger=Mock())
        file_a = os.path.join(discover_dir(), "test_a.py")
        file_b = os.path.join(discover_dir(), "test_b.py")
        with pytest.raises(LoaderException):
            loader.load([file_a, file_b], [discover_dir()])

    def check_test_loader_raises_on_include_subdir_exclude_parent_dir(self):
        loader = TestLoader(self.SESSION_CONTEXT, logger=Mock())
        with pytest.raises(LoaderException):
            loader.load([(sub_dir_a())], [(discover_dir())])

    def check_test_loader_with_nonexistent_file(self):
        """Check discovery on a non-existent path should throw LoaderException"""
        with pytest.raises(LoaderException):
            loader = TestLoader(self.SESSION_CONTEXT, logger=Mock())
            loader.load([os.path.join(discover_dir(), "file_that_does_not_exist.py")])

    def check_test_loader_include_dir_without_tests(self):
        with pytest.raises(LoaderException):
            loader = TestLoader(self.SESSION_CONTEXT, logger=Mock())
            loader.load([os.path.join(discover_dir(), "sub_dir_no_tests")])

    def check_test_loader_include_file_without_tests(self):
        with pytest.raises(LoaderException):
            loader = TestLoader(self.SESSION_CONTEXT, logger=Mock())
            loader.load([os.path.join(discover_dir(), "sub_dir_no_tests", "just_some_file.py")])

    def check_test_loader_allow_exclude_dir_without_tests(self):
        loader = TestLoader(self.SESSION_CONTEXT, logger=Mock())
        tests = loader.load([discover_dir()], [os.path.join(discover_dir(), "sub_dir_no_tests")])
        assert len(tests) == num_tests_in_dir(discover_dir())

    def check_test_loader_allow_exclude_file_without_tests(self):
        loader = TestLoader(self.SESSION_CONTEXT, logger=Mock())
        tests = loader.load([discover_dir()], [os.path.join(discover_dir(), "sub_dir_no_tests", "just_some_file.py")])
        assert len(tests) == num_tests_in_dir(discover_dir())

    def check_test_loader_allow_exclude_nonexistent_file(self):
        loader = TestLoader(self.SESSION_CONTEXT, logger=Mock())
        tests = loader.load([discover_dir()], [os.path.join(discover_dir(), "file_that_does_not_exist.py")])
        assert len(tests) == num_tests_in_dir(discover_dir())

    def check_test_loader_with_class(self):
        """Check test discovery with discover class syntax."""
        loader = TestLoader(self.SESSION_CONTEXT, logger=Mock())
        tests = loader.load([os.path.join(discover_dir(), "test_b.py::TestBB")])
        assert len(tests) == 2

        # Sanity check, test that it discovers two test class & 3 tests if it searches the whole module
        tests = loader.load([os.path.join(discover_dir(), "test_b.py")])
        assert len(tests) == 3

    def check_test_loader_include_dir_exclude_class(self):
        """Check test discovery with discover class syntax."""
        loader = TestLoader(self.SESSION_CONTEXT, logger=Mock())
        tests = loader.load([discover_dir()], [os.path.join(discover_dir(), "test_b.py::TestBB")])
        # TestBB contains 2 test methods
        assert len(tests) == num_tests_in_dir(discover_dir()) - 2

    def check_test_loader_include_class_exclude_method(self):
        loader = TestLoader(self.SESSION_CONTEXT, logger=Mock())
        included = [os.path.join(discover_dir(), "test_b.py::TestBB")]
        excluded = [os.path.join(discover_dir(), "test_b.py::TestBB.test_bb_one")]
        tests = loader.load(included, excluded)
        # TestBB contains 2 test methods, but 1 is excluded
        assert len(tests) == 1

    def check_test_loader_include_dir_exclude_method(self):
        loader = TestLoader(self.SESSION_CONTEXT, logger=Mock())
        excluded = [os.path.join(discover_dir(), "test_b.py::TestBB.test_bb_one")]
        tests = loader.load([discover_dir()], excluded)
        # excluded 1 method only
        assert len(tests) == num_tests_in_dir(discover_dir()) - 1

    def check_test_loader_with_matrix_params(self):
        loader = TestLoader(self.SESSION_CONTEXT, logger=Mock())
        included = [os.path.join(discover_dir(), 'test_decorated.py::TestMatrix.test_thing@{"x": 1,"y": "test "}')]
        tests = loader.load(included)
        # TestMatrix contains a single parametrized method. Since we only provide a single set of params, we should
        # end up with a single context
        assert len(tests) == 1
        assert tests[0].injected_args == {'x': 1, 'y': 'test '}

    def check_test_loader_with_multiple_matrix_params(self):
        loader = TestLoader(self.SESSION_CONTEXT, logger=Mock())
        params = '[{"x": 1,"y": "test "}, {"x": 2,"y": "I\'m"}]'
        included = [os.path.join(discover_dir(), 'test_decorated.py::TestMatrix.test_thing@{}'.format(params))]
        tests = loader.load(included)
        # TestMatrix contains a single parametrized method.
        # We provide two sets of params, so we should end up with two contexts
        assert len(tests) == 2
        injected_args = map(lambda t: t.injected_args, tests)
        assert {'x': 1, 'y': 'test '} in injected_args
        assert {'x': 2, 'y': "I'm"} in injected_args

    def check_test_loader_with_parametrize(self):
        loader = TestLoader(self.SESSION_CONTEXT, logger=Mock())
        included = [os.path.join(discover_dir(), 'test_decorated.py::TestParametrized.test_thing@{"x":1,"y":2}')]
        tests = loader.load(included)
        assert len(tests) == 1
        assert tests[0].injected_args == {'x': 1, 'y': 2}

    def check_test_loader_with_parametrize_with_objects(self):
        loader = TestLoader(self.SESSION_CONTEXT, logger=Mock())
        parameters = '{"d": {"a": "A"}, "lst": ["whatever"]}'
        included = [os.path.join(
            discover_dir(), 'test_decorated.py::TestObjectParameters.test_thing@{}'.format(parameters))]
        tests = loader.load(included)
        assert len(tests) == 1
        assert tests[0].injected_args == {'d': {'a': 'A'}, 'lst': ['whatever']}

    def check_test_loader_with_injected_args(self):
        """When the --parameters command-line option is used, the loader behaves a little bit differently:

        each test method annotated with @parametrize or @matrix should only expand to a single discovered test,
        and the injected args should be those passed in from command-line.
        """
        # parameter values don't have to match any of the pre-defined parameters in the annotation
        # moreover, even parameter keys don't have to match method arguments, though if that's the case
        # the runner will complain, but the loader wouldn't care (this has been ducktape's behavior for a while now)
        injected_args = {"x": 100, "y": -100}
        loader = TestLoader(self.SESSION_CONTEXT, logger=Mock(), injected_args=injected_args)

        file = os.path.join(discover_dir(), "test_decorated.py")
        tests = loader.load([file])
        assert len(tests) == 5

        for t in tests:
            assert t.injected_args == injected_args

    def check_test_loader_raises_with_both_injected_args_and_parameters(self):
        """One should not use both --parameters command-line flag and parameterized test symbols at the same time.
        Loader will explicitly raise in such cases to avoid confusing behavior.
        """
        injected_args = {"x": 1, "y": "test "}
        loader = TestLoader(self.SESSION_CONTEXT, logger=Mock(), injected_args=injected_args)
        included = [os.path.join(discover_dir(), 'test_decorated.py::TestMatrix.test_thing@{"x": 1,"y": "test "}')]
        with pytest.raises(LoaderException, match='Cannot use both'):
            loader.load(included)

    def check_test_loader_raises_on_params_not_found(self):
        loader = TestLoader(self.SESSION_CONTEXT, logger=Mock())
        # parameter syntax is valid, but there is no such parameter defined in the test annotation in the code
        included = [os.path.join(discover_dir(), 'test_decorated.py::TestMatrix.test_thing@{"x": 1,"y": "missing"}')]
        with pytest.raises(LoaderException, match='No tests to run'):
            loader.load(included)

    @pytest.mark.parametrize("symbol", [
        # no class
        'test_decorated.py::.test_thing'
        # no method
        'test_decorated.py::TestMatrix@{"x": 1, "y": "test "}'
        # invalid json in params
        'test_decorated.py::TestMatrix.test_thing@{x: 1,"y": "test "}'
    ])
    def check_test_loader_raises_on_malformed_test_discovery_symbol(self, symbol):
        loader = TestLoader(self.SESSION_CONTEXT, logger=Mock())
        included = [os.path.join(discover_dir(), symbol)]
        with pytest.raises(LoaderException, match='Invalid discovery symbol'):
            loader.load(included)

    def check_test_loader_exclude_with_injected_args(self):
        injected_args = {"x": 1, "y": -1}
        loader = TestLoader(self.SESSION_CONTEXT, logger=Mock(), injected_args=injected_args)

        included = [os.path.join(discover_dir(), "test_decorated.py")]
        excluded = [os.path.join(discover_dir(), "test_decorated.py::TestStackedMatrix")]
        tests = loader.load(included, excluded)
        # test_decorated.py contains 5 test methods total
        # we exclude 1 class with 1 method so should be 4
        # exclusion shouldn't care about injected args
        assert len(tests) == 4

        for t in tests:
            assert t.injected_args == injected_args

    def check_test_loader_exclude_with_params(self):
        """
        Checks behavior of exclude flag with parametrized annotations.
        Should exclude only a single permutation of the method
        """
        loader = TestLoader(self.SESSION_CONTEXT, logger=Mock())
        # included 8 tests
        included = [os.path.join(discover_dir(), "test_decorated.py::TestMatrix")]
        # exclude 1 test
        excluded = [os.path.join(discover_dir(), 'test_decorated.py::TestMatrix.test_thing@{"x": 1,"y": "test "}')]
        tests = loader.load(included, excluded)
        assert len(tests) == 7

    def check_test_loader_exclude_with_params_multiple(self):
        """
        Checks behavior of exclude flag with parametrized annotations.
        Should exclude two permutations of the method
        """
        loader = TestLoader(self.SESSION_CONTEXT, logger=Mock())
        # include 8 tests
        included = [os.path.join(discover_dir(), "test_decorated.py::TestMatrix")]
        # exclude 2 tests
        params = '[{"x": 1,"y": "test "}, {"x": 2,"y": "I\'m"}]'
        excluded = [os.path.join(discover_dir(), 'test_decorated.py::TestMatrix.test_thing@{}'.format(params))]
        tests = loader.load(included, excluded)
        assert len(tests) == 6

    def check_test_loader_with_subsets(self):
        """Check that computation of subsets work properly. This validates both that the division of tests is correct
        (i.e. as even a distribution as we can get but uneven in the expected way when necessary) and that the division
        happens after the expansion of tests marked for possible expansion (e.g. matrix, parametrize)."""

        file = os.path.join(discover_dir(), "test_decorated.py")

        # The test file contains 17 tests. With 4 subsets, first subset should have an "extra"
        loader = TestLoader(self.SESSION_CONTEXT, logger=Mock(), subset=0, subsets=4)
        tests = loader.load([file])
        assert len(tests) == 5

        loader = TestLoader(self.SESSION_CONTEXT, logger=Mock(), subset=1, subsets=4)
        tests = loader.load([file])
        assert len(tests) == 4

        loader = TestLoader(self.SESSION_CONTEXT, logger=Mock(), subset=2, subsets=4)
        tests = loader.load([file])
        assert len(tests) == 4

        loader = TestLoader(self.SESSION_CONTEXT, logger=Mock(), subset=3, subsets=4)
        tests = loader.load([file])
        assert len(tests) == 4

    def check_test_loader_with_invalid_subsets(self):
        """Check that the TestLoader throws an exception if the requests subset is larger than the number of subsets"""
        with pytest.raises(ValueError):
            TestLoader(self.SESSION_CONTEXT, logger=Mock(), subset=4, subsets=4)
        with pytest.raises(ValueError):
            TestLoader(self.SESSION_CONTEXT, logger=Mock(), subset=5, subsets=4)

    def check_test_loader_with_time_based_subsets(self):
        """Check that computation of subsets using a report with timing information correctly generates subsets that
        are optimized based on timing rather than number of tests.
        """

        file = os.path.join(discover_dir(), "test_b.py")
        report_url = "file://" + os.path.join(resources_dir(), "report.json")

        # The expected behavior of the current implementation is to add tests to each subset from largest to smallest,
        # using the least full subset each time. The test data with times of (10, 5, 1) should result in the first
        # subset containing 1 test and the second containing 2 (the opposite of the simple count-based strategy)

        loader = TestLoader(self.SESSION_CONTEXT, logger=Mock(), subset=0, subsets=2, historical_report=report_url)
        tests = loader.load([file])
        assert len(tests) == 1

        loader = TestLoader(self.SESSION_CONTEXT, logger=Mock(), subset=1, subsets=2, historical_report=report_url)
        tests = loader.load([file])
        assert len(tests) == 2


def join_parsed_symbol_components(parsed):
    """
    Join together a parsed symbol

    e.g.
        {
            'path': 'path/to/dir/test_file.py',
            'cls': 'ClassName',
            'method': 'method'
        },
        ->
        'path/to/dir/test_file.py::ClassName.method'
    """
    symbol = os.path.join(parsed['path'])

    if parsed['cls'] or parsed['method']:
        symbol += "::"
        symbol += parsed['cls']
        if parsed['method']:
            symbol += "."
            symbol += parsed['method']

    return symbol


def normalize_ending_slash(dirname):
    if dirname.endswith(os.path.sep):
        dirname = dirname[:-len(os.path.sep)]
    return dirname


class CheckParseSymbol(object):
    def check_parse_discovery_symbol(self):
        """Check that "test discovery symbol" parsing logic works correctly"""
        parsed_symbols = [
            {
                'path': 'path/to/dir',
                'cls': '',
                'method': ''
            },
            {
                'path': 'path/to/dir/test_file.py',
                'cls': '',
                'method': ''
            },
            {
                'path': 'path/to/dir/test_file.py',
                'cls': 'ClassName',
                'method': ''
            },
            {
                'path': 'path/to/dir/test_file.py',
                'cls': 'ClassName',
                'method': 'method'
            },
            {
                'path': 'path/to/dir',
                'cls': 'ClassName',
                'method': ''
            },
            {
                'path': 'test_file.py',
                'cls': '',
                'method': ''
            },
            {
                'path': 'test_file.py',
                'cls': 'ClassName',
                'method': ''
            },
            {
                'path': 'test_file.py',
                'cls': 'ClassName',
                'method': 'method'
            }
        ]

        loader = TestLoader(tests.ducktape_mock.session_context(), logger=Mock())
        for parsed in parsed_symbols:
            symbol = join_parsed_symbol_components(parsed)

            expected_parsed = (
                normalize_ending_slash(parsed['path']),
                parsed['cls'],
                parsed['method']
            )

            actually_parsed = loader._parse_discovery_symbol(symbol)
            actually_parsed = (
                normalize_ending_slash(actually_parsed[0]),
                actually_parsed[1],
                actually_parsed[2]
            )

            assert actually_parsed == expected_parsed, "%s did not parse as expected" % symbol
