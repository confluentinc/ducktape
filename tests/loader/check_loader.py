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
            file_path = os.path.abspath(os.path.join(pwd, f))
            num_tests += num_tests_in_file(file_path)
    return num_tests


class CheckTestLoader(object):
    def setup_method(self, method):
        self.SESSION_CONTEXT = tests.ducktape_mock.session_context()
        # To simplify unit tests, add file:// support to the test loader's functionality for loading previous
        # report.json files
        _requests_session.mount('file://', LocalFileAdapter())

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

    def check_test_loader_multiple_files(self):
        loader = TestLoader(self.SESSION_CONTEXT, logger=Mock())
        file_a = os.path.join(discover_dir(), "test_a.py")
        file_b = os.path.join(discover_dir(), "test_b.py")

        tests = loader.load([file_a, file_b])
        assert len(tests) == num_tests_in_file(file_a) + num_tests_in_file(file_b)

    def check_test_loader_with_nonexistent_file(self):
        """Check discovery on a non-existent path should throw LoaderException"""
        with pytest.raises(LoaderException):
            loader = TestLoader(self.SESSION_CONTEXT, logger=Mock())
            loader.load([os.path.join(discover_dir(), "file_that_does_not_exist.py")])

    def check_test_loader_with_class(self):
        """Check test discovery with discover class syntax."""
        loader = TestLoader(self.SESSION_CONTEXT, logger=Mock())
        tests = loader.load([os.path.join(discover_dir(), "test_b.py::TestBB")])
        assert len(tests) == 2

        # Sanity check, test that it discovers two test class & 3 tests if it searches the whole module
        tests = loader.load([os.path.join(discover_dir(), "test_b.py")])
        assert len(tests) == 3

    def check_test_loader_with_injected_args(self):
        """When the --parameters command-line option is used, the loader behaves a little bit differently:

        each test method annotated with @parametrize or @matrix should only expand to a single discovered test,
        and the injected args should be those passed in from command-line.
        """
        parameters = {"x": 1, "y": -1}
        loader = TestLoader(self.SESSION_CONTEXT, logger=Mock(), injected_args=parameters)

        file = os.path.join(discover_dir(), "test_decorated.py")
        tests = loader.load([file])
        assert len(tests) == 4

        for t in tests:
            assert t.injected_args == parameters

    def check_test_loader_with_subsets(self):
        """Check that computation of subsets work properly. This validates both that the division of tests is correct
        (i.e. as even a distribution as we can get but uneven in the expected way when necessary) and that the division
        happens after the expansion of tests marked for possible expansion (e.g. matrix, parametrize)."""

        file = os.path.join(discover_dir(), "test_decorated.py")

        # The test file contains 15 tests. With 4 subsets, first three subsets should have an "extra"
        loader = TestLoader(self.SESSION_CONTEXT, logger=Mock(), subset=0, subsets=4)
        tests = loader.load([file])
        assert len(tests) == 4

        loader = TestLoader(self.SESSION_CONTEXT, logger=Mock(), subset=1, subsets=4)
        tests = loader.load([file])
        assert len(tests) == 4

        loader = TestLoader(self.SESSION_CONTEXT, logger=Mock(), subset=2, subsets=4)
        tests = loader.load([file])
        assert len(tests) == 4

        loader = TestLoader(self.SESSION_CONTEXT, logger=Mock(), subset=3, subsets=4)
        tests = loader.load([file])
        assert len(tests) == 3

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
            'dir': 'path/to/dir',
            'file': 'test_file.py',
            'cls': 'ClassName',
            'method': 'method'
        },
        ->
        'path/to/dir/test_file.py::ClassName.method'
    """
    symbol = os.path.join(parsed['dir'], parsed['file'])

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
                'dir': 'path/to/dir',
                'file': '',
                'cls': '',
                'method': ''
            },
            {
                'dir': 'path/to/dir',
                'file': 'test_file.py',
                'cls': '',
                'method': ''
            },
            {
                'dir': 'path/to/dir',
                'file': 'test_file.py',
                'cls': 'ClassName',
                'method': ''
            },
            {
                'dir': 'path/to/dir',
                'file': 'test_file.py',
                'cls': 'ClassName',
                'method': 'method'
            },
            {
                'dir': 'path/to/dir',
                'file': '',
                'cls': 'ClassName',
                'method': ''
            },
        ]

        loader = TestLoader(tests.ducktape_mock.session_context(), logger=Mock())
        for parsed in parsed_symbols:
            symbol = join_parsed_symbol_components(parsed)

            expected_parsed = (
                normalize_ending_slash(parsed['dir']),
                parsed['file'],
                parsed['cls'],
                parsed['method']
            )

            actually_parsed = loader._parse_discovery_symbol(symbol)
            actually_parsed = (
                normalize_ending_slash(actually_parsed[0]),
                actually_parsed[1],
                actually_parsed[2],
                actually_parsed[3]
            )

            assert actually_parsed == expected_parsed, "%s did not parse as expected" % symbol
