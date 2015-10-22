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

from ducktape.tests.loader import TestLoader, LoaderException

import tests.ducktape_mock

import os
import os.path
import pytest
import re


def discover_dir():
    """Return the absolute path to the directory to use with discovery tests."""
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "resources", "loader_test_directory")


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

    def check_test_loader_with_directory(self):
        """Check discovery on a directory."""
        loader = TestLoader(self.SESSION_CONTEXT)
        tests = loader.discover([discover_dir()])
        assert len(tests) == num_tests_in_dir(discover_dir())

    def check_test_loader_with_file(self):
        """Check discovery on a file. """
        loader = TestLoader(self.SESSION_CONTEXT)
        module_path = os.path.join(discover_dir(), "test_a.py")

        tests = loader.discover([module_path])
        assert len(tests) == num_tests_in_file(module_path)

    def check_test_loader_multiple_files(self):
        loader = TestLoader(self.SESSION_CONTEXT)
        file_a = os.path.join(discover_dir(), "test_a.py")
        file_b = os.path.join(discover_dir(), "test_b.py")

        tests = loader.discover([file_a, file_b])
        assert len(tests) == num_tests_in_file(file_a) + num_tests_in_file(file_b)

    def check_test_loader_with_nonexistent_file(self):
        """Check discovery on a non-existent path should throw LoaderException"""
        with pytest.raises(LoaderException):
            loader = TestLoader(self.SESSION_CONTEXT)
            tests = loader.discover([os.path.join(discover_dir(), "file_that_does_not_exist.py")])

    def check_test_loader_with_class(self):
        """Check test discovery with discover class syntax."""
        loader = TestLoader(self.SESSION_CONTEXT)
        tests = loader.discover([os.path.join(discover_dir(), "test_b.py::TestBB")])
        assert len(tests) == 2

        # Sanity check, test that it discovers two test class & 3 tests if it searches the whole module
        tests = loader.discover([os.path.join(discover_dir(), "test_b.py")])
        assert len(tests) == 3

    def check_test_loader_with_injected_args(self):
        """When the --parameters command-line option is used, the loader behaves a little bit differently:

        each test method annotated with @parametrize or @matrix should only expand to a single discovered test,
        and the injected args should be those passed in from command-line.
        """
        parameters = {"x": 1, "y": -1}
        loader = TestLoader(self.SESSION_CONTEXT, test_parameters=parameters)

        file = os.path.join(discover_dir(), "test_decorated.py")
        tests = loader.discover([file])
        assert len(tests) == 4

        for t in tests:
            assert t.injected_args == parameters


