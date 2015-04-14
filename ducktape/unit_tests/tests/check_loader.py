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

from ducktape.tests.loader import TestLoader

import os


def find_ducktape_base_dir():
    """Return ducktape root directory.

    Ducktape root directory is the directory named "ducktape" (containing setup.py) above the ducktape python package.
    This function assumes there aren't additional directories named "ducktape" in the path, and that we are
    running it from *somewhere* inside the ducktape root directory.
    """
    pwd = os.path.abspath(os.path.curdir)
    pwd_vector = pwd.split("/")

    i = pwd_vector.index("ducktape")  # leftmost index
    assert i >= 0
    pwd_vector = pwd_vector[:i + 1]   # truncate path components after first occurrence of "ducktape"

    root_ducktape_path = "/".join(pwd_vector)
    return os.path.abspath(root_ducktape_path)


def discover_dir():
    """Return the absolute path to the directory to use with discovery tests."""
    base_dir = find_ducktape_base_dir()
    return os.path.join(base_dir, "ducktape", "unit_tests", "tests", "resources", "loader_test_directory")


class CheckTestLoader(object):

    def check_test_loader_with_directory(self):
        """Check discovery on a directory."""
        loader = TestLoader()
        test_classes = loader.discover(discover_dir())
        assert len(test_classes) == 4

    def check_test_loader_with_file(self):
        """Check discovery on a file. """
        loader = TestLoader()
        test_classes = loader.discover(os.path.join(discover_dir(), "test_a.py"))
        assert len(test_classes) == 1




