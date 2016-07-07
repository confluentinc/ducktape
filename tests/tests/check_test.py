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

import os
import random
import shutil
import tempfile

from tests import ducktape_mock
from ducktape.tests.test import Test, TestContext, _escape_pathname, _compress_cmd


class CheckEscapePathname(object):

    def check_illegal_path(self):
        path = "\\/.a=2,   b=x/y/z"
        assert _escape_pathname(path) == "a=2.b=x.y.z"

    def check_negative(self):
        # it's better if negative numbers are preserved
        path = "x= -2, y=-50"
        assert _escape_pathname(path) == "x=-2.y=-50"

    def check_many_dots(self):
        path = "..a.....b.c...d."
        assert _escape_pathname(path) == "a.b.c.d"


class CheckDescription(object):
    """Check that pulling a description from a test works as expected."""
    def check_from_function(self):
        """If the function has a docstring, the description should come from the function"""
        context = TestContext(session_context=ducktape_mock.session_context(), cls=DummyTest, function=DummyTest.test_function_description)
        assert context.description == "function description"

    def check_from_class(self):
        """If the test method has no docstring, description should come from the class docstring"""
        context = TestContext(session_context=ducktape_mock.session_context(), cls=DummyTest, function=DummyTest.test_class_description)
        assert context.description == "class description"

    def check_no_description(self):
        """If nobody has a docstring, there shouldn't be an error, and description should be empty string"""
        context = TestContext(session_context=ducktape_mock.session_context(), cls=DummyTestNoDescription, function=DummyTestNoDescription.test_this)
        assert context.description == ""


class CheckCompressCmd(object):
    """Check expected behavior of compress command used before collecting service logs"""
    def setup_method(self, _):
        self.tempdir = tempfile.mkdtemp()

    def _make_random_file(self, dir, num_chars=10000):
        """Populate filename with random characters."""
        filename = os.path.join(dir, "f-%d" % random.randint(1, 2**63 - 1))
        content = "".join([random.choice("0123456789abcdefghijklmnopqrstuvwxyz\n") for _ in range(num_chars)])
        with open(filename, "w") as f:
            f.writelines(content)
        return filename

    def _make_files(self, dir, num_files=10):
        """Populate dir with several files with random characters."""
        for i in range(num_files):
            self._make_random_file(dir)

    def _validate_compressed(self, uncompressed_path):
        if uncompressed_path.endswith(os.path.sep):
            uncompressed_path = uncompressed_path[:-len(os.path.sep)]

        compressed_path = uncompressed_path + ".tgz"

        # verify original file is replaced by filename.tgz
        assert os.path.exists(compressed_path)
        assert not os.path.exists(uncompressed_path)

        # verify that uncompressing gets us back the original
        os.chdir(self.tempdir)
        os.system("tar xzf %s" % (compressed_path))
        assert os.path.exists(uncompressed_path)

    def check_abs_path_file(self):
        """Check compress command on an absolute path to a file"""
        filename = self._make_random_file(self.tempdir)
        abspath_filename = os.path.abspath(filename)

        # since we're using absolute path to file, we should be able to run the compress command from anywhere
        tempdir2 = tempfile.mkdtemp(dir=self.tempdir)
        os.chdir(tempdir2)
        os.system(_compress_cmd(abspath_filename))
        self._validate_compressed(abspath_filename)

    def check_relative_path_file(self):
        """Check compress command on a relative path to a file"""
        filename = self._make_random_file(self.tempdir)
        os.chdir(self.tempdir)
        filename = os.path.basename(filename)
        assert len(filename.split(os.path.sep)) == 1

        # compress it!
        os.system(_compress_cmd(filename))
        self._validate_compressed(filename)

    def check_abs_path_dir(self):
        """Validate compress command with absolute path to a directory"""
        dirname = tempfile.mkdtemp(dir=self.tempdir)
        dirname = os.path.abspath(dirname)
        self._make_files(dirname)

        # compress it!
        if not dirname.endswith(os.path.sep):
            # extra check - ensure all of this works with trailing '/'
            dirname += os.path.sep

        os.system(_compress_cmd(dirname))
        self._validate_compressed(dirname)

    def check_relative_path_dir(self):
        """Validate tarball compression of a directory"""
        dirname = tempfile.mkdtemp(dir=self.tempdir)
        self._make_files(dirname)

        dirname = os.path.basename(dirname)
        assert len(dirname.split(os.path.sep)) == 1

        # compress it!
        os.chdir(self.tempdir)
        os.system(_compress_cmd(dirname))
        self._validate_compressed(dirname)

    def teardown_method(self, _):
        if os.path.exists(self.tempdir):
            shutil.rmtree(self.tempdir)


class DummyTest(Test):
    """class description"""
    def test_class_description(self):
        pass

    def test_function_description(self):
        """function description"""
        pass


class DummyTestNoDescription(Test):
    def test_this(self):
        pass