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

import logging
import os
import random
import shutil
import sys
import tempfile

from ducktape.cluster.cluster_spec import ClusterSpec
from ducktape.tests.test import Test, TestContext, _escape_pathname, _compress_cmd, in_dir, in_temp_dir
from tests import ducktape_mock


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


class CheckLifecycle(object):
    def check_test_context_double_close(self):
        context = TestContext(session_context=ducktape_mock.session_context(),
                              cls=DummyTest, function=DummyTest.test_function_description)
        context.close()
        context.close()
        assert not hasattr(context, "services")

    def check_cluster_property(self):
        exp_cluster = ClusterSpec.simple_linux(5)
        tc = TestContext(session_context=ducktape_mock.session_context(), cluster=exp_cluster,
                         cls=DummyTest, function=DummyTest.test_function_description)
        test_obj = tc.cls(tc)
        assert test_obj.cluster == exp_cluster


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
        context = TestContext(session_context=ducktape_mock.session_context(),
                              cls=DummyTest, function=DummyTest.test_function_description)
        assert context.description == "function description"

    def check_from_class(self):
        """If the test method has no docstring, description should come from the class docstring"""
        context = TestContext(session_context=ducktape_mock.session_context(),
                              cls=DummyTest, function=DummyTest.test_class_description)
        assert context.description == "class description"

    def check_no_description(self):
        """If nobody has a docstring, there shouldn't be an error, and description should be empty string"""
        context = TestContext(session_context=ducktape_mock.session_context(),
                              cls=DummyTestNoDescription, function=DummyTestNoDescription.test_this)
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
        with in_dir(self.tempdir):
            os.system("tar xzf %s" % (compressed_path))
            assert os.path.exists(uncompressed_path)

    def check_compress_service_logs_swallow_error(self):
        """Try compressing a non-existent service log, and check that it logs a message without throwing an error.
        """
        from tests.ducktape_mock import session_context
        tc = TestContext(
            session_context=session_context(),
            module=sys.modules[DummyTestNoDescription.__module__],
            cls=DummyTestNoDescription,
            function=DummyTestNoDescription.test_this
        )

        tc._logger = logging.getLogger(__name__)
        temp_log_file = tempfile.NamedTemporaryFile(delete=False).name

        try:
            tmp_log_handler = logging.FileHandler(temp_log_file)
            tc._logger.addHandler(tmp_log_handler)

            test_obj = tc.cls(tc)

            # Expect an error to be triggered but swallowed
            test_obj.compress_service_logs(node=None, service=None, node_logs=["hi"])

            tmp_log_handler.close()
            with open(temp_log_file, "r") as f:
                s = f.read()
                assert s.find("Error compressing log hi") >= 0
        finally:
            if os.path.exists(temp_log_file):
                os.remove(temp_log_file)

    def check_abs_path_file(self):
        """Check compress command on an absolute path to a file"""
        with in_temp_dir() as dir1:
            filename = self._make_random_file(self.tempdir)
            abspath_filename = os.path.abspath(filename)

            # since we're using absolute path to file, we should be able to run the compress command from anywhere
            with in_temp_dir() as dir2:
                assert dir1 != dir2

                os.system(_compress_cmd(abspath_filename))
                self._validate_compressed(abspath_filename)

    def check_relative_path_file(self):
        """Check compress command on a relative path to a file"""
        with in_temp_dir():
            filename = self._make_random_file(self.tempdir)
            with in_dir(self.tempdir):
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
        with in_dir(self.tempdir):
            os.system(_compress_cmd(dirname))
            self._validate_compressed(dirname)

    def teardown_method(self, _):
        if os.path.exists(self.tempdir):
            shutil.rmtree(self.tempdir)
