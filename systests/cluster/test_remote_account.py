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
from ducktape.cluster.cluster_spec import ClusterSpec, WINDOWS, LINUX, NodeSpec
from ducktape.services.service import Service
from ducktape.tests.test import Test
from ducktape.errors import TimeoutError
from ducktape.mark.resource import cluster

import os
import pytest
import random
import shutil
from six import iteritems
import tempfile
from threading import Thread
import time
import logging

from ducktape.utils.util import wait_until


def generate_tempdir_name():
    """Use this ad-hoc function instead of the tempfile module since we're creating and removing
    this directory with ssh commands.
    """
    return "/tmp/" + "t" + str(int(time.time()))


class RemoteAccountTestService(Service):
    """Simple service that allocates one node for performing tests of RemoteAccount functionality"""

    def __init__(self, context):
        super(RemoteAccountTestService, self).__init__(context, num_nodes=1)
        self.temp_dir = generate_tempdir_name()
        self.logs = {
            "my_log": {
                "path": self.log_file,
                "collect_default": True
            },
            "non_existent_log": {
                "path": os.path.join(self.temp_dir, "absent.log"),
                "collect_default": True
            }
        }

    @property
    def log_file(self):
        return os.path.join(self.temp_dir, "test.log")

    def start_node(self, node):
        node.account.ssh("mkdir -p " + self.temp_dir)
        node.account.ssh("touch " + self.log_file)

    def stop_node(self, node):
        pass

    def clean_node(self, node):
        node.account.ssh("rm -rf " + self.temp_dir)

    def write_to_log(self, msg):
        self.nodes[0].account.ssh("echo -e -n " + repr(msg) + " >> " + self.log_file)


class GenericService(Service):
    """Service which doesn't do anything - just a group of nodes, each of which has a scratch directory."""

    def __init__(self, context, num_nodes):
        super(GenericService, self).__init__(context, num_nodes)
        self.worker_scratch_dir = "scratch"
        for node in self.nodes:
            node.account.mkdirs(self.worker_scratch_dir)

    def stop_node(self, node):
        # noop
        pass

    def clean_node(self, node):
        node.account.remove(self.worker_scratch_dir, allow_fail=True)


class UnderUtilizedTest(Test):

    def setup(self):
        self.service = GenericService(self.test_context, 1)

    @cluster(num_nodes=3)
    def under_utilized_test(self):
        # setup() creates a service instance, which calls alloc() for one node
        assert self.test_context.cluster.max_used() == 1
        assert len(self.test_context.cluster.used()) == 1

        self.another_service = GenericService(self.test_context, 1)
        assert len(self.test_context.cluster.used()) == 2
        assert self.test_context.cluster.max_used() == 2

        self.service.stop()
        self.service.free()
        assert len(self.test_context.cluster.used()) == 1
        assert self.test_context.cluster.max_used() == 2


class FileSystemTest(Test):
    """
    Note that in an attempt to isolate the file system methods, validation should be done with ssh/shell commands.
    """

    def setup(self):
        self.service = GenericService(self.test_context, 1)
        self.node = self.service.nodes[0]
        self.scratch_dir = self.service.worker_scratch_dir

    @cluster(num_nodes=1)
    def create_file_test(self):
        expected_contents = "hello world"
        fname = "myfile.txt"
        fpath = "%s/%s" % (self.scratch_dir, fname)

        self.node.account.create_file(fpath, expected_contents)

        # validate existence and contents
        self.node.account.ssh("test -f %s" % fpath)
        contents = "\n".join([line for line in self.node.account.ssh_capture("cat %s" % fpath)])
        assert contents == expected_contents

        # TODO also check absolute path

    @cluster(num_nodes=1)
    def mkdir_test(self):
        dirname = "%s/mydir" % self.scratch_dir
        self.node.account.mkdir(dirname)

        # TODO - important!! check mode
        self.node.account.ssh("test -d %s" % dirname, allow_fail=False)

        # mkdir should not succeed if the base directories do not already exist
        dirname = "%s/a/b/c/d" % self.scratch_dir
        with pytest.raises(IOError):
            self.node.account.mkdir(dirname)

        # TODO also check absolute path

    @cluster(num_nodes=1)
    def mkdirs_nested_test(self):
        dirname = "%s/a/b/c/d" % self.scratch_dir

        # TODO important!! check mode
        self.node.account.mkdirs(dirname)
        self.node.account.ssh("test -d %s" % dirname, allow_fail=False)

        # TODO also check absolute path

    @cluster(num_nodes=1)
    def open_test(self):
        """Try opening, writing, reading a file."""
        fname = "%s/myfile.txt" % self.scratch_dir
        expected_contents = b"hello world\nhooray!"
        with self.node.account.open(fname, "w") as f:
            f.write(expected_contents)

        with self.node.account.open(fname, "r") as f:
            contents = f.read()
        assert contents == expected_contents

        # Now try opening in append mode
        append = b"hithere"
        expected_contents = expected_contents + append
        with self.node.account.open(fname, "a") as f:
            f.write(append)

        with self.node.account.open(fname, "r") as f:
            contents = f.read()

        assert contents == expected_contents

    @cluster(num_nodes=1)
    def exists_file_test(self):
        """
        Create various kinds of files and symlinks, verifying that exists works as expected.

        Note that because
        """

        # create file, test existence with relative and absolute path
        self.node.account.ssh("touch %s/hi" % self.scratch_dir)
        assert self.node.account.exists("%s/hi" % self.scratch_dir)
        # TODO abspath

        # create symlink, test existence with relative and absolute path
        self.node.account.ssh("ln -s %s/hi %s/hi-link" % (self.scratch_dir, self.scratch_dir))
        assert self.node.account.exists("%s/hi-link" % self.scratch_dir)
        # TODO abspath

    def exists_dir_test(self):
        # check bad path doesn't exist
        assert not self.node.account.exists("a/b/c/d")

        # create dir, test existence with relative and absolute path
        dpath = "%s/mydir" % self.scratch_dir
        self.node.account.ssh("mkdir %s" % dpath)
        assert self.node.account.exists(dpath)
        # TODO abspath

        # create symlink, test existence with relative and absolute path
        self.node.account.ssh("ln -s %s %s/mydir-link" % (dpath, self.scratch_dir))
        assert self.node.account.exists("%s/mydir-link" % self.scratch_dir)
        # # TODO abspath

    def remove_test(self):
        """Test functionality of remove method"""
        # remove a non-empty directory
        dpath = "%s/mydir" % self.scratch_dir
        self.node.account.ssh("mkdir %s" % dpath)
        self.node.account.ssh("touch %s/hi.txt" % dpath)
        self.node.account.ssh("test -d %s" % dpath)
        self.node.account.remove(dpath)
        self.node.account.ssh("test ! -d %s" % dpath)

        # remove a file
        fpath = "%s/hello.txt" % self.scratch_dir
        self.node.account.ssh("echo 'hello world' > %s" % fpath)
        self.node.account.remove(fpath)

        # remove non-existent path
        with pytest.raises(RuntimeError):
            self.node.account.remove("a/b/c/d")

        # remove non-existent path with allow_fail = True should be ok
        self.node.account.remove("a/b/c/d", allow_fail=True)


# Representation of a somewhat arbitrary directory structure for testing copy functionality
# A key which has a string as its value represents a file
# A key which has a dict as its value represents a subdirectory
DIR_STRUCTURE = {
    "d00": {
        "another_file": b"1\n2\n3\n4\ncats and dogs",
        "d10": {
            "fasdf": b"lasdf;asfd\nahoppoqnbasnb"
        },
        "d11": {
            "f65": b"afasdfsafdsadf"
        }
    },
    "a_file": b"hello world!"
}


def make_dir_structure(base_dir, dir_structure, node=None):
    """Make a file tree starting at base_dir with structure specified by dir_structure.

    if node is None, make the structure locally, else make it on the given node
    """
    for k, v in iteritems(dir_structure):
        if isinstance(v, dict):
            # it's a subdirectory
            subdir_name = k
            subdir_path = os.path.join(base_dir, subdir_name)
            subdir_structure = v

            if node:
                node.account.mkdir(subdir_path)
            else:
                os.mkdir(subdir_path)

            make_dir_structure(subdir_path, subdir_structure, node)
        else:
            # it's a file
            file_name = k
            file_path = os.path.join(base_dir, file_name)
            file_contents = v

            if node:
                with node.account.open(file_path, "wb") as f:
                    f.write(file_contents)
            else:
                with open(file_path, "wb") as f:
                    f.write(file_contents)


def verify_dir_structure(base_dir, dir_structure, node=None):
    """Verify locally or on the given node whether the file subtree at base_dir matches dir_structure."""
    for k, v in iteritems(dir_structure):
        if isinstance(v, dict):
            # it's a subdirectory
            subdir_name = k
            subdir_path = os.path.join(base_dir, subdir_name)
            subdir_structure = v

            if node:
                assert node.account.isdir(subdir_path)
            else:
                assert os.path.isdir(subdir_path)

            verify_dir_structure(subdir_path, subdir_structure, node)
        else:
            # it's a file
            file_name = k
            file_path = os.path.join(base_dir, file_name)
            expected_file_contents = v

            if node:
                with node.account.open(file_path, "r") as f:
                    contents = f.read()
            else:
                with open(file_path, "rb") as f:
                    contents = f.read()
            assert expected_file_contents == contents, contents


class CopyToAndFroTest(Test):
    """These tests check copy_to, and copy_from functionality."""

    def setup(self):
        self.service = GenericService(self.test_context, 1)
        self.node = self.service.nodes[0]
        self.remote_scratch_dir = self.service.worker_scratch_dir

        self.local_temp_dir = tempfile.mkdtemp()

        self.logger.info("local_temp_dir: %s" % self.local_temp_dir)
        self.logger.info("node: %s" % str(self.node.account))

    @cluster(num_nodes=1)
    def test_copy_to_dir_with_rename(self):
        # make dir structure locally
        make_dir_structure(self.local_temp_dir, DIR_STRUCTURE)
        dest = os.path.join(self.remote_scratch_dir, "renamed")
        self.node.account.copy_to(self.local_temp_dir, dest)

        # now validate the directory structure on the remote machine
        verify_dir_structure(dest, DIR_STRUCTURE, node=self.node)

    @cluster(num_nodes=1)
    def test_copy_to_dir_as_subtree(self):
        # copy directory "into" a directory; this should preserve the original directoryname
        make_dir_structure(self.local_temp_dir, DIR_STRUCTURE)
        self.node.account.copy_to(self.local_temp_dir, self.remote_scratch_dir)
        local_temp_dir_name = self.local_temp_dir
        if local_temp_dir_name.endswith(os.path.sep):
            local_temp_dir_name = local_temp_dir_name[:-len(os.path.sep)]

        verify_dir_structure(os.path.join(self.remote_scratch_dir, local_temp_dir_name), DIR_STRUCTURE)

    @cluster(num_nodes=1)
    def test_copy_from_dir_with_rename(self):
        # make dir structure remotely
        make_dir_structure(self.remote_scratch_dir, DIR_STRUCTURE, node=self.node)
        dest = os.path.join(self.local_temp_dir, "renamed")
        self.node.account.copy_from(self.remote_scratch_dir, dest)

        # now validate the directory structure locally
        verify_dir_structure(dest, DIR_STRUCTURE)

    @cluster(num_nodes=1)
    def test_copy_from_dir_as_subtree(self):
        # copy directory "into" a directory; this should preserve the original directoryname
        make_dir_structure(self.remote_scratch_dir, DIR_STRUCTURE, node=self.node)
        self.node.account.copy_from(self.remote_scratch_dir, self.local_temp_dir)

        verify_dir_structure(os.path.join(self.local_temp_dir, "scratch"), DIR_STRUCTURE)

    def teardown(self):
        # allow_fail in case scratch dir was not successfully created
        if os.path.exists(self.local_temp_dir):
            shutil.rmtree(self.local_temp_dir)


class CopyDirectTest(Test):

    def setup(self):
        self.service = GenericService(self.test_context, 2)
        self.src_node, self.dest_node = self.service.nodes
        self.remote_scratch_dir = self.service.worker_scratch_dir

        self.logger.info("src_node: %s" % str(self.src_node.account))
        self.logger.info("dest_node: %s" % str(self.dest_node.account))

    @cluster(num_nodes=2)
    def test_copy_file(self):
        """Verify that a file can be correctly copied directly between nodes.

        This should work with or without the recursive flag.
        """
        file_path = os.path.join(self.remote_scratch_dir, "myfile.txt")
        expected_contents = b"123"
        self.src_node.account.create_file(file_path, expected_contents)

        self.src_node.account.copy_between(file_path, file_path, self.dest_node)

        assert self.dest_node.account.isfile(file_path)
        with self.dest_node.account.open(file_path, "r") as f:
            contents = f.read()
            assert expected_contents == contents

    @cluster(num_nodes=2)
    def test_copy_directory(self):
        """Verify that a directory can be correctly copied directly between nodes.
        """

        make_dir_structure(self.remote_scratch_dir, DIR_STRUCTURE, node=self.src_node)
        self.src_node.account.copy_between(self.remote_scratch_dir, self.remote_scratch_dir, self.dest_node)
        verify_dir_structure(os.path.join(self.remote_scratch_dir, "scratch"), DIR_STRUCTURE, node=self.dest_node)


class TestClusterSpec(Test):
    @cluster(cluster_spec=ClusterSpec.simple_linux(2))
    def test_create_two_node_service(self):
        self.service = GenericService(self.test_context, 2)
        for node in self.service.nodes:
            node.account.ssh("echo hi")

    @cluster(cluster_spec=ClusterSpec.from_nodes(
        [
            NodeSpec(operating_system=WINDOWS),
            NodeSpec(operating_system=LINUX),
            NodeSpec()  # this one is also linux
        ]
    ))
    def three_nodes_test(self):
        self.service = GenericService(self.test_context, 3)
        for node in self.service.nodes:
            node.account.ssh("echo hi")


class RemoteAccountTest(Test):
    def __init__(self, test_context):
        super(RemoteAccountTest, self).__init__(test_context)
        self.account_service = RemoteAccountTestService(test_context)

    def setup(self):
        self.account_service.start()

    @cluster(num_nodes=1)
    def test_flaky(self):
        assert random.choice([True, False, False])

    @cluster(num_nodes=1)
    def test_ssh_capture_combine_stderr(self):
        """Test that ssh_capture correctly captures stderr and stdout from remote process.
        """
        node = self.account_service.nodes[0]

        # swap stdout and stderr in the echo process
        cmd = "for i in $(seq 1 5); do echo $i 3>&1 1>&2 2>&3; done"

        ssh_output = node.account.ssh_capture(cmd, combine_stderr=True)
        bad_ssh_output = node.account.ssh_capture(cmd, combine_stderr=False)  # Same command, but don't capture stderr

        lines = [int(line.strip()) for line in ssh_output]
        assert lines == [i for i in range(1, 6)]
        bad_lines = [int(line.strip()) for line in bad_ssh_output]
        assert bad_lines == []

    @cluster(num_nodes=1)
    def test_ssh_output_combine_stderr(self):
        """Test that ssh_output correctly captures stderr and stdout from remote process.
        """
        node = self.account_service.nodes[0]

        # swap stdout and stderr in the echo process
        cmd = "for i in $(seq 1 5); do echo $i 3>&1 1>&2 2>&3; done"

        ssh_output = node.account.ssh_output(cmd, combine_stderr=True)
        bad_ssh_output = node.account.ssh_output(cmd, combine_stderr=False)  # Same command, but don't capture stderr

        assert ssh_output == b"\n".join([str(i).encode('utf-8') for i in range(1, 6)]) + b"\n", ssh_output
        assert bad_ssh_output == b"", bad_ssh_output

    @cluster(num_nodes=1)
    def test_ssh_capture(self):
        """Test that ssh_capture correctly captures output from ssh subprocess.
        """
        node = self.account_service.nodes[0]
        cmd = "for i in $(seq 1 5); do echo $i; done"
        ssh_output = node.account.ssh_capture(cmd, combine_stderr=False)

        lines = [int(line.strip()) for line in ssh_output]
        assert lines == [i for i in range(1, 6)]

    @cluster(num_nodes=1)
    def test_ssh_output(self):
        """Test that ssh_output correctly captures output from ssh subprocess.
        """
        node = self.account_service.nodes[0]
        cmd = "for i in $(seq 1 5); do echo $i; done"
        ssh_output = node.account.ssh_output(cmd, combine_stderr=False)

        assert ssh_output == b"\n".join([str(i).encode('utf-8') for i in range(1, 6)]) + b"\n", ssh_output

    @cluster(num_nodes=1)
    def test_monitor_log(self):
        """Tests log monitoring by writing to a log in the background thread"""

        node = self.account_service.nodes[0]

        # Make sure we start the log with some data, including the value we're going to grep for
        self.account_service.write_to_log("foo\nbar\nbaz")

        # Background thread that simulates a process writing to the log
        self.wrote_log_line = False

        def background_logging_thread():
            # This needs to be large enough that we can verify we've actually
            # waited some time for the data to be written, but not too long that
            # the test takes a long time
            time.sleep(3)
            self.wrote_log_line = True
            self.account_service.write_to_log("foo\nbar\nbaz")

        with node.account.monitor_log(self.account_service.log_file) as monitor:
            logging_thread = Thread(target=background_logging_thread)
            logging_thread.start()
            monitor.wait_until('foo', timeout_sec=10, err_msg="Never saw expected log")
            assert self.wrote_log_line

        logging_thread.join(5.0)
        if logging_thread.isAlive():
            raise Exception("Timed out waiting for background thread.")

    @cluster(num_nodes=1)
    def test_monitor_log_exception(self):
        """Tests log monitoring correctly throws an exception when the regex was not found"""

        node = self.account_service.nodes[0]

        # Make sure we start the log with some data, including the value we're going to grep for
        self.account_service.write_to_log("foo\nbar\nbaz")

        timeout = 3
        try:
            with node.account.monitor_log(self.account_service.log_file) as monitor:
                start = time.time()
                monitor.wait_until('foo', timeout_sec=timeout, err_msg="Never saw expected log")
                assert False, "Log monitoring should have timed out and thrown an exception"
        except TimeoutError:
            # expected
            end = time.time()
            assert end - start > timeout, "Should have waited full timeout period while monitoring the log"

    @cluster(num_nodes=1)
    def test_kill_process(self):
        """Tests that kill_process correctly works"""

        def get_pids():
            pid_cmd = "ps ax | grep -i nc | grep -v grep | awk '{print $1}'"

            return list(node.account.ssh_capture(pid_cmd, callback=int))

        node = self.account_service.nodes[0]

        # Run TCP service using netcat
        node.account.ssh_capture("nohup nc -l -p 5000 > /dev/null 2>&1 &")

        wait_until(lambda: len(get_pids()) > 0, timeout_sec=10,
                   err_msg="Failed to start process within %d sec" % 10)

        # Kill service.
        node.account.kill_process("nc")

        wait_until(lambda: len(get_pids()) == 0, timeout_sec=10,
                   err_msg="Failed to kill process within %d sec" % 10)


class TestIterWrapper(Test):
    def setup(self):
        self.line_num = 6
        self.eps = 0.01

        self.service = GenericService(self.test_context, num_nodes=1)
        self.node = self.service.nodes[0]

        self.temp_file = "ducktape-test-" + str(random.randint(0, 100000))
        contents = ""
        for i in range(self.line_num):
            contents += "%d\n" % i

        self.node.account.create_file(self.temp_file, contents)

    def test_iter_wrapper(self):
        """Test has_next functionality on the returned iterable item."""
        output = self.node.account.ssh_capture("cat " + self.temp_file)
        for i in range(self.line_num):
            assert output.has_next()  # with timeout in case of hang
            assert output.next().strip() == str(i)
        start = time.time()
        assert output.has_next() is False
        stop = time.time()
        assert stop - start < self.eps, "has_next() should return immediately"

    def test_iter_wrapper_timeout(self):
        """Test has_next with timeout"""
        output = self.node.account.ssh_capture("tail -F " + self.temp_file)
        # allow command to be executed before we check output with timeout_sec = 0
        time.sleep(.5)
        for i in range(self.line_num):
            assert output.has_next(timeout_sec=0)
            assert output.next().strip() == str(i)

        timeout = .25
        start = time.time()
        # This check will last for the duration of the timeout because the the remote tail -F process
        # remains running, and the output stream is not closed.
        assert output.has_next(timeout_sec=timeout) is False
        stop = time.time()
        assert (stop - start >= timeout) and (stop - start) < timeout + self.eps, \
            "has_next() should return right after %s second" % str(timeout)

    def teardown(self):
        # tail -F call above will leave stray processes, so clean up
        cmd = "for p in $(ps ax | grep -v grep | grep \"%s\" | awk '{print $1}'); do kill $p; done" % self.temp_file
        self.node.account.ssh(cmd, allow_fail=True)

        self.node.account.ssh("rm -f " + self.temp_file, allow_fail=True)


class RemoteAccountCompressedTest(Test):
    def __init__(self, test_context):
        super(RemoteAccountCompressedTest, self).__init__(test_context)
        self.account_service = RemoteAccountTestService(test_context)
        self.test_context.session_context.compress = True
        self.tar_msg = False
        self.tar_error = False

    def setup(self):
        self.account_service.start()

    @cluster(num_nodes=1)
    def test_log_compression_with_non_existent_files(self):
        """Test that log compression with tar works even when a specific log file has not been generated
        (e.g. heap dump)
        """
        self.test_context.logger.addFilter(CompressionErrorFilter(self))
        self.copy_service_logs(None)

        if not self.tar_msg:
            raise Exception("Never saw attempt to compress log")
        if self.tar_error:
            raise Exception("Failure when compressing logs")


class CompressionErrorFilter(logging.Filter):

    def __init__(self, test):
        super(CompressionErrorFilter, self).__init__()
        self.test = test

    def filter(self, record):
        if 'tar czf' in record.msg:
            self.test.tar_msg = True
            if 'Error' in record.msg:
                self.test.tar_error = True
        return True
