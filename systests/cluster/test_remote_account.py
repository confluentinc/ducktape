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

from ducktape.services.service import Service
from ducktape.tests.test import Test
from ducktape.errors import TimeoutError
from ducktape.utils.util import wait_until

import os
from threading import Thread
import time

def generate_tempdir_name():
    """Use this ad-hoc function instead of the tempfile module since we're creating and removing
    this directory with ssh commands.
    """
    return "/tmp/" + "t" + str(int(time.time()))

class RemoteAccountTestService(Service):
    """Simple service that allocates one node for performing tests of RemoteAccount functionality"""

    def __init__(self, context):
        super(RemoteAccountTestService, self).__init__(context, 1)
        self.temp_dir = generate_tempdir_name()

    @property
    def log_file(self):
        return os.path.join(self.temp_dir, "test.log")

    def start_node(self, node):
        node.account.ssh("mkdir -p " + self.temp_dir)

    def stop_node(self, node):
        pass

    def clean_node(self, node):
        node.account.ssh("rm -rf " + self.temp_dir)

    def write_to_log(self, msg):
        self.nodes[0].account.ssh("echo -e -n " + repr(msg) + " >> " + self.log_file)

class RemoteAccountTest(Test):

    def __init__(self, test_context):
        super(RemoteAccountTest, self).__init__(test_context)
        self.account_service = RemoteAccountTestService(test_context)

    def setUp(self):
        self.account_service.start()

    def test_ssh_capture(self):
        """Test that ssh_capture correctly captures output from ssh subprocess.
        Also, verify that the ssh command is run before the user iterates through
        the generator returned by ssh_capture.
        """
        node = self.account_service.nodes[0]
        hello_file = os.path.join(self.account_service.temp_dir, "hello.txt")

        assert node.account.ssh("test -f %s" % hello_file, allow_fail=True) != 0
        cmd = "touch %s; for i in $(seq 1 5); do echo $i; done" % hello_file
        ssh_output = node.account.ssh_capture(cmd)

        # Here we check that the remote command gets run before we iterate through the output from the process
        # Use wait_until because it can a little time for the subprocess to get scheduled and run
        wait_until(lambda: node.account.ssh("test -f %s" % hello_file, allow_fail=True) == 0,
            timeout_sec=2, err_msg="Timed out waiting for %s to be created." % hello_file)

        lines = [int(l.strip()) for l in ssh_output]
        assert lines == [i for i in range(1, 6)]

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
