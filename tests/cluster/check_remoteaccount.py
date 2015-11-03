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

from ducktape.errors import TimeoutError
from tests.ducktape_mock import MockAccount
from tests.test_utils import find_available_port

from threading import Thread
import SimpleHTTPServer
import SocketServer
import threading
import time
import random

class SimpleServer(object):
    """Helper class which starts a simple server listening on localhost at the specified port
    """
    def __init__(self):
        self.port = find_available_port()
        self.handler = SimpleHTTPServer.SimpleHTTPRequestHandler
        self.httpd = SocketServer.TCPServer(("", self.port), self.handler)
        self.close_signal = threading.Event()
        self.server_started = False

    def start(self, delay_sec=0.0):
        """Run the server after specified delay"""
        def run():
            self.close_signal.wait(delay_sec)

            if not self.close_signal.is_set():
                self.server_started = True
                self.httpd.serve_forever()

        self.background_thread = Thread(target=run)
        self.background_thread.start()

    def stop(self):
        self.close_signal.set()

        if self.server_started:
            self.httpd.shutdown()
        self.background_thread.join(timeout=.5)
        if self.background_thread.is_alive():
            raise Exception("SimpleServer failed to stop quickly")



class CheckIterWrapper(object):
    def setup(self):
        self.line_num = 6
        self.eps = 0.01
        self.account = MockAccount()
        self.account.ssh("mkdir -p /tmp")
        self.temp_file = "/tmp/ducktape-test-" + str(random.randint(0, 100000))
        for i in range(self.line_num):
            self.account.ssh("echo " + str(i) + " >> " + self.temp_file)

    def check_iter_wrapper(self):
        output = self.account.ssh_capture("tail " + self.temp_file)
        for i in range(self.line_num):
            assert output.has_next()
            assert output.next().strip() == str(i)
        start = time.time()
        assert output.has_next() == False
        stop = time.time()
        assert stop - start < self.eps, "has_next() should return immediately"

    def check_iter_wrapper_timeout(self):
        output = self.account.ssh_capture("tail -F " + self.temp_file)
        # allow command to be executed before we check output with timeout_sec = 0
        time.sleep(1)
        for i in range(self.line_num):
            assert output.has_next(timeout_sec=0)
            assert output.next().strip() == str(i)
        start = time.time()
        assert output.has_next(timeout_sec=5) == False
        stop = time.time()
        assert (stop - start >= 5) and (stop - start) < 5 + self.eps, "has_next() should return right after 5 seconds"

    def teardown(self):
        self.account.ssh("rm -f " + self.temp_file)

class CheckRemoteAccount(object):
    def setup(self):
        self.server = SimpleServer()
        self.account = MockAccount()

    def check_wait_for_http(self):
        """Check waiting without timeout"""
        self.server.start(delay_sec=0.0)
        self.account.wait_for_http_service(port=self.server.port, headers={}, timeout=10, path="/")

    def check_wait_for_http_timeout(self):
        """Check waiting with timeout"""

        timeout = 1
        start = time.time()
        self.server.start(delay_sec=5)

        try:
            self.account.wait_for_http_service(port=self.server.port, headers={}, timeout=timeout, path='/')
            raise Exception("Should have timed out waiting for server to start")
        except TimeoutError:
            # expected behavior. Now check that we're reasonably close to the expected timeout
            # This is a fairly loose check since there are various internal timeouts that can affect the overall
            # timing
            actual_timeout = time.time() - start
            assert abs(actual_timeout - timeout) / timeout < 1

    def teardown(self):
        self.server.stop()

