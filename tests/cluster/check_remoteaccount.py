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

from threading import Thread

import SimpleHTTPServer
import socket
import SocketServer
import threading
import time


def find_available_port(min_port=8000, max_port=9000):
    """Return first available port in the range [min_port, max_port], inclusive."""
    for p in range(min_port, max_port + 1):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind(("localhost", p))
            s.close()
            return p
        except socket.error:
            pass

    raise Exception("No available port found in range [%d, %d]" % (min_port, max_port))


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
            end_delay = time.time() + delay_sec
            while time.time() < end_delay and not self.close_signal.is_set():
                self.close_signal.wait(end_delay - time.time())

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


class CheckRemoteAccount(object):
    def setup_method(self, method):
        self.server = SimpleServer()
        self.account = MockAccount()

    def check_wait_for_http(self):
        """Check waiting without timeout"""
        self.server.start(delay_sec=0.0)
        self.account.wait_for_http_service(port=self.server.port, headers={}, timeout=10, path="/")

    def check_wait_for_http_timeout(self):
        """Check waiting with timeout"""

        timeout = .25
        start = time.time()
        self.server.start(delay_sec=5)

        try:
            self.account.wait_for_http_service(port=self.server.port, headers={}, timeout=timeout, path='/')
            raise Exception("Should have timed out waiting for server to start")
        except TimeoutError:
            # expected behavior
            actual_timeout = time.time() - start

            assert abs(actual_timeout - timeout) / timeout < .5
            pass

    def teardown_method(self, method):
        self.server.stop()