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
from ducktape.cluster.remoteaccount import RemoteAccount
from ducktape.cluster.remoteaccount import RemoteAccountSSHConfig
import pytest

import logging
from threading import Thread
from http.server import SimpleHTTPRequestHandler
import socketserver
import threading
import time


class DummyException(Exception):
    pass


def raise_error_checker(error, remote_account):
    raise DummyException("dummy raise: {}\nfrom: {}".format(error, remote_account))


def raise_no_error_checker(error, remote_account):
    pass


class SimpleServer(object):
    """Helper class which starts a simple server listening on localhost at the specified port
    """

    def __init__(self):
        self.port = find_available_port()
        self.handler = SimpleHTTPRequestHandler
        self.httpd = socketserver.TCPServer(("", self.port), self.handler)
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

    @pytest.mark.parametrize("checkers", [[raise_error_checker],
                                          [raise_no_error_checker, raise_error_checker],
                                          [raise_error_checker, raise_no_error_checker]])
    def check_ssh_checker(self, checkers):
        self.server.start()
        ssh_config = RemoteAccountSSHConfig.from_string(
            """
        Host dummy_host.com
            Hostname dummy_host.name.com
            Port 22
            User dummy
            ConnectTimeout 1
        """)
        self.account = RemoteAccount(ssh_config, ssh_exception_checks=checkers)
        with pytest.raises(DummyException):
            self.account.ssh('echo test')

    def teardown(self):
        self.server.stop()


class CheckRemoteAccountEquality(object):

    def check_remote_account_equality(self):
        """Different instances of remote account initialized with the same parameters should be equal."""

        ssh_config = RemoteAccountSSHConfig(host="thehost", hostname="localhost", port=22)

        kwargs = {
            "ssh_config": ssh_config,
            "externally_routable_ip": "345",
            "logger": logging.getLogger(__name__)
        }
        r1 = RemoteAccount(**kwargs)
        r2 = RemoteAccount(**kwargs)

        assert r1 == r2
