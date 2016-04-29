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

from ducktape.services.background_thread import BackgroundThreadService
from ducktape.errors import TimeoutError
from tests.ducktape_mock import test_context, MockNode
import time


class DummyService(BackgroundThreadService):
    """Single node service that sleeps for self.run_time_sec seconds in a background thread."""

    def __init__(self, context, run_time_sec):
        super(DummyService, self).__init__(context, 1)
        self.running = False
        self.run_time_sec = run_time_sec

    def who_am_i(self, node=None):
        return "DummyService"

    def idx(self, node):
        return 1

    def allocate_nodes(self):
        self.nodes = [MockNode()]

    def _worker(self, idx, node):
        self.running = True

        end = time.time() + self.run_time_sec
        while self.running:
            time.sleep(.1)
            if time.time() > end:
                self.running = False
                break

    def stop_node(self, node):
        self.running = False


class CheckBackgroundThreadService(object):

    def setup_method(self, method):
        self.context = test_context()

    def check_service_timeout(self):
        """Test that wait(timeout_sec) raise a TimeoutError in approximately the expected time."""
        self.service = DummyService(self.context, float('inf'))
        self.service.start()
        start = time.time()
        timeout_sec = .1
        try:
            self.service.wait(timeout_sec=timeout_sec)
            raise Exception("Expected service to timeout.")
        except TimeoutError:
            end = time.time()
            # Relative difference should be pretty small
            # within 10% should be reasonable
            actual_timeout = end - start
            relative_difference = abs(timeout_sec - actual_timeout) / timeout_sec
            assert relative_difference < .1, \
                "Correctly threw timeout error, but timeout doesn't match closely with expected timeout. " + \
                "(expected timeout, actual timeout): (%s, %s)" % (str(timeout_sec), str(actual_timeout))

    def check_no_timeout(self):
        """Run an instance of DummyService with a short run_time_sec. It should stop without
        timing out."""

        self.service = DummyService(self.context, run_time_sec=.1)
        self.service.start()
        self.service.wait(timeout_sec=.5)

    def check_wait_node(self):
        self.service = DummyService(self.context, run_time_sec=float('inf'))
        self.service.start()
        node = self.service.nodes[0]
        assert not(self.service.wait_node(node, timeout_sec=.1))
        self.service.stop_node(node)
        assert self.service.wait_node(node)
