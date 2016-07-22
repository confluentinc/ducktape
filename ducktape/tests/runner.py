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
import multiprocessing
import time
import traceback
import zmq

from ducktape.tests.serde import SerDe
from ducktape.command_line.defaults import ConsoleDefaults
from ducktape.tests.runner_client import run_client
from ducktape.tests.result import TestResults
from ducktape.utils.terminal_size import get_terminal_size
from ducktape.tests.event import ClientEventFactory, EventResponseFactory
from ducktape.cluster.finite_subcluster import FiniteSubcluster


class Receiver(object):
    def __init__(self, port):
        self.port = port
        self.serde = SerDe()

        self.zmq_context = zmq.Context()
        self.socket = self.zmq_context.socket(zmq.REP)
        self.socket.bind("tcp://*:%s" % str(self.port))

    def recv(self):
        message = self.socket.recv()
        return self.serde.deserialize(message)

    def send(self, event):
        self.socket.send(self.serde.serialize(event))


class TestScheduler(object):
    def __init__(self, test_context_list, runner):
        self.runner = runner
        self.test_context_list = test_context_list
        self._sort_test_context_list()

    def __len__(self):
        return len(self.test_context_list)

    def _sort_test_context_list(self):
        """Replace self.test_context_list with a sorted shallow copy

        Sort from largest cluster users to smallest
        """
        # sort from largest cluster users to smallest
        self.test_context_list = sorted(self.test_context_list,
                                        key=lambda tc: self.runner.expected_num_nodes(tc),
                                        reverse=True)

    def __iter__(self):
        """This class is iterable"""
        return self

    def peek(self):
        """Locate and return the next object to be scheduled, without removing it internally.

        :return test_context for the next test to be scheduled
        :raise RuntimeError if the scheduler is empty
        """
        if len(self) == 0:
            raise RuntimeError("No more tests available")

        for tc in self.test_context_list:
            if self.runner.expected_num_nodes(tc) <= self.runner.cluster.num_available_nodes():
                return tc

        return None

    def next(self):
        """Get the next test"""
        tc = self.peek()
        self.test_context_list.remove(tc)
        return tc

    def add(self, test_context):
        """Enqueue another test"""
        self.test_context_list.append(test_context)
        self._sort_test_context_list()


class TestRunner(object):
    """Runs tests serially."""

    # When set to True, the test runner will finish running/cleaning the current test, but it will not run any more
    stop_testing = False

    def __init__(self, cluster, session_context, session_logger, tests, port=ConsoleDefaults.TEST_DRIVER_PORT):
        # session_logger, message logger,
        self.session_logger = session_logger
        self.cluster = cluster
        self.event_response = EventResponseFactory()
        self.hostname = "localhost"
        self.port = port
        self.receiver = Receiver(port)

        self.session_context = session_context
        self.max_parallel = session_context.max_parallel
        self.results = TestResults(self.session_context)

        self.proc_list = []
        self.scheduler = TestScheduler(tests, self)
        self._test_context = {t.test_id: t for t in tests}
        self._test_cluster = {}  # Track subcluster assigned to a particular test_id
        self.active_tests = {}
        self.finished_tests = {}

    def who_am_i(self):
        """Human-readable name helpful for logging."""
        return self.__class__.__name__

    def run_all_tests(self):
        self.results.start_time = time.time()
        self._log(logging.INFO, "starting test run with session id %s..." % self.session_context.session_id)
        self._log(logging.INFO, "running %d tests..." % len(self.scheduler))

        while len(self.scheduler) > 0 or len(self.active_tests) > 0:

            while len(self.active_tests) <= self.max_parallel and len(self.scheduler) > 0 and self.scheduler.peek() is not None:
                next_test_context = self.scheduler.next()
                self._preallocate_subcluster(next_test_context)
                self._run_single_test(next_test_context)

            try:
                event = self.receiver.recv()
                self._handle(event)
            except Exception as e:
                err_str = "Exception receiving message: %s: %s" % (str(type(e)), str(e))
                err_str += "\n" + traceback.format_exc(limit=16)
                self._log(logging.ERROR, err_str)
                continue

        for proc in self.proc_list:
            proc.join()

        return self.results

    def expected_num_nodes(self, test_context):
        """Helper method for deciding how many nodes we expect the given test to use."""
        expected = test_context.expected_num_nodes
        if expected is None:
            # If there is no information on cluster usage, allocate entire cluster
            if self.session_context.max_parallel > 1:
                self._log(logging.WARNING,
                          "Test %s has no cluster use metadata, so this test will not run in parallel with any others."
                          % test_context.test_id)
            return len(self.cluster)
        else:
            return expected

    def _run_single_test(self, test_context):
        """Start a test runner client in a subprocess"""
        # Test is considered "active" as soon as we start it up in a subprocess
        self.active_tests[test_context.test_id] = True

        proc = multiprocessing.Process(
            target=run_client,
            args=[
                self.hostname,
                self.port,
                test_context.test_id,
                test_context.logger_name,
                test_context.results_dir,
                self.session_context.debug
            ])
        self.proc_list.append(proc)
        proc.start()

    def _preallocate_subcluster(self, test_context):
        """Preallocate the subcluster which will be used to run the test.

        Side effect: store association between the test_id and the preallocated subcluster.

        :param test_context
        :return None
        """
        expected = self.expected_num_nodes(test_context)
        self._test_cluster[test_context.test_id] = FiniteSubcluster(self.cluster.alloc(expected))

    def _handle(self, event):
        self._log(logging.DEBUG, str(event))

        if event["event_type"] == ClientEventFactory.READY:
            self._handle_ready(event)
        elif event["event_type"] in [ClientEventFactory.RUNNING, ClientEventFactory.SETTING_UP, ClientEventFactory.TEARING_DOWN]:
            self._handle_lifecycle(event)
        elif event["event_type"] == ClientEventFactory.FINISHED:
            self._handle_finished(event)
        elif event["event_type"] == ClientEventFactory.LOG:
            self._handle_log(event)
        else:
            raise RuntimeError("Received event with unknown event type: " + str(event))

    def _handle_ready(self, event):
        test_id = event["test_id"]
        test_context = self._test_context[test_id]
        subcluster = self._test_cluster[test_id]

        self.receiver.send(
                self.event_response.ready(event, self.session_context, test_context, subcluster))

    def _handle_log(self, event):
        self.receiver.send(self.event_response.log(event))
        self._log(event["log_level"], event["message"])

    def _handle_finished(self, event):
        self.receiver.send(self.event_response.finished(event))

        # Transition this test from running to finished
        test_id = event["test_id"]
        del self.active_tests[test_id]
        self.finished_tests[test_id] = event
        self.results.append(event['result'])

        # Free nodes used by the test
        subcluster = self._test_cluster[test_id]
        self.cluster.free(subcluster.alloc(len(subcluster)))
        del self._test_cluster[test_id]

        if len(self.scheduler) + len(self.active_tests) > 0 and self.session_context.max_parallel == 1:
            terminal_width, y = get_terminal_size()
            self._log(logging.INFO, "~" * int(2 * terminal_width / 3))

    def _handle_lifecycle(self, event):
        self.receiver.send(self.event_response._event_response(event))

    def _log(self, log_level, msg, *args, **kwargs):
        """Log to the service log of the current test."""
        self.session_logger.log(log_level, msg, *args, **kwargs)
