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
import os
import time
import traceback
import zmq

from ducktape.tests.serde import SerDe
from ducktape.command_line.defaults import ConsoleDefaults
from ducktape.tests.runner_client import run_client
from ducktape.tests.result import TestResults
from ducktape.utils.terminal_size import get_terminal_size
from ducktape.tests.event import ClientEventSupplier, EventResponseSupplier


class Receiver(object):
    def __init__(self, port):
        self.port = port
        self.serde = SerDe()

        self.zmq_context = zmq.Context()
        self.socket = self.zmq_context.socket(zmq.REP)
        self.socket.bind("tcp://*:%s" % str(self.port))
        # self.poller = zmq.Poller()
        # self.poller.register(self.socket, zmq.POLLIN)

    def recv(self):
        message = self.socket.recv()
        return self.serde.deserialize(message)

    def send(self, event):
        self.socket.send(self.serde.serialize(event))


class TestRunner(object):
    """Runs tests serially."""

    # When set to True, the test runner will finish running/cleaning the current test, but it will not run any more
    stop_testing = False

    def __init__(self, cluster, session_context, session_logger, tests, port=ConsoleDefaults.TEST_DRIVER_PORT):
        # session_logger, message logger,
        self.session_logger = session_logger
        self.cluster = cluster
        self.event_response = EventResponseSupplier()
        self.hostname = "localhost"
        self.port = port
        self.receiver = Receiver(port)

        self.session_context = session_context
        self.results = TestResults(self.session_context)

        self.proc_list = []
        self.staged_tests = tests
        self.active_tests = {}
        self.finished_tests = {}

        self.current_test_context = None

    def who_am_i(self):
        """Human-readable name helpful for logging."""
        return self.__class__.__name__

    def run_all_tests(self):
        self.results.start_time = time.time()
        self.log(logging.INFO, "starting test run with session id %s..." % self.session_context.session_id)
        self.log(logging.INFO, "running %d tests..." % len(self.staged_tests))

        while len(self.staged_tests) > 0 or len(self.active_tests) > 0:

            if len(self.active_tests) == 0:
                self.current_test_context = self.staged_tests.pop()
                self.run_single_test()

            try:
                event = self.receiver.recv()
                self.handle(event)
            except Exception as e:
                err_str = "Exception receiving message: %s: %s" % (str(type(e)), str(e))
                err_str += "\n" + traceback.format_exc(limit=16)
                self.log(logging.ERROR, err_str)
                continue

        for proc in self.proc_list:
            proc.join()

        return self.results

    def run_single_test(self):
        """Start a test runner client in a subprocess"""
        proc = multiprocessing.Process(
            target=run_client,
            args=[
                self.hostname,
                self.port,
                self.current_test_context.test_id,
                self.current_test_context.logger_name,
                self.current_test_context.results_dir,
                self.session_context.debug,
                self.session_context.max_parallel
            ])
        self.proc_list.append(proc)
        proc.start()

    def handle(self, event):
        self.log(logging.DEBUG, str(event))

        if event["event_type"] == ClientEventSupplier.READY:
            self.handle_ready(event)
        elif event["event_type"] in [ClientEventSupplier.RUNNING, ClientEventSupplier.SETTING_UP, ClientEventSupplier.TEARING_DOWN]:
            self.handle_lifecycle(event)
        elif event["event_type"] == ClientEventSupplier.FINISHED:
            self.handle_finished(event)
        elif event["event_type"] == ClientEventSupplier.LOG:
            self.handle_log(event)
        else:
            raise RuntimeError("Received event with unknown event type: " + str(event))

    def handle_ready(self, event):
        self.receiver.send(
                self.event_response.ready(event, self.session_context, self.current_test_context, self.cluster))

        # Test is considered "active" as soon as we receive the ready request
        self.active_tests[event["test_id"]] = event

    def handle_log(self, event):
        self.receiver.send(self.event_response.log(event))
        self.log(event["log_level"], event["message"])

    def handle_finished(self, event):
        self.receiver.send(self.event_response.finished(event))

        # Move this test from running to finished
        del self.active_tests[event["test_id"]]
        self.finished_tests[event["test_id"]] = event
        self.results.append(event['result'])

        if len(self.staged_tests) + len(self.active_tests) > 0 and self.session_context.max_parallel == 1:
            terminal_width, y = get_terminal_size()
            print "~" * int(2 * terminal_width / 3)

    def handle_lifecycle(self, event):
        self.receiver.send(self.event_response.event_response(event))

    def log_event(self, log_level, event):
        """Log an event received from the client."""
        pass

    def log(self, log_level, msg):
        """Log to the service log of the current test."""
        self.session_logger.log(log_level, msg)
