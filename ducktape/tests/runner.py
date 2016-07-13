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
import zmq

from ducktape.tests.serde import SerDe
from ducktape.tests.session import SessionLogger
from ducktape.command_line.defaults import ConsoleDefaults
from ducktape.tests.runner_client import run_client
from ducktape.tests.result import TestResults
from ducktape.utils.terminal_size import get_terminal_size
from ducktape.tests.message import Request, ready_reply
from ducktape.tests.logger import Logger


class RequestLogger(Logger):
    def __init__(self, session_context):
        self.session_context = session_context

    @property
    def logger_name(self):
        # Naming means RequestLogger.logger is a child of SessionLogger.logger
        # I.e. requests
        return "%s.requestlogger" % SessionLogger(self.session_context).logger_name

    def configure_logger(self):
        """Log to request_log."""
        if self.configured:
            return

        self._logger.setLevel(logging.DEBUG)

        fh_debug = logging.FileHandler(os.path.join(self.session_context.results_dir, "request_log.debug"))
        fh_debug.setLevel(logging.DEBUG)

        # create formatter and add it to the handlers
        formatter = logging.Formatter(ConsoleDefaults.SESSION_LOG_FORMATTER)
        fh_debug.setFormatter(formatter)

        # add the handlers to the logger
        self._logger.addHandler(fh_debug)


class TestRunner(object):
    """Runs tests serially."""

    # When set to True, the test runner will finish running/cleaning the current test, but it will not run any more
    stop_testing = False

    def __init__(self, cluster, session_context, session_logger, tests, port=ConsoleDefaults.TEST_DRIVER_PORT):
        # session_logger, message logger,
        self.session_logger = session_logger
        self.request_logger = RequestLogger(session_context).logger
        self.cluster = cluster
        self.serde = SerDe()
        self.port = port

        self.zmq_context = zmq.Context()
        self.socket = self.zmq_context.socket(zmq.REP)
        self.socket.bind("tcp://*:%s" % str(self.port))
        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN)

        self.session_context = session_context
        self.results = TestResults(self.session_context)

        self.proc_list = []
        self.staged_tests = tests
        self.active_tests = {}
        self.finished_tests = {}

        self.current_test = None
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
                message = self.socket.recv()
                event = self.serde.deserialize(message)
                self.handle(event)
            except Exception as e:
                s = str(type(e))
                s += " " + str(e)
                print "Exception receiving message:", str(e)
                continue

        for proc in self.proc_list:
            proc.join()

        return self.results

    def run_single_test(self):
        proc = multiprocessing.Process(
            target=run_client,
            args=[
                self.port,
                self.current_test_context.logger_name,
                self.current_test_context.results_dir,
                self.session_context.debug,
                self.session_context.max_parallel
            ])
        self.proc_list.append(proc)
        proc.start()

    def handle(self, event):
        self.request_logger.debug(event)

        if event["event_type"] == Request.READY:
            self.handle_ready(event)
        elif event["event_type"] in [Request.RUNNING, Request.SETTING_UP, Request.TEARING_DOWN]:
            self.handle_lifecycle(event)
        elif event["event_type"] == Request.FINISHED:
            self.handle_finished(event)
        elif event["event_type"] == Request.LOG:
            self.handle_log(event)
        else:
            self.handle_other(event)

    def handle_ready(self, event):
        self.socket.send(
            self.serde.serialize(ready_reply(self.session_context, self.current_test_context, self.cluster)))

        # Test is "active" once we receive the ready request
        self.active_tests[event["source_id"]] = event["event_type"]

    def handle_log(self, event):
        self.log(event["log_level"], event["message"])
        self.socket.send(self.serde.serialize(event))

    def handle_finished(self, event):
        self.socket.send(self.serde.serialize(event))

        # Move this test from running to finished
        del self.active_tests[event["source_id"]]
        self.finished_tests[event["source_id"]] = event["event_type"]
        self.results.append(event['result'])

        if len(self.staged_tests) + len(self.active_tests) > 0 and self.session_context.max_parallel == 1:
            terminal_width, y = get_terminal_size()
            print "~" * int(2 * terminal_width / 3)

    def handle_lifecycle(self, event):
        self.socket.send(self.serde.serialize(event))

    def handle_other(self, event):
        self.socket.send(self.serde.serialize(event))

    def log(self, log_level, msg):
        """Log to the service log and the test log of the current test."""

        if self.current_test is None:
            msg = "%s: %s" % (self.who_am_i(), str(msg))
            self.session_logger.log(log_level, msg)
        else:
            msg = "%s: %s: %s" % (self.who_am_i(), self.current_test_context.test_name, str(msg))
            self.session_logger.log(log_level, msg)
            self.current_test.logger.log(log_level, msg)






