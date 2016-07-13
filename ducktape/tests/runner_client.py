# Copyright 2016 Confluent Inc.
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
import time
import traceback
import zmq

from ducktape.command_line.defaults import ConsoleDefaults
from ducktape.tests.message import Request
from ducktape.tests.loader import TestLoader
from ducktape.tests.serde import SerDe
from ducktape.tests.test import TestLogger

from ducktape.tests.result import TestResult, IGNORE, PASS, FAIL
from ducktape.tests.reporter import SingleResultFileReporter
from ducktape.utils.local_filesystem_utils import mkdir_p


def run_client(logger_name, log_dir, debug, max_parallel):
    client = RunnerClient(logger_name, log_dir, debug, max_parallel)
    client.run()


class RunnerClient(object):
    """Run a single test"""

    def __init__(self, logger_name, log_dir, debug, max_parallel):
        self.serde = SerDe()
        self.logger = TestLogger(logger_name, log_dir, debug, max_parallel).logger

        self.id = "test-runner-%d-%d" % (os.getpid(), id(self))
        self.request = Request(self.id)
        self.sender = Sender("localhost", str(ConsoleDefaults.TEST_DRIVER_PORT), self.logger)

        ready_reply = self.sender.send(self.request.ready())
        self.session_context = ready_reply["session_context"]
        self.test_metadata = ready_reply["test_metadata"]
        self.cluster = ready_reply["cluster"]

        self.send(self.request.log("Loading test %s" % str(self.test_metadata), logging.INFO))
        self.test_context = self.collect_test_context(**self.test_metadata)

        # Wait to instantiate the test object until running the test
        self.test = None

    def send(self, event):
        return self.sender.send(event)

    def collect_test_context(self, directory, file_name, cls_name, method_name, injected_args):
        # TODO - different logger for TestLoader object
        loader = TestLoader(self.session_context, self.logger, injected_args=injected_args)
        loaded_context_list = loader._discover(directory, file_name, cls_name, method_name)

        assert len(loaded_context_list) == 1
        test_context = loaded_context_list[0]
        test_context.cluster = self.cluster
        return test_context

    def run(self):
        self.send(self.request.running())
        if len(self.cluster) != self.cluster.num_available_nodes():
            # Sanity check - are we leaking cluster nodes?
            raise RuntimeError(
                "Expected all nodes to be available. Instead, %d of %d are available" %
                (self.cluster.num_available_nodes(), len(self.cluster)))

        if self.test_context.ignore:
            # Skip running this test, but keep track of the fact that we ignored it
            result = TestResult(self.test_context,
                                self.session_context,
                                test_status=IGNORE,
                                start_time=time.time(),
                                stop_time=time.time())
            # Tell the server we are finished
            self.send(self.request.finished(result=result))
            return

        # Results from this test, as well as logs will be dumped here
        mkdir_p(self.test_context.results_dir)

        start_time = -1
        stop_time = -1
        test_status = PASS
        summary = ""
        data = None

        try:
            # Instantiate test
            self.test = self.test_context.cls(self.test_context)

            # Run the test unit
            start_time = time.time()
            self.log(logging.INFO, "setting up")
            self.setup_test()

            self.log(logging.INFO, "running")
            data = self.run_test()
            test_status = PASS
            self.log(logging.INFO, "PASS")

        except BaseException as e:
            err_trace = str(e.message) + "\n" + traceback.format_exc(limit=16)
            self.log(logging.INFO, "FAIL: " + err_trace)

            test_status = FAIL
            summary += err_trace

        finally:
            self.teardown_test(teardown_services=not self.session_context.no_teardown)
            stop_time = time.time()

            result = TestResult(
                self.test_context,
                self.session_context,
                test_status,
                summary,
                data,
                start_time,
                stop_time)

            self.log(logging.INFO, "Summary: %s" % str(result.summary))
            self.log(logging.INFO, "Data: %s" % str(result.data))

            test_reporter = SingleResultFileReporter(result)
            test_reporter.report()

        # Tell the server we are finished
        try:
            self.send(self.request.finished(result=result))
        except Exception as e:
            self.logger.error("Problem sending FINISHED message:", str(e))

    def setup_test(self):
        """start services etc"""
        self.send(self.request.log("Setting up test", logging.INFO))
        self.test.setUp()

    def run_test(self):
        """Run the test!

        We expect test_context.function to be a function or unbound method which takes an
        instantiated test object as its argument.
        """
        return self.test_context.function(self.test)

    def teardown_test(self, teardown_services=True):
        """teardown method which stops services, gathers log data, removes persistent state, and releases cluster nodes.

        Catch all exceptions so that every step in the teardown process is tried, but signal that the test runner
        should stop if a keyboard interrupt is caught.
        """
        if teardown_services:
            self.log(logging.INFO, "tearing down")

        exceptions = []
        if hasattr(self.test_context, 'services'):
            services = self.test_context.services

            # stop services
            if teardown_services:
                try:
                    services.stop_all()
                except BaseException as e:
                    exceptions.append(e)
                    self.log(logging.WARN, "Error stopping services: %s" % e.message + "\n" + traceback.format_exc(limit=16))

            # always collect service logs
            try:
                self.test.copy_service_logs()
            except BaseException as e:
                exceptions.append(e)
                self.log(logging.WARN, "Error copying service logs: %s" % e.message + "\n" + traceback.format_exc(limit=16))

            # clean up stray processes and persistent state
            if teardown_services:
                try:
                    services.clean_all()
                except BaseException as e:
                    exceptions.append(e)
                    self.log(logging.WARN, "Error cleaning services: %s" % e.message + "\n" + traceback.format_exc(limit=16))

        try:
            self.test.free_nodes()
        except BaseException as e:
            exceptions.append(e)
            self.log(logging.WARN, "Error freeing nodes: %s" % e.message + "\n" + traceback.format_exc(limit=16))

        # Remove reference to services. This is important to prevent potential memory leaks if users write services
        # which themselves have references to large memory-intensive objects
        del self.test_context.services

    def log(self, log_level, msg):
        """Log to the service log and the test log of the current test."""
        if self.test is None:
            msg = "%s: %s" % (str(self), str(msg))
            self.logger.log(log_level, msg)
        else:
            msg = "%s: %s: %s" % (str(self), self.test_context.test_name, str(msg))
            self.logger.log(log_level, msg)
            self.test.logger.log(log_level, msg)


class Sender(object):
    REQUEST_TIMEOUT_MS = 3000
    NUM_RETRIES = 5

    def __init__(self, server_host, server_port, logger):
        self.serde = SerDe()
        self.server_endpoint = "tcp://%s:%s" % (str(server_host), str(server_port))
        self.zmq_context = zmq.Context()
        self.socket = None
        self.poller = zmq.Poller()

        self.logger = logger

        self._init_socket()

    def _init_socket(self):
        self.socket = self.zmq_context.socket(zmq.REQ)
        self.socket.connect(self.server_endpoint)
        self.poller.register(self.socket, zmq.POLLIN)

    def send(self, event, blocking=True):

        retries_left = Sender.NUM_RETRIES
        serialized_event = self.serde.serialize(event)

        while retries_left > 0:
            print "client: sending event:", str(event)
            self.socket.send(serialized_event)
            retries_left -= 1
            waiting_for_reply = True

            while waiting_for_reply:
                self.logger.debug("polling for response")
                sockets = dict(self.poller.poll(Sender.REQUEST_TIMEOUT_MS))

                if sockets.get(self.socket) == zmq.POLLIN:
                    self.logger.debug("POLLIN")
                    reply = self.socket.recv()
                    if reply:
                        self.logger.debug("received", self.serde.deserialize(reply))
                        return self.serde.deserialize(reply)
                    else:
                        # send another request...
                        break
                else:
                    self.logger.debug("NO-POLLIN")
                    self.close()
                    self._init_socket()
                    waiting_for_reply = False
                time.sleep(.5)

        raise RuntimeError("Unable to receive response from driver")

    def close(self):
        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.close()
        self.poller.unregister(self.socket)
