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
import signal
import time
import traceback
import zmq

from ducktape.tests.event import ClientEventFactory
from ducktape.tests.loader import TestLoader
from ducktape.tests.serde import SerDe
from ducktape.tests.test import test_logger

from ducktape.tests.result import TestResult, IGNORE, PASS, FAIL
from ducktape.tests.reporter import SingleResultFileReporter
from ducktape.utils.local_filesystem_utils import mkdir_p


def run_client(server_hostname, server_port, logger_name, test_id, log_dir, debug):
    client = RunnerClient(server_hostname, server_port, test_id, logger_name, log_dir, debug)
    client.run()


class RunnerClient(object):
    """Run a single test"""

    def __init__(self, server_hostname, server_port, test_id, logger_name, log_dir, debug):
        signal.signal(signal.SIGTERM, self._sigterm_handler)  # register a SIGTERM handler

        self.serde = SerDe()
        self.logger = test_logger(logger_name, log_dir, debug)
        self.runner_port = server_port

        self.test_id = test_id
        self.id = "test-runner-%d-%d" % (os.getpid(), id(self))
        self.message = ClientEventFactory(self.test_id, self.id)
        self.sender = Sender(server_hostname, str(self.runner_port), self.message, self.logger)

        ready_reply = self.sender.send(self.message.ready())
        self.session_context = ready_reply["session_context"]
        self.test_metadata = ready_reply["test_metadata"]
        self.cluster = ready_reply["cluster"]

        # Wait to instantiate the test object until running the test
        self.test = None
        self.test_context = None

    def send(self, event):
        return self.sender.send(event)

    def _sigterm_handler(self, signum, frame):
        """Translate SIGTERM to SIGINT on this process

        python will treat SIGINT as a Keyboard exception. Exception handling does the rest.
        """
        os.kill(os.getpid(), signal.SIGINT)

    def _collect_test_context(self, directory, file_name, cls_name, method_name, injected_args):
        # TODO - different logger for TestLoader object
        loader = TestLoader(self.session_context, self.logger, injected_args=injected_args, cluster=self.cluster)
        loaded_context_list = loader.discover(directory, file_name, cls_name, method_name)

        assert len(loaded_context_list) == 1
        test_context = loaded_context_list[0]
        test_context.cluster = self.cluster
        return test_context

    def run(self):
        self.log(logging.INFO, "Loading test %s" % str(self.test_metadata))
        self.test_context = self._collect_test_context(**self.test_metadata)

        self.send(self.message.running())
        if self.test_context.ignore:
            # Skip running this test, but keep track of the fact that we ignored it
            result = TestResult(self.test_context,
                                self.session_context,
                                test_status=IGNORE,
                                start_time=time.time(),
                                stop_time=time.time())
            # Tell the server we are finished
            self.send(self.message.finished(result=result))
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

            self.log(logging.DEBUG, "Checking if there are enough nodes...")
            if self.test.min_cluster_size() > len(self.cluster):
                raise RuntimeError(
                    "There are not enough nodes available in the cluster to run this test. "
                    "Cluster size: %d, Need at least: %d. Services currently registered: %s" %
                    (len(self.cluster), self.test.min_cluster_size(), self.test_context.services))

            # Run the test unit
            start_time = time.time()
            self.setup_test()

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
            self.send(self.message.finished(result=result))
        except Exception as e:
            self.logger.error("Problem sending FINISHED message:", str(e))

    def setup_test(self):
        """start services etc"""
        self.log(logging.INFO, "Setting up...")
        self.test.setUp()

    def run_test(self):
        """Run the test!

        We expect test_context.function to be a function or unbound method which takes an
        instantiated test object as its argument.
        """
        self.log(logging.INFO, "Running...")
        return self.test_context.function(self.test)

    def teardown_test(self, teardown_services=True):
        """teardown method which stops services, gathers log data, removes persistent state, and releases cluster nodes.

        Catch all exceptions so that every step in the teardown process is tried, but signal that the test runner
        should stop if a keyboard interrupt is caught.
        """
        self.log(logging.INFO, "Tearing down...")
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

    def log(self, log_level, msg, *args, **kwargs):
        """Log to the service log and the test log of the current test."""

        if self.test_context is None:
            msg = "%s: %s" % (self.__class__.__name__, str(msg))
            self.logger.log(log_level, msg, *args, **kwargs)
        else:
            msg = "%s: %s: %s" % (self.__class__.__name__, self.test_context.test_name, str(msg))
            self.logger.log(log_level, msg, *args, **kwargs)

        self.send(self.message.log(msg, level=log_level))


class Sender(object):
    REQUEST_TIMEOUT_MS = 3000
    NUM_RETRIES = 5

    def __init__(self, server_host, server_port, message_supplier, logger):
        self.serde = SerDe()
        self.server_endpoint = "tcp://%s:%s" % (str(server_host), str(server_port))
        self.zmq_context = zmq.Context()
        self.socket = None
        self.poller = zmq.Poller()

        self.message_supplier = message_supplier
        self.logger = logger

        self._init_socket()

    def _init_socket(self):
        self.socket = self.zmq_context.socket(zmq.REQ)
        self.socket.connect(self.server_endpoint)
        self.poller.register(self.socket, zmq.POLLIN)

    def send(self, event, blocking=True):

        retries_left = Sender.NUM_RETRIES

        while retries_left > 0:
            self.logger.debug("client: sending event: " + str(event))
            serialized_event = self.serde.serialize(event)
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
                        self.logger.debug("received " + str(self.serde.deserialize(reply)))
                        return self.serde.deserialize(reply)
                    else:
                        # send another request...
                        break
                else:
                    self.logger.debug("NO-POLLIN")
                    self.close()
                    self._init_socket()
                    waiting_for_reply = False
                # Ensure each message we attempt to send has a unique id
                # This copy constructor gives us a duplicate with a new message id
                event = self.message_supplier.copy(event)

        raise RuntimeError("Unable to receive response from driver")

    def close(self):
        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.close()
        self.poller.unregister(self.socket)
