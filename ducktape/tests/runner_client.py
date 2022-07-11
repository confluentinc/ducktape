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

from ducktape.services.service import MultiRunServiceIdFactory, service_id_factory
from ducktape.services.service_registry import ServiceRegistry

from ducktape.tests.event import ClientEventFactory
from ducktape.tests.loader import TestLoader
from ducktape.tests.serde import SerDe
from ducktape.tests.status import FLAKY
from ducktape.tests.test import test_logger, TestContext

from ducktape.tests.result import TestResult, IGNORE, PASS, FAIL
from ducktape.utils.local_filesystem_utils import mkdir_p


def run_client(*args, **kwargs):
    client = RunnerClient(*args, **kwargs)
    client.run()


class RunnerClient(object):
    """Run a single test"""

    def __init__(self, server_hostname, server_port, test_id,
                 test_index, logger_name, log_dir, debug, fail_bad_cluster_utilization, deflake_num):
        signal.signal(signal.SIGTERM, self._sigterm_handler)  # register a SIGTERM handler

        self.serde = SerDe()
        self.logger = test_logger(logger_name, log_dir, debug)
        self.runner_port = server_port

        self.fail_bad_cluster_utilization = fail_bad_cluster_utilization
        self.test_id = test_id
        self.test_index = test_index
        self.id = "test-runner-%d-%d" % (os.getpid(), id(self))
        self.message = ClientEventFactory(self.test_id, self.test_index, self.id)
        self.sender = Sender(server_hostname, str(self.runner_port), self.message, self.logger)

        ready_reply = self.sender.send(self.message.ready())
        self.session_context = ready_reply["session_context"]
        self.test_metadata = ready_reply["test_metadata"]
        self.cluster = ready_reply["cluster"]

        self.deflake_num = deflake_num

        # Wait to instantiate the test object until running the test
        self.test = None
        self.test_context = None
        self.all_services = None

    def send(self, event):
        return self.sender.send(event)

    def _sigterm_handler(self, signum, frame):
        """Translate SIGTERM to SIGINT on this process

        python will treat SIGINT as a Keyboard exception. Exception handling does the rest.
        """
        os.kill(os.getpid(), signal.SIGINT)

    def _collect_test_context(self, directory, file_name, cls_name, method_name, injected_args):
        loader = TestLoader(self.session_context, self.logger, injected_args=injected_args, cluster=self.cluster)
        # TODO: deal with this in a more graceful fashion.
        #       In an unlikely even that discover either raises the exception or fails to find exactly one test
        #       we should probably continue trying other tests rather than killing this process
        loaded_context_list = loader.discover(directory, file_name, cls_name, method_name)

        assert len(loaded_context_list) == 1
        test_context = loaded_context_list[0]
        test_context.cluster = self.cluster
        return test_context

    def run(self):
        self.log(logging.INFO, "Loading test %s" % str(self.test_metadata))
        self.test_context = self._collect_test_context(**self.test_metadata)
        self.test_context.test_index = self.test_index

        self.send(self.message.running())
        if self.test_context.ignore:
            # Skip running this test, but keep track of the fact that we ignored it
            result = TestResult(self.test_context,
                                self.test_index,
                                self.session_context,
                                test_status=IGNORE,
                                start_time=time.time(),
                                stop_time=time.time())
            result.report()
            # Tell the server we are finished
            self.send(self.message.finished(result=result))
            return

        start_time = -1
        stop_time = -1
        test_status = FAIL
        summary = []
        data = None
        self.all_services = ServiceRegistry()

        num_runs = 0

        try:
            while test_status == FAIL and num_runs < self.deflake_num:
                num_runs += 1
                self.log(logging.INFO, "on run {}/{}".format(num_runs, self.deflake_num))
                start_time = time.time()
                test_status, summary, data = self._do_run(num_runs)

                if test_status == PASS and num_runs > 1:
                    test_status = FLAKY

                msg = str(test_status.to_json())
                if summary:
                    msg += ": {}".format(summary)
                if num_runs != self.deflake_num:
                    msg += "\n" + "~" * max(len(line) for line in summary.split('\n'))

                self.log(logging.INFO, msg)

        finally:
            stop_time = time.time()

            test_status, summary = self._check_cluster_utilization(test_status, summary)

            if num_runs > 1:
                # for reporting purposes report all services
                self.test_context.services = self.all_services
            # for flaky tests, we report the start and end time of the successful run, and not the whole run period
            result = TestResult(
                self.test_context,
                self.test_index,
                self.session_context,
                test_status,
                summary,
                data,
                start_time,
                stop_time)

            self.log(logging.INFO, "Data: %s" % str(result.data))

            result.report()
            # Tell the server we are finished
            self._do_safely(lambda: self.send(self.message.finished(result=result)),
                            "Problem sending FINISHED message for " + str(self.test_metadata) + ":\n")
            # Release test_context resources only after creating the result and finishing logging activity
            # The Sender object uses the same logger, so we postpone closing until after the finished message is sent
            self.test_context.close()
            self.all_services = None
            self.test_context = None
            self.test = None

    def _do_run(self, num_runs):
        test_status = FAIL
        summary = []
        data = None
        sid_factory = MultiRunServiceIdFactory(num_runs) if self.deflake_num > 1 else service_id_factory
        try:
            # Results from this test, as well as logs will be dumped here
            mkdir_p(TestContext.results_dir(self.test_context, self.test_index))
            # Instantiate test
            self.test = self.test_context.cls(self.test_context)

            # Run the test unit
            self.setup_test()
            data = self.run_test()
            test_status = PASS

        except BaseException as e:
            # mark the test as failed before doing anything else
            test_status = FAIL
            err_trace = self._exc_msg(e)
            summary.append(err_trace)

        finally:
            for service in self.test_context.services:
                service.service_id_factory = sid_factory
                self.all_services.append(service)

            self.teardown_test(teardown_services=not self.session_context.no_teardown, test_status=test_status)

            if hasattr(self.test_context, "services"):
                service_errors = self.test_context.services.errors()
                if service_errors:
                    summary.extend(["\n\n", service_errors])

            # free nodes
            if self.test:
                self.log(logging.DEBUG, "Freeing nodes...")
                self._do_safely(self.test.free_nodes, "Error freeing nodes:")
            return test_status, "".join(summary), data

    def _check_cluster_utilization(self, result, summary):
        """Checks if the number of nodes used by a test is less than the number of
        nodes requested by the test. If this is the case and we wish to fail
        on bad cluster utilization, the result value is failed. Will also print
        a warning if the test passes and the node utilization doesn't match.
        """
        max_used = self.cluster.max_used()
        total = len(self.cluster.all())
        if max_used < total:
            message = "Test requested %d nodes, used only %d" % (total, max_used)
            if self.fail_bad_cluster_utilization:
                # only check node utilization on test pass
                if result == PASS or result == FLAKY:
                    self.log(logging.INFO, "FAIL: " + message)

                result = FAIL
                summary += message
            else:
                self.log(logging.WARN, message)
        return result, summary

    def setup_test(self):
        """start services etc"""
        self.log(logging.INFO, "Setting up...")
        self.test.setup()

    def run_test(self):
        """Run the test!

        We expect test_context.function to be a function or unbound method which takes an
        instantiated test object as its argument.
        """
        self.log(logging.INFO, "Running...")
        return self.test_context.function(self.test)

    def _exc_msg(self, e):
        return repr(e) + "\n" + traceback.format_exc(limit=16)

    def _do_safely(self, action, err_msg):
        try:
            action()
        except BaseException as e:
            self.log(logging.WARN, err_msg + " " + self._exc_msg(e))

    def teardown_test(self, teardown_services=True, test_status=None):
        """teardown method which stops services, gathers log data, removes persistent state, and releases cluster nodes.

        Catch all exceptions so that every step in the teardown process is tried, but signal that the test runner
        should stop if a keyboard interrupt is caught.
        """
        self.log(logging.INFO, "Tearing down...")
        if not self.test:
            self.log(logging.WARN, "%s failed to instantiate" % self.test_id)
            self.test_context.close()
            return

        services = self.test_context.services

        if teardown_services:
            self._do_safely(self.test.teardown, "Error running teardown method:")
            # stop services
            self._do_safely(services.stop_all, "Error stopping services:")

        # always collect service logs whether or not we tear down
        # logs are typically removed during "clean" phase, so collect logs before cleaning
        self.log(logging.DEBUG, "Copying logs from services...")
        self._do_safely(lambda: self.test.copy_service_logs(test_status), "Error copying service logs:")

        # clean up stray processes and persistent state
        if teardown_services:
            self.log(logging.DEBUG, "Cleaning up services...")
            self._do_safely(services.clean_all, "Error cleaning services:")

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
            serialized_event = self.serde.serialize(event)
            self.socket.send(serialized_event)
            retries_left -= 1
            waiting_for_reply = True

            while waiting_for_reply:
                sockets = dict(self.poller.poll(Sender.REQUEST_TIMEOUT_MS))

                if sockets.get(self.socket) == zmq.POLLIN:
                    reply = self.socket.recv()
                    if reply:
                        return self.serde.deserialize(reply)
                    else:
                        # send another request...
                        break
                else:
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
