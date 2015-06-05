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

from ducktape.tests.logger import Logger
from ducktape.utils.local_filesystem_utils import mkdir_p
from ducktape.command_line.config import ConsoleConfig
from ducktape.services.service_registry import ServiceRegistry
from ducktape.template import TemplateRenderer

import logging
import os
import sys


class Test(TemplateRenderer):
    """Base class for tests.
    """
    def __init__(self, test_context, *args, **kwargs):
        """
        :type test_context: ducktape.tests.test.TestContext
        """
        super(Test, self).__init__(*args, **kwargs)
        self.cluster = test_context.session_context.cluster
        self.test_context = test_context
        self.logger = test_context.logger

    def min_cluster_size(self):
        """Heuristic for guessing whether there are enough nodes in the cluster to run this test.

        Note this is not a reliable indicator of the true minimum cluster size, since new service instances may
        be added at any time. However, it does provide a lower bound on the minimum cluster size.
        """
        return self.test_context.services.num_nodes()

    def setUp(self):
        """Override this for custom setup logic."""
        pass

    def tearDown(self):
        """Override this for custom teardown logic."""
        pass

    def free_nodes(self):
        try:
            self.test_context.services.free_all()
        except BaseException as e:
            if isinstance(e, KeyboardInterrupt):
                raise e

    def copy_service_logs(self):
        """Copy logs from service nodes to the results directory."""
        for service in self.test_context.services:
            if not hasattr(service, 'logs') or len(service.logs) == 0:
                self.test_context.logger.debug("Won't collect service logs from %s - no logs to collect." %
                    service.__class__.__name__)
                return

            log_dirs = service.logs
            for node in service.nodes:
                # Gather locations of logs to collect
                node_logs = []
                for log_name in log_dirs.keys():
                    if self.should_collect_log(log_name, service):
                        node_logs.append(log_dirs[log_name]["path"])

                if len(node_logs) > 0:
                    # Create directory into which service logs will be copied
                    dest = os.path.join(
                        self.test_context.results_dir, service.__class__.__name__, node.account.hostname)
                    if not os.path.isdir(dest):
                        mkdir_p(dest)

                    # Try to copy the service logs
                    try:
                        node.account.scp_from(node_logs, dest, recursive=True)
                    except Exception as e:
                        self.test_context.logger.warn(
                            "Error copying log %(log_name)s from %(source)s to %(dest)s. \
                            service %(service)s: %(message)s" %
                            {'log_name': log_name,
                             'source': log_dirs[log_name],
                             'dest': dest,
                             'service': service,
                             'message': e.message})

    def mark_for_collect(self, service, log_name=None):
        if log_name is None:
            # Mark every log for collection
            for log_name in service.logs:
                self.test_context.log_collect[(log_name, service)] = True
        else:
            self.test_context.log_collect[(log_name, service)] = True

    def mark_no_collect(self, service, log_name=None):
        self.test_context.log_collect[(log_name, service)] = False

    def should_collect_log(self, log_name, service):
        key = (log_name, service)
        default = service.logs[log_name]["collect_default"]
        val = self.test_context.log_collect.get(key, default)
        return val


class TestContext(Logger):
    """Wrapper class for state variables needed to properly run a single 'test unit'."""
    def __init__(self, session_context, module=None, cls=None, function=None, config=None):
        """
        :type session_context: ducktape.tests.session.SessionContext
        """
        self.module = module
        self.cls = cls
        self.function = function
        self.config = config
        self.session_context = session_context
        self.cluster = session_context.cluster
        self.services = ServiceRegistry()

        # dict for toggling service log collection on/off
        self.log_collect = {}

        self.results_dir = self.session_context.results_dir
        if self.cls is not None:
            self.results_dir = os.path.join(self.results_dir, self.cls.__name__)
        if self.function is not None:
            self.results_dir = os.path.join(self.results_dir, self.function.__name__)
        mkdir_p(self.results_dir)

        self._logger_configured = False
        self.configure_logger()

    @property
    def test_id(self):
        name_components = [self.session_context.session_id,
                           self.module,
                           self.cls.__name__ if self.cls is not None else None,
                           self.function.__name__ if self.function is not None else None]

        return ".".join(filter(lambda x: x is not None, name_components))

    @property
    def test_name(self):
        """
        The fully-qualified name of the test. This is similar to test_id, but does not include the session ID. It
        includes the module, class, and method name.
        """
        name_components = [self.module,
                           self.cls.__name__ if self.cls is not None else None,
                           self.function.__name__ if self.function is not None else None]

        return ".".join(filter(lambda x: x is not None, name_components))

    @property
    def logger_name(self):
        return self.test_id

    def configure_logger(self):
        if self._logger_configured:
            raise RuntimeError("test logger should only be configured once.")

        self.logger.setLevel(logging.DEBUG)
        mkdir_p(self.results_dir)

        # Create info and debug level handlers to pipe to log files
        info_fh = logging.FileHandler(os.path.join(self.results_dir, "test_log.info"))
        debug_fh = logging.FileHandler(os.path.join(self.results_dir, "test_log.debug"))

        info_fh.setLevel(logging.INFO)
        debug_fh.setLevel(logging.DEBUG)

        formatter = logging.Formatter(ConsoleConfig.TEST_LOG_FORMATTER)
        info_fh.setFormatter(formatter)
        debug_fh.setFormatter(formatter)

        self.logger.addHandler(info_fh)
        self.logger.addHandler(debug_fh)

        # If debug flag is set, pipe verbose test logging to stdout
        if self.session_context.debug:
            ch = logging.StreamHandler(sys.stdout)
            ch.setLevel(logging.DEBUG)
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

        self._logger_configured = True


