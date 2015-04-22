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
from ducktape.services.service import ServiceContext
from ducktape.services.service_registry import ServiceRegistry

import logging
import os
import sys


class Test(object):
    """Base class for tests.
    """
    def __init__(self, test_context):
        """
        :type test_context: ducktape.tests.test.TestContext
        """
        self.cluster = test_context.session_context.cluster
        self.test_context = test_context
        self.logger = test_context.logger
        self.services = ServiceRegistry()

        # dict for toggling service log collection on/off
        self.log_collect = {}

    def min_cluster_size(self):
        """
        Provides a helpful heuristic for determining if there are enough nodes in the cluster to run this test.
        """
        return self.services.num_nodes()

    def tearDown(self):
        """Default teardown method which stops and cleans services registered in the ServiceRegistry.
        Note that there is not a default setUp method. This is because the framework makes no assumptions
        about when and in what methods the various services are started.
        """
        if hasattr(self, 'services') and type(self.services) == ServiceRegistry:
            self.services.stop_all()
            self.copy_service_logs()
            self.services.clean_all()
            self.services.free_all()

    def copy_service_logs(self):
        """Copy logs from service nodes to the results directory."""
        for service in self.services.values():
            if not hasattr(service, 'logs'):
                self.test_context.logger.debug("Won't collect service logs from %s - no 'logs' attribute." %
                    service.__class__.__name__)
                return

            log_dirs = service.logs
            for node in service.nodes:
                for log_name in log_dirs.keys():
                    if self.should_collect_log(log_name, service, node):
                        dest = os.path.join(
                            self.test_context.results_dir, service.__class__.__name__, node.account.hostname)

                        if not os.path.isdir(dest):
                            mkdir_p(dest)

                        try:
                            node.account.scp_from(log_dirs[log_name], dest, recursive=True)
                        except Exception as e:
                            self.test_context.logger.warn(
                                "Error copying log %(log_name)s from %(source)s to %(dest)s. \
                                service %(service)s: %(message)s" %
                                {'log_name': log_name,
                                 'source': log_dirs[log_name],
                                 'dest': dest,
                                 'service': service,
                                 'message': e.message})

    def mark_log_for_collect(self, log_name, service, node=None):
        if node is None:
            # By default, mark all nodes for collection
            for n in service.nodes:
                self.log_collect[(log_name, service.__class__.__name__, n.account.hostname)] = True
        else:
            self.log_collect[(log_name, service.__class__.__name__, node.account.hostname)] = True

    def mark_log_for_no_collect(self, log_name, service, node):
        if node is None:
            # By default, mark all nodes for collection
            for n in service.nodes:
                self.log_collect[(log_name, service.__class__.__name__, n.account.hostname)] = False
        else:
            self.log_collect[(log_name, service.__class__.__name__, node.account.hostname)] = False

    def should_collect_log(self, log_name, service, node):
        return True

    def service_context(self, num_nodes=1):
        """A convenience method to reduce boilerplate which returns ServiceContext object.
        :type num_nodes: int    Number of nodes to allocate to the service.
        :rtype ducktape.services.service.ServiceContext
        """
        return ServiceContext(self.cluster, num_nodes, self.logger)


class TestContext(Logger):
    """Wrapper class for state variables needed to properly run a single 'test unit'."""
    def __init__(self, session_context, module, cls, function, config, log_config=None):
        """
        :type session_context: ducktape.tests.session.SessionContext
        """
        self.module = module
        self.cls = cls
        self.function = function
        self.config = config
        self.session_context = session_context

        self.results_dir = os.path.join(self.session_context.results_dir, self.cls.__name__)
        mkdir_p(self.results_dir)

        self._logger_configured = False
        self.configure_logger(log_config)

    @property
    def test_id(self):
        name_components = [
            self.session_context.session_id,
            self.module]

        if self.cls is not None:
            name_components.append(self.cls.__name__)

        name_components.append(self.function.__name__)
        return ".".join(name_components)

    @property
    def logger_name(self):
        return self.test_id

    def configure_logger(self, log_config=None):
        if self._logger_configured:
            raise RuntimeError("test logger should only be configured once.")

        self.logger.setLevel(logging.DEBUG)

        mkdir_p(self.results_dir)
        fh = logging.FileHandler(os.path.join(self.results_dir, "test_log"))
        fh.setLevel(logging.DEBUG)
        # create console handler with a higher log level
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.INFO)
        # create formatter and add it to the handlers
        formatter = logging.Formatter(ConsoleConfig.TEST_LOG_FORMATTER)
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        # add the handlers to the logger
        self.logger.addHandler(fh)
        # test_context.logger.addHandler(ch)

        self._logger_configured = True


