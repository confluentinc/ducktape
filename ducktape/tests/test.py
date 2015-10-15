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

import json
import logging
import os
import re
import sys


class Test(TemplateRenderer):
    """Base class for tests.
    """
    def __init__(self, test_context, *args, **kwargs):
        """
        :type test_context: ducktape.tests.test.TestContext
        """
        super(Test, self).__init__(*args, **kwargs)
        self.test_context = test_context

    @property
    def cluster(self):
        return self.test_context.session_context.cluster

    @property
    def logger(self):
        return self.test_context.logger

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


def _escape_pathname(s):
    """Remove fishy characters, replace most with dots"""
    # Remove all whitespace completely
    s = re.sub("\s+", "", s)

    # Replace bad characters with dots
    blacklist = "[^\.\-=_\w\d]+"
    s = re.sub(blacklist, ".", s)

    # Multiple dots -> single dot (and no leading or trailing dot)
    s = re.sub("[\.]+", ".", s)
    return re.sub("^\.|\.$", "", s)


class TestContext(Logger):
    """Wrapper class for state variables needed to properly run a single 'test unit'."""
    _TEST_ID_DELIMITER = "~~"

    @staticmethod
    def create_test_id(module_name, cls_name, function_name, injected_args):
        name_components = [module_name,
                           cls_name,
                           function_name,
                           json.dumps(injected_args, separators=(',', ':'))]

        return TestContext._TEST_ID_DELIMITER.join(filter(lambda x: x is not None and len(x) > 0, name_components))

    @staticmethod
    def from_test_context(test_context):
        """Make a copy"""
        return TestContext(test_context.session_context, test_context.module, test_context.cls, test_context.function, test_context.injected_args)

    def __init__(self, session_context, module=None, cls=None, function=None, injected_args=None):
        """
        :type session_context: ducktape.tests.session.SessionContext
        """
        self.module = module
        self.cls = cls
        self.function = function
        self.injected_args = injected_args

        self.session_context = session_context
        self.services = ServiceRegistry()

        # dict for toggling service log collection on/off
        self.log_collect = {}

        # Individual test results go here
        mkdir_p(self.results_dir)

        self._logger_configured = False
        self.configure_logger()

    def __repr__(self):
        return "<module=%s, cls=%s, function=%s, injected_args=%s>" % \
               (self.module, self.cls_name, self.function_name, str(self.injected_args))

    @property
    def module_name(self):
        return "" if self.module is None else self.module

    @property
    def cls_name(self):
        return "" if self.cls is None else self.cls.__name__

    @property
    def function_name(self):
        return "" if self.function is None else self.function.__name__

    @property
    def description(self):
        """Description of the test, needed in particular for reporting.
        If the function has a docstring, return that, otherwise return the class docstring or "".
        """
        if self.function.__doc__:
            return self.function.__doc__
        elif self.cls.__doc__ is not None:
            return self.cls.__doc__
        else:
            return ""

    @property
    def cluster(self):
        return self.session_context.cluster

    @property
    def results_dir(self):
        d = self.session_context.results_dir

        if self.cls is not None:
            d = os.path.join(d, self.cls.__name__)
        if self.function is not None:
            d = os.path.join(d, self.function.__name__)
        if self.injected_args is not None:
            params = ".".join(["%s=%s" % (k, self.injected_args[k]) for k in self.injected_args])
            d = os.path.join(d, _escape_pathname(params))

        return d

    @property
    def test_id(self):
        """
        The fully-qualified name of the test. This is similar to test_id, but does not include the session ID. It
        includes the module, class, and method name.
        """

        return TestContext.create_test_id(self.module_name, self.cls_name, self.function_name, self.injected_args)

    @property
    def logger_name(self):
        return ".".join([self.session_context.session_id, self.test_id])

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

        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(formatter)
        if self.session_context.debug:
            # If debug flag is set, pipe verbose test logging to stdout
            ch.setLevel(logging.DEBUG)
        else:
            # default - pipe warning level logging to stdout
            ch.setLevel(logging.WARNING)
        self.logger.addHandler(ch)

        self._logger_configured = True



