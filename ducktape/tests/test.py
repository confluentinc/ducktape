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

import copy
from contextlib import contextmanager
import logging
import os
import re
import shutil
import sys
import tempfile

from ducktape.cluster.cluster_spec import ClusterSpec
from ducktape.tests.loggermaker import LoggerMaker, close_logger
from ducktape.tests.session import SessionContext
from ducktape.utils.local_filesystem_utils import mkdir_p
from ducktape.command_line.defaults import ConsoleDefaults
from ducktape.services.service_registry import ServiceRegistry
from ducktape.template import TemplateRenderer
from ducktape.mark.resource import CLUSTER_SPEC_KEYWORD, CLUSTER_SIZE_KEYWORD
from ducktape.tests.status import FAIL


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
        return self.test_context.cluster

    @property
    def logger(self):
        return self.test_context.logger

    def min_cluster_spec(self):
        """
        THIS METHOD IS DEPRECATED AND WILL BE REMOVED IN THE SUBSEQUENT RELEASES.
        Nothing in the ducktape framework calls it, it is only provided so that subclasses don't break.
        If you're overriding this method in your subclass, please remove it.
        """
        raise NotImplementedError

    def min_cluster_size(self):
        """
        THIS METHOD IS DEPRECATED AND WILL BE REMOVED IN THE SUBSEQUENT RELEASES.
        Nothing in the ducktape framework calls it, it is only provided so that subclasses don't break.
        If you're overriding this method in your subclass, please remove it.
        """
        raise NotImplementedError

    def setup(self):
        """Override this for custom setup logic."""

        # for backward compatibility
        self.setUp()

    def teardown(self):
        """Override this for custom teardown logic."""

        # for backward compatibility
        self.tearDown()

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def free_nodes(self):
        try:
            self.test_context.services.free_all()
        except BaseException as e:
            if isinstance(e, KeyboardInterrupt):
                raise

    def compress_service_logs(self, node, service, node_logs):
        """Compress logs on a node corresponding to the given service.

        :param node: The node on which to compress the given logs
        :param service: The service to which the node belongs
        :param node_logs: Paths to logs (or log directories) which will be compressed
        :return: a list of paths to compressed logs.

        """
        compressed_logs = []
        for nlog in node_logs:
            try:
                node.account.ssh(_compress_cmd(nlog))
                if nlog.endswith(os.path.sep):
                    nlog = nlog[:-len(os.path.sep)]
                nlog += ".tgz"
                compressed_logs.append(nlog)

            except Exception as e:
                self.test_context.logger.warn(
                    "Error compressing log %s: service %s: %s" % (nlog, service, str(e))
                )

        return compressed_logs

    def copy_service_logs(self, test_status):
        """
        Copy logs from service nodes to the results directory.

        If the test passed, only the default set will be collected. If the the test failed, all logs will be collected.
        """
        for service in self.test_context.services:
            if not hasattr(service, 'logs') or len(service.logs) == 0:
                self.test_context.logger.debug("Won't collect service logs from %s - no logs to collect." %
                                               service.service_id)
                continue

            log_dirs = service.logs
            for node in service.nodes:
                # Gather locations of logs to collect
                node_logs = []
                for log_name in log_dirs.keys():
                    if test_status == FAIL or self.should_collect_log(log_name, service):
                        node_logs.append(log_dirs[log_name]["path"])

                self.test_context.logger.debug("Preparing to copy logs from %s: %s" %
                                               (node.account.hostname, node_logs))

                if self.test_context.session_context.compress:
                    self.test_context.logger.debug("Compressing logs...")
                    node_logs = self.compress_service_logs(node, service, node_logs)

                if len(node_logs) > 0:
                    # Create directory into which service logs will be copied
                    dest = os.path.join(
                        TestContext.results_dir(self.test_context, self.test_context.test_index),
                        service.service_id, node.account.hostname)
                    if not os.path.isdir(dest):
                        mkdir_p(dest)

                    # Try to copy the service logs
                    self.test_context.logger.debug("Copying logs...")
                    try:
                        for log in node_logs:
                            node.account.copy_from(log, dest)
                    except Exception as e:
                        self.test_context.logger.warn(
                            "Error copying log %(log_name)s from %(source)s to %(dest)s. \
                            service %(service)s: %(message)s" %
                            {'log_name': log_name,
                             'source': log_dirs[log_name],
                             'dest': dest,
                             'service': service,
                             'message': e})

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


def _compress_cmd(log_path):
    """Return bash command which compresses the given path to a tarball."""
    compres_cmd = 'cd "$(dirname %s)" && ' % log_path
    compres_cmd += 'f="$(basename %s)" && ' % log_path
    compres_cmd += 'if [ -e "$f" ]; then tar czf "$f.tgz" "$f"; fi && '
    compres_cmd += 'rm -rf %s' % log_path

    return compres_cmd


def _escape_pathname(s):
    """Remove fishy characters, replace most with dots"""
    # Remove all whitespace completely
    s = re.sub(r"\s+", "", s)

    # Replace bad characters with dots
    blacklist = r"[^\.\-=_\w\d]+"
    s = re.sub(blacklist, ".", s)

    # Multiple dots -> single dot (and no leading or trailing dot)
    s = re.sub(r"[\.]+", ".", s)
    return re.sub(r"^\.|\.$", "", s)


def test_logger(logger_name, log_dir, debug):
    """Helper method for getting a test logger object

    Note that if this method is called multiple times with the same ``logger_name``, it returns the same logger object.
    Note also, that for a fixed ``logger_name``, configuration occurs only the first time this function is called.
    """
    return TestLoggerMaker(logger_name, log_dir, debug).logger


class TestLoggerMaker(LoggerMaker):
    def __init__(self, logger_name, log_dir, debug):
        super(TestLoggerMaker, self).__init__(logger_name)
        self.log_dir = log_dir
        self.debug = debug

    def configure_logger(self):
        """Set up the logger to log to stdout and files.
        This creates a directory and a few files as a side-effect.
        """
        if self.configured:
            return

        self._logger.setLevel(logging.DEBUG)
        mkdir_p(self.log_dir)

        # Create info and debug level handlers to pipe to log files
        info_fh = logging.FileHandler(os.path.join(self.log_dir, "test_log.info"))
        debug_fh = logging.FileHandler(os.path.join(self.log_dir, "test_log.debug"))

        info_fh.setLevel(logging.INFO)
        debug_fh.setLevel(logging.DEBUG)

        formatter = logging.Formatter(ConsoleDefaults.TEST_LOG_FORMATTER)
        info_fh.setFormatter(formatter)
        debug_fh.setFormatter(formatter)

        self._logger.addHandler(info_fh)
        self._logger.addHandler(debug_fh)

        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(formatter)
        if self.debug:
            # If debug flag is set, pipe debug logs to stdout
            ch.setLevel(logging.DEBUG)
        else:
            # default - pipe warning level logging to stdout
            ch.setLevel(logging.WARNING)
        self._logger.addHandler(ch)


class TestContext(object):
    """Wrapper class for state variables needed to properly run a single 'test unit'."""

    def __init__(self, **kwargs):
        """
        :param session_context:
        :param cluster: the cluster object which will be used by this test
        :param module: name of the module containing the test class/method
        :param cls: class object containing the test method
        :param function: the test method
        :param file: file containing this module
        :param injected_args: a dict containing keyword args which will be passed to the test method
        :param cluster_use_metadata: dict containing information about how this test will use cluster resources
        """

        self.session_context: SessionContext = kwargs.get("session_context")
        self.cluster = kwargs.get("cluster")
        self.module = kwargs.get("module")
        self.test_suite_name = kwargs.get("test_suite_name")

        if kwargs.get("file") is not None:
            self.file = os.path.abspath(kwargs.get("file"))
        else:
            self.file = None
        self.cls = kwargs.get("cls")
        self.function = kwargs.get("function")
        self.injected_args = kwargs.get("injected_args")
        self.ignore = kwargs.get("ignore", False)

        # cluster_use_metadata is a dict containing information about how this test will use cluster resources
        self.cluster_use_metadata = copy.copy(kwargs.get("cluster_use_metadata", {}))

        self.services = ServiceRegistry()
        self.test_index = None

        # dict for toggling service log collection on/off
        self.log_collect = {}

        self._logger = None
        self._local_scratch_dir = None

    def __repr__(self):
        return \
            f"<module={self.module}, cls={self.cls_name}, function={self.function_name}, " \
            f"injected_args={self.injected_args}, file={self.file}, ignore={self.ignore}, " \
            f"cluster_spec={self.expected_cluster_spec}>"

    def copy(self, **kwargs):
        """Construct a new TestContext object from another TestContext object
        Note that this is not a true copy, since a fresh ServiceRegistry instance will be created.
        """
        ctx_copy = TestContext(**self.__dict__)
        ctx_copy.__dict__.update(**kwargs)

        return ctx_copy

    @property
    def local_scratch_dir(self):
        """This local scratch directory is created/destroyed on the test driver before/after each test is run."""
        if not self._local_scratch_dir:
            self._local_scratch_dir = tempfile.mkdtemp()
        return self._local_scratch_dir

    @property
    def test_metadata(self):
        return {
            "directory": os.path.dirname(self.file),
            "file_name": os.path.basename(self.file),
            "cls_name": self.cls_name,
            "method_name": self.function_name,
            "injected_args": self.injected_args
        }

    @staticmethod
    def logger_name(test_context, test_index):
        if test_index is None:
            return test_context.test_id
        else:
            return "%s-%s" % (test_context.test_id, str(test_index))

    @staticmethod
    def results_dir(test_context, test_index):
        d = test_context.session_context.results_dir

        if test_context.cls is not None:
            d = os.path.join(d, test_context.cls.__name__)
        if test_context.function is not None:
            d = os.path.join(d, test_context.function.__name__)
        if test_context.injected_args is not None:
            d = os.path.join(d, test_context.injected_args_name)
        if test_index is not None:
            d = os.path.join(d, str(test_index))

        return d

    @property
    def expected_num_nodes(self):
        """
        How many nodes of any type we expect this test to consume when run.
        Note that this will be 0 for both unschedulable tests and the tests that legitimately need 0 nodes.

        :return:            an integer number of nodes.
        """
        return self.expected_cluster_spec.size() if self.expected_cluster_spec else 0

    @property
    def expected_cluster_spec(self):
        """
        The cluster spec we expect this test to consume when run.

        :return:            A ClusterSpec object or None if the test cannot be run
                            (e.g. session context settings disallow tests with no cluster metadata attached).
        """
        cluster_spec = self.cluster_use_metadata.get(CLUSTER_SPEC_KEYWORD)
        cluster_size = self.cluster_use_metadata.get(CLUSTER_SIZE_KEYWORD)
        if cluster_spec is not None:
            return cluster_spec
        elif cluster_size is not None:
            return ClusterSpec.simple_linux(cluster_size)
        elif not self.cluster:
            return ClusterSpec.empty()
        elif self.session_context.fail_greedy_tests:
            return None
        else:
            return self.cluster.all()

    @property
    def globals(self):
        return self.session_context.globals

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
    def injected_args_name(self):
        if self.injected_args is None:
            return ""
        else:
            params = ".".join(["%s=%s" % (k, self.injected_args[k]) for k in self.injected_args])
            return _escape_pathname(params)

    @property
    def test_id(self):
        return self.test_name

    @property
    def test_name(self):
        """
        The fully-qualified name of the test. This is similar to test_id, but does not include the session ID. It
        includes the module, class, and method name.
        """
        name_components = [self.module_name,
                           self.cls_name,
                           self.function_name,
                           self.injected_args_name]

        return ".".join(filter(lambda x: x is not None and len(x) > 0, name_components))

    @property
    def logger(self):
        if self._logger is None:
            self._logger = test_logger(
                TestContext.logger_name(self, self.test_index),
                TestContext.results_dir(self, self.test_index),
                self.session_context.debug)
        return self._logger

    def close(self):
        """Release resources, etc."""
        if hasattr(self, "services"):
            for service in self.services:
                service.close()
            # Remove reference to services. This is important to prevent potential memory leaks if users write services
            # which themselves have references to large memory-intensive objects
            del self.services

        # Remove local scratch directory
        if self._local_scratch_dir and os.path.exists(self._local_scratch_dir):
            shutil.rmtree(self._local_scratch_dir)

        # Release file handles held by logger
        if self._logger:
            close_logger(self._logger)


@contextmanager
def in_dir(path):
    """ Changes working directory to given path. On exit, restore to original working directory. """
    cwd = os.getcwd()

    try:
        os.chdir(path)
        yield

    finally:
        os.chdir(cwd)


@contextmanager
def in_temp_dir():
    """ Creates a temporary directory as the working directory. On exit, it is removed. """
    with _new_temp_dir() as tmpdir:
        with in_dir(tmpdir):
            yield tmpdir


@contextmanager
def _new_temp_dir():
    """ Create a temporary directory that is removed automatically """
    tmpdir = tempfile.mkdtemp()

    try:
        yield tmpdir

    finally:
        shutil.rmtree(tmpdir)
