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

import os
import shutil
import tempfile
from contextlib import contextmanager

from ducktape.template import TemplateRenderer
from ducktape.tests.status import FAIL
from ducktape.utils.local_filesystem_utils import mkdir_p

from .test_context import TestContext


class Test(TemplateRenderer):
    """Base class for tests."""

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
                    nlog = nlog[: -len(os.path.sep)]
                nlog += ".tgz"
                compressed_logs.append(nlog)

            except Exception as e:
                self.test_context.logger.warn("Error compressing log %s: service %s: %s" % (nlog, service, str(e)))

        return compressed_logs

    def copy_service_logs(self, test_status):
        """
        Copy logs from service nodes to the results directory.

        If the test passed, only the default set will be collected. If the the test failed, all logs will be collected.
        """
        for service in self.test_context.services:
            if not hasattr(service, "logs") or len(service.logs) == 0:
                self.test_context.logger.debug(
                    "Won't collect service logs from %s - no logs to collect." % service.service_id
                )
                continue

            log_dirs = service.logs
            for node in service.nodes:
                # Gather locations of logs to collect
                node_logs = []
                for log_name in log_dirs.keys():
                    if test_status == FAIL or self.should_collect_log(log_name, service):
                        node_logs.append(log_dirs[log_name]["path"])

                self.test_context.logger.debug(
                    "Preparing to copy logs from %s: %s" % (node.account.hostname, node_logs)
                )

                if self.test_context.session_context.compress:
                    self.test_context.logger.debug("Compressing logs...")
                    node_logs = self.compress_service_logs(node, service, node_logs)

                if len(node_logs) > 0:
                    # Create directory into which service logs will be copied
                    dest = os.path.join(
                        TestContext.results_dir(self.test_context, self.test_context.test_index),
                        service.service_id,
                        node.account.hostname,
                    )
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
                            service %(service)s: %(message)s"
                            % {
                                "log_name": log_name,
                                "source": log_dirs[log_name],
                                "dest": dest,
                                "service": service,
                                "message": e,
                            }
                        )

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
    compres_cmd += "rm -rf %s" % log_path

    return compres_cmd


@contextmanager
def in_dir(path):
    """Changes working directory to given path. On exit, restore to original working directory."""
    cwd = os.getcwd()

    try:
        os.chdir(path)
        yield

    finally:
        os.chdir(cwd)


@contextmanager
def in_temp_dir():
    """Creates a temporary directory as the working directory. On exit, it is removed."""
    with _new_temp_dir() as tmpdir:
        with in_dir(tmpdir):
            yield tmpdir


@contextmanager
def _new_temp_dir():
    """Create a temporary directory that is removed automatically"""
    tmpdir = tempfile.mkdtemp()

    try:
        yield tmpdir

    finally:
        shutil.rmtree(tmpdir)
