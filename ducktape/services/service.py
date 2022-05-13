# Copyright 2014 Confluent Inc.
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

from ducktape.cluster.cluster_spec import ClusterSpec
from ducktape.command_line.defaults import ConsoleDefaults
from ducktape.template import TemplateRenderer
from ducktape.errors import TimeoutError

import os
import shutil
import tempfile
import time


class ServiceIdFactory:
    def generate_service_id(self, service):
        return "{service_name}-{service_number}-{service_id}".format(
            service_name=service.__class__.__name__,
            service_number=service._order,
            service_id=id(service)
        )


class MultiRunServiceIdFactory:
    def __init__(self, run_number=1):
        self.run_number = run_number

    def generate_service_id(self, service):
        return "{run_number}-{service_name}-{service_number}-{service_id}".format(
            run_number=self.run_number,
            service_name=service.__class__.__name__,
            service_number=service._order,
            service_id=id(service)
        )


service_id_factory = ServiceIdFactory()


class Service(TemplateRenderer):
    """Service classes know how to deploy a service onto a set of nodes and then clean up after themselves.

    They request the necessary resources from the cluster,
    configure each node, and bring up/tear down the service.

    They also expose
    information about the service so that other services or test scripts can
    easily be configured to work with them. Finally, they may be able to collect
    and check logs/output from the service, which can be helpful in writing tests
    or benchmarks.

    Services should generally be written to support an arbitrary number of nodes,
    even if instances are independent of each other. They should be able to assume
    that there won't be resource conflicts: the cluster tests are being run on
    should be large enough to use one instance per service instance.
    """

    # Provides a mechanism for locating and collecting log files produced by the service on its nodes.
    # logs is a dict with entries that look like log_name: {"path": log_path, "collect_default": boolean}
    #
    # For example, zookeeper service might have self.logs like this:
    # self.logs = {
    #    "zk_log": {"path": "/mnt/zk.log",
    #               "collect_default": True}
    # }
    logs = {}

    def __init__(self, context, num_nodes=None, cluster_spec=None, *args, **kwargs):
        """
        Initialize the Service.

        Note: only one of (num_nodes, cluster_spec) may be set.

        :param context:         An object which has at minimum 'cluster' and 'logger' attributes. In tests, this
                                is always a TestContext object.
        :param num_nodes:       An integer representing the number of Linux nodes to allocate.
        :param cluster_spec:    A ClusterSpec object representing the minimum cluster specification needed.
        """
        super(Service, self).__init__(*args, **kwargs)
        # Keep track of significant events in the lifetime of this service
        self._init_time = time.time()
        self._start_time = -1
        self._start_duration_seconds = -1
        self._stop_time = -1
        self._stop_duration_seconds = -1
        self._clean_time = -1

        self._initialized = False
        self.service_id_factory = service_id_factory
        self.cluster_spec = Service.setup_cluster_spec(num_nodes=num_nodes, cluster_spec=cluster_spec)
        self.context = context

        self.nodes = []
        self.skip_nodes_allocation = kwargs.get("skip_nodes_allocation", False)
        if not self.skip_nodes_allocation:
            self.allocate_nodes()

        # Keep track of which nodes nodes were allocated to this service, even after nodes are freed
        # Note: only keep references to representations of the nodes, not the actual node objects themselves
        self._nodes_formerly_allocated = [str(node.account) for node in self.nodes]

        # Every time a service instance is created, it registers itself with its
        # context object. This makes it possible for external mechanisms to clean up
        # after the service if something goes wrong.
        #
        # Note: Allocate nodes *before* registering self with the service registry
        self.context.services.append(self)

        # Each service instance has its own local scratch directory on the test driver
        self._local_scratch_dir = None
        self._initialized = True

    @staticmethod
    def setup_cluster_spec(num_nodes=None, cluster_spec=None):
        if num_nodes is None:
            if cluster_spec is None:
                raise RuntimeError("You must set either num_nodes or cluster_spec.")
            else:
                return cluster_spec
        else:
            if cluster_spec is not None:
                raise RuntimeError("You must set only one of (num_nodes, cluster_spec)")
            return ClusterSpec.simple_linux(num_nodes)

    def __repr__(self):
        return "<%s: %s>" % (self.who_am_i(), "num_nodes: %d, nodes: %s" %
                             (len(self.nodes), [n.account.hostname for n in self.nodes]))

    @property
    def num_nodes(self):
        return len(self.nodes)

    @property
    def local_scratch_dir(self):
        """This local scratch directory is created/destroyed on the test driver before/after each test is run."""
        if not self._local_scratch_dir:
            self._local_scratch_dir = tempfile.mkdtemp()
        return self._local_scratch_dir

    @property
    def service_id(self):
        """Human-readable identifier (almost certainly) unique within a test run."""
        return self.service_id_factory.generate_service_id(self)

    @property
    def _order(self):
        """Index of this service instance with respect to other services of the same type registered with self.context.
        When used with a test_context, this lets the user know

        Example::

            suppose the services registered with the same context looks like
                context.services == [Zookeeper, Kafka, Zookeeper, Kafka, MirrorMaker]
            then:
                context.services[0]._order == 0  # "0th" Zookeeper instance
                context.services[2]._order == 0  # "0th" Kafka instance
                context.services[1]._order == 1  # "1st" Zookeeper instance
                context.services[3]._order == 1  # "1st" Kafka instance
                context.services[4]._order == 0  # "0th" MirrorMaker instance

        """
        if hasattr(self.context, "services"):
            same_services = [id(s) for s in self.context.services if type(s) == type(self)]

            if self not in self.context.services and not self._initialized:
                # It's possible that _order will be invoked in the constructor *before* self has been registered with
                # the service registry (aka self.context.services).
                return len(same_services)

            # Note: index raises ValueError if the item is not in the list
            index = same_services.index(id(self))
            return index
        else:
            return 0

    @property
    def logger(self):
        """The logger instance for this service."""
        return self.context.logger

    @property
    def cluster(self):
        """The cluster object from which this service instance gets its nodes."""
        return self.context.cluster

    @property
    def allocated(self):
        """Return True iff nodes have been allocated to this service instance."""
        return len(self.nodes) > 0

    def who_am_i(self, node=None):
        """Human-readable identifier useful for log messages."""
        if node is None:
            return self.service_id
        else:
            return "%s node %d on %s" % (self.service_id, self.idx(node), node.account.hostname)

    def allocate_nodes(self):
        """Request resources from the cluster."""
        if self.allocated:
            raise Exception("Requesting nodes for a service that has already been allocated nodes.")

        self.logger.debug("Requesting nodes from the cluster: %s" % self.cluster_spec)

        try:
            self.nodes = self.cluster.alloc(self.cluster_spec)
        except RuntimeError as e:
            msg = str(e)
            if hasattr(self.context, "services"):
                msg += " Currently registered services: " + str(self.context.services)
            raise RuntimeError(msg)

        for idx, node in enumerate(self.nodes, 1):
            # Remote accounts utilities should log where this service logs
            if node.account._logger is not None:
                # This log message help test-writer identify which test and/or service didn't clean up after itself
                node.account.logger.critical(ConsoleDefaults.BAD_TEST_MESSAGE)
                raise RuntimeError(
                    "logger was not None on service start. There may be a concurrency issue, "
                    "or some service which isn't properly cleaning up after itself. "
                    "Service: %s, node.account: %s" % (self.__class__.__name__, str(node.account)))
            node.account.logger = self.logger

        self.logger.debug("Successfully allocated %d nodes to %s" % (len(self.nodes), self.who_am_i()))

    def start(self, **kwargs):
        """Start the service on all nodes."""
        self.logger.info("%s: starting service" % self.who_am_i())
        if self._start_time < 0:
            # Set self._start_time only the first time self.start is invoked
            self._start_time = time.time()

        self.logger.debug(self.who_am_i() + ": killing processes and attempting to clean up before starting")
        for node in self.nodes:
            # Added precaution - kill running processes, clean persistent files (if 'clean'=False flag passed,
            # skip cleaning), try/except for each step, since each of these steps may fail if there
            # are no processes to kill or no files to remove

            try:
                self.stop_node(node)
            except Exception:
                pass

            try:
                if kwargs.get('clean', True):
                    self.clean_node(node)
                else:
                    self.logger.debug("%s: skip cleaning node" % self.who_am_i(node))
            except Exception:
                pass

        for node in self.nodes:
            self.logger.debug("%s: starting node" % self.who_am_i(node))
            self.start_node(node, **kwargs)

        if self._start_duration_seconds < 0:
            self._start_duration_seconds = time.time() - self._start_time

    def start_node(self, node, **kwargs):
        """Start service process(es) on the given node."""
        pass

    def wait(self, timeout_sec=600):
        """Wait for the service to finish.
        This only makes sense for tasks with a fixed amount of work to do. For services that generate
        output, it is only guaranteed to be available after this call returns.
        """
        unfinished_nodes = []
        start = time.time()
        end = start + timeout_sec
        for node in self.nodes:
            now = time.time()
            node_name = self.who_am_i(node)
            if end > now:
                self.logger.debug("%s: waiting for node", node_name)
                if not self.wait_node(node, end - now):
                    unfinished_nodes.append(node_name)
            else:
                unfinished_nodes.append(node_name)

        if unfinished_nodes:
            raise TimeoutError("Timed out waiting %s seconds for service nodes to finish. " % str(timeout_sec)
                               + "These nodes are still alive: " + str(unfinished_nodes))

    def wait_node(self, node, timeout_sec=None):
        """Wait for the service on the given node to finish.
        Return True if the node finished shutdown, False otherwise.
        """
        pass

    def stop(self, **kwargs):
        """Stop service processes on each node in this service.
        Subclasses must override stop_node.
        """
        self._stop_time = time.time()  # The last time stop is invoked
        self.logger.info("%s: stopping service" % self.who_am_i())
        for node in self.nodes:
            self.logger.info("%s: stopping node" % self.who_am_i(node))
            self.stop_node(node, **kwargs)

        self._stop_duration_seconds = time.time() - self._stop_time

    def stop_node(self, node, **kwargs):
        """Halt service process(es) on this node."""
        pass

    def clean(self, **kwargs):
        """Clean up persistent state on each node - e.g. logs, config files etc.
        Subclasses must override clean_node.
        """
        self._clean_time = time.time()
        self.logger.info("%s: cleaning service" % self.who_am_i())
        for node in self.nodes:
            self.logger.info("%s: cleaning node" % self.who_am_i(node))
            self.clean_node(node, **kwargs)

    def clean_node(self, node, **kwargs):
        """Clean up persistent state on this node - e.g. service logs, configuration files etc."""
        self.logger.warn("%s: clean_node has not been overriden. "
                         "This may be fine if the service leaves no persistent state."
                         % self.who_am_i())

    def free(self):
        """Free each node. This 'deallocates' the nodes so the cluster can assign them to other services."""
        while self.nodes:
            node = self.nodes.pop()
            self.logger.info("%s: freeing node" % self.who_am_i(node))
            node.account.logger = None
            self.cluster.free(node)

    def run(self):
        """Helper that executes run(), wait(), and stop() in sequence."""
        self.start()
        self.wait()
        self.stop()

    def get_node(self, idx):
        """ids presented externally are indexed from 1, so we provide a helper method to avoid confusion."""
        return self.nodes[idx - 1]

    def idx(self, node):
        """Return id of the given node. Return -1 if node does not belong to this service.

        idx identifies the node within this service instance (not globally).
        """
        for idx, n in enumerate(self.nodes, 1):
            if self.get_node(idx) == node:
                return idx
        return -1

    def close(self):
        """Release resources."""
        # Remove local scratch directory
        if self._local_scratch_dir and os.path.exists(self._local_scratch_dir):
            shutil.rmtree(self._local_scratch_dir)

    @staticmethod
    def run_parallel(*args):
        """Helper to run a set of services in parallel. This is useful if you want
           multiple services of different types to run concurrently, e.g. a
           producer + consumer pair.
        """
        for svc in args:
            svc.start()
        for svc in args:
            svc.wait()
        for svc in args:
            svc.stop()

    def to_json(self):
        return {
            "cls_name": self.__class__.__name__,
            "module_name": self.__module__,

            "lifecycle": {
                "init_time": self._init_time,
                "start_time": self._start_time,
                "start_duration_seconds": self._start_duration_seconds,
                "stop_time": self._stop_time,
                "stop_duration_seconds": self._stop_duration_seconds,
                "clean_time": self._clean_time
            },
            "service_id": self.service_id,
            "nodes": self._nodes_formerly_allocated
        }
