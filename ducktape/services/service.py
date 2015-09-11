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


from ducktape.command_line.config import ConsoleConfig
from ducktape.template import TemplateRenderer
from ducktape.cluster.cluster import ClusterSlot


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

    def __init__(self, context, num_nodes, *args, **kwargs):
        """
        :param context    An object which has at minimum 'cluster' and 'logger' attributes. In tests, this is always a TestContext object.
        :param num_nodes  Number of nodes to allocate to this service from the cluster. Node allocation takes place
                          when start() is called, or when allocate_nodes() is called, whichever happens first.
        """
        super(Service, self).__init__(*args, **kwargs)
        self.num_nodes = num_nodes
        self.context = context
        self.allocated = False
        self.nodes = [ClusterSlot(self.context.cluster, account=None) for i in range(num_nodes)]

        # Every time a service instance is created, it registers itself with its
        # context object. This makes it possible for external mechanisms to clean up
        # after the service if something goes wrong.
        if hasattr(self.context, "services"):
            self.context.services.append(self)

    def __repr__(self):
        return "<%s: %s>" % (self.who_am_i(), "num_nodes: %d, allocated: %s, nodes: %s" %
                             (self.num_nodes, self.allocated, [str(n) for n in self.nodes]))

    @property
    def logger(self):
        return self.context.logger

    @property
    def cluster(self):
        return self.context.cluster

    def who_am_i(self, node=None):
        """Human-readable identifier useful for log messages."""
        if node is None:
            return self.__class__.__name__
        else:
            return "%s node %d on %s" % (self.__class__.__name__, self.idx(node), node.account.hostname)

    def allocate_nodes(self):
        """Request resources from the cluster."""
        if self.allocated:
            raise Exception("Requesting nodes for a service that has already been allocated nodes.")

        self.logger.debug("Requesting nodes from the cluster.")

        try:
            remote_accounts = self.cluster.request(self.num_nodes)
            assert len(self.nodes) == len(remote_accounts)
            for cluster_slot, account in zip(self.nodes, remote_accounts):
                cluster_slot.account = account

        except RuntimeError as e:
            if hasattr(self.context, "services"):
                msg = e.message + " Currently registered services: " + str(self.context.services)
            raise RuntimeError(msg)

        for idx, node in enumerate(self.nodes, 1):
            # Remote accounts utilities should log where this service logs
            if node.account.logger is not None:
                # This log message help test-writer identify which test and/or service didn't clean up after itself
                node.account.logger.critical(ConsoleConfig.BAD_TEST_MESSAGE)
                raise RuntimeError(
                    "logger was not None on service start. There may be a concurrency issue, " +
                    "or some service which isn't properly cleaning up after itself. " +
                    "Service: %s, node.account: %s" % (self.__class__.__name__, str(node.account)))
            node.account.logger = self.logger

        self.allocated = True

    def start(self):
        """Start the service on all nodes."""
        self.logger.info("%s: starting service" % self.who_am_i())

        if not self.allocated:
            self.allocate_nodes()

        self.logger.debug(self.who_am_i() + ": killing processes and attempting to clean up before starting")
        for node in self.nodes:
            # Added precaution - kill running processes, clean persistent files
            # try/except for each step, since each of these steps may fail if there are no processes
            # to kill or no files to remove

            try:
                self.stop_node(node)
            except:
                pass

            try:
                self.clean_node(node)
            except:
                pass

        for node in self.nodes:
            self.logger.debug("%s: starting node" % self.who_am_i(node))
            self.start_node(node)

    def start_node(self, node):
        """Start service process(es) on the given node."""
        raise NotImplementedError("%s: subclasses must implement start_node." % self.who_am_i())

    def wait(self):
        """Wait for the service to finish.
        This only makes sense for tasks with a fixed amount of work to do. For services that generate
        output, it is only guaranteed to be available after this call returns.
        """
        pass

    def stop(self):
        """Stop service processes on each node in this service.
        Subclasses must override stop_node.
        """
        self.logger.info("%s: stopping service" % self.who_am_i())
        for node in self.nodes:
            self.logger.info("%s: stopping node" % self.who_am_i(node))
            self.stop_node(node)

    def stop_node(self, node):
        """Halt service process(es) on this node."""
        raise NotImplementedError("%s: subclasses must implement stop_node." % self.who_am_i())

    def clean(self):
        """Clean up persistent state on each node - e.g. logs, config files etc.
        Subclasses must override clean_node.
        """
        self.logger.info("%s: cleaning service" % self.who_am_i())
        for node in self.nodes:
            self.logger.info("%s: cleaning node" % self.who_am_i(node))
            self.clean_node(node)

    def clean_node(self, node):
        """Clean up persistent state on this node - e.g. service logs, configuration files etc."""
        self.logger.warn("%s: clean_node has not been overriden. " % self.who_am_i(),
                         "This may be fine if the service leaves no persistent state.")

    def free(self):
        """Free each node. This 'deallocates' the nodes so the cluster can assign them to other services."""
        for node in self.nodes:
            self.logger.info("%s: freeing node" % self.who_am_i(node))
            node.account.logger = None
            self.free_node(node)

    def free_node(self, node):
        """Release this node back to the cluster that owns it."""
        node.free()

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

