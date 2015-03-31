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

# Service classes know how to deploy a service onto a set of nodes and then
# clean up the services. They request the necessary resources from the cluster,
# configure each server, and bring up/tear down the service. They also expose
# information about the service so that other services or test scripts can
# easily be configured to work with them. Finally, they may be able to collect
# and check logs/output from the service, which can be helpful in writing tests
# or benchmarks.
#
# Services should generally be written to support an arbitrary number of nodes,
# even if instances are independent of each other. They should be able to assume
# that there won't be resource conflicts: the cluster tests are being run on
# should be large enough to use one instance per service instance.

from ducktape.logger import Logger


class Service(Logger):
    def __init__(self, cluster, num_nodes):
        self.num_nodes = num_nodes
        self.cluster = cluster

    def start(self):
        """Start the service running in the background."""
        self.nodes = self.cluster.request(self.num_nodes)
        for idx, node in enumerate(self.nodes, 1):
            self.logger.debug("Forcibly cleaning node %d on %s", idx, node.account.hostname)
            self._clean_node(node)

    def wait(self):
        """Wait for the service to finish. This only makes sense for tasks with a fixed
        amount of work to do. For services that generate output, it is only
        guaranteed to be available after this call returns.
        """
        pass

    def stop(self):
        """If the service left any running processes or data, clean them up."""
        pass

    def run(self):
        """Helper that executes run(), wait(), and stop() in sequence."""
        self.start()
        self.wait()
        self.stop()

    def get_node(self, idx):
        """ids presented externally are indexed from 1, so we provide a helper method to avoid confusion."""
        return self.nodes[idx - 1]

    def idx(self, node):
        """Return id of the given node. Return -1 if node does not belong to this service. """
        for idx, n in enumerate(self.nodes, 1):
            if self.get_node(idx) == node:
                return idx
        return -1

    def _clean_node(self, node):
        """
        Helper that tries to kill off all running Java processes to make sure a node
        is in a clean state.
        """
        node.account.kill_process("java", clean_shutdown=False)

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
