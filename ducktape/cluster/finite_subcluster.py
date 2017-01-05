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

from ducktape.cluster.cluster import Cluster
from ducktape.cluster.remoteaccount import RemoteAccount


class FiniteSubcluster(Cluster):
    """This cluster class gives us a mechanism for allocating finite blocks of nodes from another cluster.
    """
    def __init__(self, nodes):
        self.nodes = nodes
        self._available = set(self.nodes)
        self._in_use = set()

    def __len__(self):
        """Size of this cluster object. I.e. number of 'nodes' in the cluster."""
        return len(self.nodes)

    def alloc(self, node_spec):
        for operating_system, num_nodes in node_spec.iteritems():
            assert num_nodes <= self.num_available_nodes(operating_system=operating_system), \
                "Not enough nodes available to allocate the requested %s nodes. Nodes requested: %s, Nodes available: %s" % \
                (operating_system, num_nodes, self.num_available_nodes())

        allocated_nodes = []
        for operating_system, num_nodes in node_spec.iteritems():
            for _ in range(num_nodes):
                node = Cluster._next_available_node(self._available, operating_system)
                self._available.remove(node)
                self._in_use.add(node)

                allocated_nodes.append(node)

        return allocated_nodes

    def num_available_nodes(self, operating_system=RemoteAccount.LINUX):
        """Number of available nodes."""
        return Cluster._node_count_helper(self._available, operating_system)

    def free_single(self, node):
        assert node in self._in_use
        self._in_use.remove(node)
        self._available.add(node)
