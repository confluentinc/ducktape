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

from ducktape.cluster.mixed_os_cluster import MixedOsCluster


class FiniteSubcluster(MixedOsCluster):
    """This cluster class gives us a mechanism for allocating finite blocks of nodes from another cluster.
    """
    def __init__(self, nodes):
        self.nodes = nodes
        self.available_nodes = set(self.nodes)
        self.in_use_nodes = set()

    def __len__(self):
        """Size of this cluster object. I.e. number of 'nodes' in the cluster."""
        return len(self.nodes)

    def alloc(self, node_spec):
        for operating_system, num_nodes in node_spec.iteritems():
            assert num_nodes <= self.num_available_nodes(operating_system=operating_system), \
                "Not enough nodes available to allocate the requested %s nodes. " % operating_system + \
                "Nodes requested: %s " % num_nodes + \
                "Nodes available: %s" % self.num_available_nodes(operating_system=operating_system)

        allocated_nodes = []
        for operating_system, num_nodes in node_spec.iteritems():
            for _ in range(num_nodes):
                node = MixedOsCluster._next_available_node(self.available_nodes, operating_system)
                self.available_nodes.remove(node)
                self.in_use_nodes.add(node)

                allocated_nodes.append(node)

        return allocated_nodes

    def free_single(self, node):
        assert node in self.in_use_nodes
        self.in_use_nodes.remove(node)
        self.available_nodes.add(node)
