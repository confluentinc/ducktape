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
from ducktape.cluster.cluster_spec import ClusterSpec
from ducktape.cluster.node_container import NodeContainer


class FiniteSubcluster(Cluster):
    """This cluster class gives us a mechanism for allocating finite blocks of nodes from another cluster.
    """

    def __init__(self, nodes):
        self.nodes = nodes
        self._available_nodes = NodeContainer(nodes)
        self._in_use_nodes = NodeContainer()

    def alloc(self, cluster_spec):
        allocated = self._available_nodes.remove_spec(cluster_spec)
        self._in_use_nodes.add_nodes(allocated)
        return allocated

    def free_single(self, node):
        self._in_use_nodes.remove_node(node)
        self._available_nodes.add_node(node)

    def available(self):
        return ClusterSpec.from_nodes(self._available_nodes)

    def used(self):
        return ClusterSpec.from_nodes(self._in_use_nodes)
