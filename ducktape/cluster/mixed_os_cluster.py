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

from .cluster import Cluster
from .remoteaccount import RemoteAccount


class MixedOsCluster(Cluster):
    def num_nodes_for_operating_system(self, operating_system):
        return self.in_use_nodes_for_operating_system(operating_system) + self.num_available_nodes(operating_system)

    def num_available_nodes(self, operating_system=RemoteAccount.LINUX):
        """Number of available nodes."""
        return MixedOsCluster._node_count_helper(self.available_nodes, operating_system)

    def in_use_nodes_for_operating_system(self, operating_system):
        return MixedOsCluster._node_count_helper(self.in_use_nodes, operating_system)

    @staticmethod
    def _node_count_helper(nodes, operating_system):
        count = 0
        for node in nodes:
            if node.operating_system == operating_system:
                count += 1
        return count

    @staticmethod
    def _next_available_node(nodes, operating_system):
        node_to_return = None
        for node in nodes:
            if node.operating_system == operating_system:
                node_to_return = node

        return node_to_return
