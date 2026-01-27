# Copyright 2017 Confluent Inc.
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

from __future__ import absolute_import

import json
import typing

from ducktape.cluster.node_container import NodeContainer

from .consts import LINUX
from .node_spec import NodeSpec


class ClusterSpec(object):
    """
    The specification for a ducktape cluster.
    """

    nodes: typing.Optional[NodeContainer] = None

    @staticmethod
    def empty():
        return ClusterSpec([])

    @staticmethod
    def simple_linux(num_nodes, node_type=None):
        """
        Create a ClusterSpec for Linux nodes, optionally of a specific type.

        Examples:
            ClusterSpec.simple_linux(5)              # 5 nodes, any type
            ClusterSpec.simple_linux(3, "large")     # 3 large nodes

        :param num_nodes: Number of Linux nodes
        :param node_type: Optional node type label (e.g., "large", "small")
        """
        node_specs = [NodeSpec(LINUX, node_type)] * num_nodes
        return ClusterSpec(node_specs)

    @staticmethod
    def from_nodes(nodes):
        """
        Create a ClusterSpec describing a list of nodes.
        """
        return ClusterSpec([NodeSpec(node.operating_system, getattr(node, 'node_type', None)) for node in nodes])

    def __init__(self, nodes=None):
        """
        Initialize the ClusterSpec.

        :param nodes:           A collection of NodeSpecs, or None to create an empty cluster spec.
        """
        self.nodes = NodeContainer(nodes)

    def __len__(self):
        return self.size()

    def __iter__(self):
        return self.nodes.elements()

    def size(self):
        """Return the total size of this cluster spec, including all types of nodes."""
        return self.nodes.size()

    def add(self, other):
        """
        Add another ClusterSpec to this one.

        :param node_spec:       The other cluster spec.  This will not be modified.
        :return:                This ClusterSpec.
        """
        for node_spec in other.nodes:
            self.nodes.add_node(node_spec)
        return self

    def clone(self):
        """
        Returns a deep copy of this object.
        """
        return ClusterSpec(self.nodes.clone())

    def __str__(self):
        node_spec_to_num = {}
        for node_spec in self.nodes.elements():
            node_spec_str = str(node_spec)
            node_spec_to_num[node_spec_str] = node_spec_to_num.get(node_spec_str, 0) + 1
        rval = []
        for node_spec_str in sorted(node_spec_to_num.keys()):
            node_spec = json.loads(node_spec_str)
            node_spec["num_nodes"] = node_spec_to_num[node_spec_str]
            rval.append(node_spec)
        return json.dumps(rval, sort_keys=True)
