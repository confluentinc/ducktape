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

from six import iteritems


class NodeNotPresentError(Exception):
    pass


class InsufficientResourcesError(Exception):
    pass


class NodeContainer(object):
    def __init__(self, nodes=None):
        """
        Create a NodeContainer with the given nodes.

        Node objects should implement at least an operating_system property.

        :param nodes:           A collection of node objects to add, or None to add nothing.
        """
        self.os_to_nodes = {}
        if nodes is not None:
            for node in nodes:
                self.os_to_nodes.setdefault(node.operating_system, []).append(node)

    def size(self):
        """
        Returns the total number of nodes in the container.
        """
        return sum([len(val) for val in self.os_to_nodes.values()])

    def __len__(self):
        return self.size()

    def __iter__(self):
        return self.elements()

    def elements(self, operating_system=None):
        """
        Yield the elements in this container.

        :param operating_system:    If this is non-None, we will iterate only over elements
                                    which have this operating system.
        """
        if operating_system is None:
            for node_list in self.os_to_nodes.values():
                for node in node_list:
                    yield node
        else:
            for node in self.os_to_nodes.get(operating_system, []):
                yield node

    def add_node(self, node):
        """
        Add a node to this collection.

        :param node:                        The node to add.
        """
        self.os_to_nodes.setdefault(node.operating_system, []).append(node)

    def add_nodes(self, nodes):
        """
        Add a collection of nodes to this collection.

        :param nodes:                       The nodes to add.
        """
        for node in nodes:
            self.add_node(node)

    def remove_node(self, node):
        """
        Removes a node from this collection.

        :param node:                        The node to remove.
        :returns:                           The node which has been removed.
        :throws NodeNotPresentError:        If the node is not in the collection.
        """
        try:
            return self.os_to_nodes.get(node.operating_system, []).remove(node)
        except ValueError:
            raise NodeNotPresentError

    def remove_nodes(self, nodes):
        """
        Remove a collection of nodes from this collection.

        :param nodes:                       The nodes to remove.
        """
        for node in nodes:
            self.remove_node(node)

    def remove_spec(self, cluster_spec):
        """
        Remove nodes matching a ClusterSpec from this NodeContainer.

        :param cluster_spec:                    The cluster spec.  This will not be modified.
        :returns:                               A list of the nodes that were removed.
        :throws InsufficientResourcesError:     If there are not enough nodes in the NodeContainer.
                                                Nothing will be removed unless enough are available.
        """
        msg = self.attempt_remove_spec(cluster_spec)
        if len(msg) > 0:
            raise InsufficientResourcesError("Not enough nodes available to allocate. " + msg)
        removed = []
        for os, node_specs in iteritems(cluster_spec.nodes.os_to_nodes):
            num_nodes = len(node_specs)
            avail_nodes = self.os_to_nodes.get(os, [])
            for i in range(0, num_nodes):
                removed.append(avail_nodes.pop(0))
        return removed

    def can_remove_spec(self, cluster_spec):
        """
        Determine if we can remove nodes matching a ClusterSpec from this NodeContainer.
        This container will not be modified.

        :param cluster_spec:                    The cluster spec.  This will not be modified.
        :returns:                               True if we could remove the nodes; false otherwise
        """
        msg = self.attempt_remove_spec(cluster_spec)
        return len(msg) == 0

    def attempt_remove_spec(self, cluster_spec):
        """
        Attempt to remove a cluster_spec from this node container.

        :param cluster_spec:                    The cluster spec.  This will not be modified.
        :returns:                               An empty string if we can remove the nodes;
                                                an error string otherwise.
        """
        msg = ""
        for os, node_specs in iteritems(cluster_spec.nodes.os_to_nodes):
            num_nodes = len(node_specs)
            avail_nodes = len(self.os_to_nodes.get(os, []))
            if avail_nodes < num_nodes:
                msg = msg + "%s nodes requested: %d. %s nodes available: %d" % \
                            (os, num_nodes, os, avail_nodes)
        return msg

    def clone(self):
        """
        Returns a deep copy of this object.
        """
        container = NodeContainer()
        for operating_system, nodes in iteritems(self.os_to_nodes):
            for node in nodes:
                container.os_to_nodes.setdefault(operating_system, []).append(node)
        return container
