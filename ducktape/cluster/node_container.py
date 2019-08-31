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
from operator import attrgetter


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
        Remove nodes matching a ClusterSpec from this NodeContainer. Nodes are allocated
        based on machine type with following strategy:

        1) To compare MachineType, different weight has been assigned to configuration
           as cpu > mem > disk > additional_disk, which means node1:{mem:4G, disk:100G}
           required more resource than node2:{mem:2G, disk:200G}.
        2) Always try to satisfy node specific that requires most resource.
        3) Always try to allocate machine with least resource as possible.

        :param cluster_spec:                    The cluster spec.  This will not be modified.
        :returns:                               A list of the nodes that were removed.
        :throws InsufficientResourcesError:     If there are not enough nodes in the NodeContainer.
                                                Nothing will be removed unless enough are available.
        """
        msg = self.attempt_remove_spec(cluster_spec)
        if len(msg) > 0:
            raise InsufficientResourcesError("Not enough nodes available to allocate. " + msg)
        removed = []
        for os, req_nodes in iteritems(cluster_spec.nodes.os_to_nodes):
            avail_nodes = self.os_to_nodes.get(os, [])
            sorted_req_nodes = NodeContainer.sort(nodes=req_nodes, reverse=True)
            sorted_avail_nodes = NodeContainer.sort(nodes=avail_nodes)
            for req_node in sorted_req_nodes[:]:
                for avail_node in sorted_avail_nodes[:]:
                    if NodeContainer.satisfy(avail_node, req_node):
                        sorted_avail_nodes.remove(avail_node)
                        avail_nodes.remove(avail_node)
                        removed.append(avail_node)
                        break
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
        Attempt to remove a cluster_spec from this node container. Uses the same strategy
        as remove_spec(self, cluster_spec).

        :param cluster_spec:                    The cluster spec.  This will not be modified.
        :returns:                               An empty string if we can remove the nodes;
                                                an error string otherwise.
        """
        msg = ""
        for os, req_nodes in iteritems(cluster_spec.nodes.os_to_nodes):
            avail_nodes = self.os_to_nodes.get(os, [])
            num_avail_nodes = len(avail_nodes)
            num_req_nodes = len(req_nodes)
            if num_avail_nodes < num_req_nodes:
                msg = msg + "%s nodes requested: %d. %s nodes available: %d" % (os, num_req_nodes, os, num_avail_nodes)
            sorted_req_nodes = NodeContainer.sort(nodes=req_nodes, reverse=True)
            sorted_avail_nodes = NodeContainer.sort(nodes=avail_nodes)
            for req_node in sorted_req_nodes[:]:
                for avail_node in sorted_avail_nodes[:]:
                    if NodeContainer.satisfy(avail_node, req_node):
                        sorted_req_nodes.remove(req_node)
                        sorted_avail_nodes.remove(avail_node)
                        break
            # check unsatisfied nodes
            for unsatisfied_node in sorted_req_nodes:
                msg += "\ncannot satisfy minimum requirement for requested node: %s" % str(unsatisfied_node)
        return msg

    @staticmethod
    def satisfy(avail_node, req_node):
        """
        Return true if available node satisfies the minimum requirement of requested node.
        """
        if avail_node.machine_type.cpu_core < req_node.machine_type.cpu_core or \
           avail_node.machine_type.mem_size_gb < req_node.machine_type.mem_size_gb or \
           avail_node.machine_type.disk_size_gb < req_node.machine_type.disk_size_gb:
            return False
        for d_name, d_size in req_node.machine_type.additional_disks.iteritems():
            if avail_node.machine_type.additional_disks.get(d_name, 0) < d_size:
                return False
        return True

    @staticmethod
    def sort(nodes, reverse=False):
        """
        Return sorted list of nodes based on machine_type.
        """
        return sorted(nodes, key=attrgetter('machine_type.cpu_core', 'machine_type.mem_size_gb',
                                            'machine_type.disk_size_gb', 'machine_type.additional_disks'),
                      reverse=reverse)

    def clone(self):
        """
        Returns a deep copy of this object.
        """
        container = NodeContainer()
        for operating_system, nodes in iteritems(self.os_to_nodes):
            for node in nodes:
                container.os_to_nodes.setdefault(operating_system, []).append(node)
        return container
