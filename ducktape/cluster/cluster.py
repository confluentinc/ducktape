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

import collections
from .remoteaccount import RemoteAccount


class ClusterSlot(object):
    def __init__(self, account, **kwargs):
        self.account = account
        for k, v in kwargs.items():
            setattr(self, k, v)

    @property
    def operating_system(self):
        return self.account.operating_system


class Cluster(object):
    """ Interface for a cluster -- a collection of nodes with login credentials.
    This interface doesn't define any mapping of roles/services to nodes. It only interacts with some underlying
    system that can describe available resources and mediates reservations of those resources. This is intentionally
    simple right now: the only "resource" right now is a generic VM and it is assumed everything is approximately
    homogeneous.
    """

    def __len__(self):
        """Size of this cluster object. I.e. number of 'nodes' in the cluster."""
        raise NotImplementedError()

    def alloc(self, node_spec):
        """Try to allocate the specified number of nodes, which will be reserved until they are freed by the caller."""
        raise NotImplementedError()

    def request(self, num_nodes):
        """Identical to alloc. Keeping for compatibility"""
        return self.alloc(num_nodes)

    def free(self, nodes):
        """Free the given node or list of nodes"""
        if isinstance(nodes, collections.Iterable):
            for s in nodes:
                self.free_single(s)
        else:
            self.free_single(nodes)

    def free_single(self, node):
        raise NotImplementedError()

    def __eq__(self, other):
        return other is not None and self.__dict__ == other.__dict__

    def __hash__(self):
        return hash(tuple(sorted(self.__dict__.items())))

    def num_nodes_for_operating_system(self, operating_system):
        return self.in_use_nodes_for_operating_system(operating_system) + \
               self.num_available_nodes(operating_system=operating_system)

    def num_available_nodes(self, operating_system=RemoteAccount.LINUX):
        """Number of available nodes."""
        return Cluster._node_count_helper(self._available_nodes, operating_system)

    def in_use_nodes_for_operating_system(self, operating_system):
        return Cluster._node_count_helper(self._in_use_nodes, operating_system)

    @property
    def node_spec(self):
        node_spec = {}
        for operating_system in RemoteAccount.SUPPORTED_OS_TYPES:
            node_spec[operating_system] = self.num_nodes_for_operating_system(operating_system)
        return node_spec

    def test_capacity_comparison(self, test):
        """
        Checks if the test can 'fit' into the cluster, using the test's node_spec.

        A negative return value (int) means the test needs more capacity than is available in the cluster.
        A return value of 0 means the cluster has exactly the right number of resources as the test needs.
        A positive return value (int) means the cluster has more capacity than the test needs.
        """
        num_available = 0
        for (operating_system, node_count) in test.expected_node_spec.iteritems():
            if node_count > self.num_nodes_for_operating_system(operating_system):
                # return -1 immediately if the cluster doesn't have enough capacity for any operating system.
                return -1
            else:
                num_available += self.num_nodes_for_operating_system(operating_system) - node_count

        return num_available

    @staticmethod
    def _node_count_helper(nodes, operating_system):
        return len([node for node in nodes if node.operating_system == operating_system])

    @staticmethod
    def _next_available_node(nodes, operating_system):
        return next(node for node in nodes if node.operating_system == operating_system)
