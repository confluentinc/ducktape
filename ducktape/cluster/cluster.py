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


class ClusterNode(object):
    def __init__(self, account, **kwargs):
        self.account = account
        for k, v in kwargs.items():
            setattr(self, k, v)

    @property
    def name(self):
        return self.account.hostname

    @property
    def operating_system(self):
        return self.account.operating_system


class Cluster(object):
    """ Interface for a cluster -- a collection of nodes with login credentials.
    This interface doesn't define any mapping of roles/services to nodes. It only interacts with some underlying
    system that can describe available resources and mediates reservations of those resources.
    """

    def __init__(self):
        self.max_used_nodes = 0

    def __len__(self):
        """Size of this cluster object. I.e. number of 'nodes' in the cluster."""
        return self.available().size() + self.used().size()

    def alloc(self, cluster_spec):
        """
        Allocate some nodes.

        :param cluster_spec:                    A ClusterSpec describing the nodes to be allocated.
        :throws InsufficientResources:          If the nodes cannot be allocated.
        :return:                                Allocated nodes spec
        """
        allocated = self.do_alloc(cluster_spec)
        self.max_used_nodes = max(self.max_used_nodes, len(self.used()))
        return allocated

    def do_alloc(self, cluster_spec):
        """
        Subclasses should implement actual allocation here.

        :param cluster_spec:                    A ClusterSpec describing the nodes to be allocated.
        :throws InsufficientResources:          If the nodes cannot be allocated.
        :return:                                Allocated nodes spec
        """
        raise NotImplementedError

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

    def num_available_nodes(self):
        return self.available().size()

    def available(self):
        """
        Return a ClusterSpec object describing the currently available nodes.
        """
        raise NotImplementedError

    def used(self):
        """
        Return a ClusterSpec object describing the currently in use nodes.
        """
        raise NotImplementedError

    def max_used(self):
        return self.max_used_nodes

    def all(self):
        """
        Return a ClusterSpec object describing all nodes.
        """
        return self.available().clone().add(self.used())
