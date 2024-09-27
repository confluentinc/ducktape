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
from typing import Iterable, List, Union

from ducktape.cluster.cluster_node import ClusterNode
from ducktape.cluster.cluster_spec import ClusterSpec


class Cluster(object):
    """Interface for a cluster -- a collection of nodes with login credentials.
    This interface doesn't define any mapping of roles/services to nodes. It only interacts with some underlying
    system that can describe available resources and mediates reservations of those resources.
    """

    def __init__(self):
        self.max_used_nodes = 0

    def __len__(self) -> int:
        """Size of this cluster object. I.e. number of 'nodes' in the cluster."""
        return self.available().size() + self.used().size()

    def alloc(self, cluster_spec) -> Union[ClusterNode, List[ClusterNode], "Cluster"]:
        """
        Allocate some nodes.

        :param cluster_spec:                    A ClusterSpec describing the nodes to be allocated.
        :throws InsufficientResources:          If the nodes cannot be allocated.
        :return:                                Allocated nodes spec
        """
        allocated = self.do_alloc(cluster_spec)
        self.max_used_nodes = max(self.max_used_nodes, len(self.used()))
        return allocated

    def do_alloc(self, cluster_spec) -> Union[ClusterNode, List[ClusterNode], "Cluster"]:
        """
        Subclasses should implement actual allocation here.

        :param cluster_spec:                    A ClusterSpec describing the nodes to be allocated.
        :throws InsufficientResources:          If the nodes cannot be allocated.
        :return:                                Allocated nodes spec
        """
        raise NotImplementedError

    def free(self, nodes: Union[Iterable[ClusterNode], ClusterNode]) -> None:
        """Free the given node or list of nodes"""
        if isinstance(nodes, collections.abc.Iterable):
            for s in nodes:
                self.free_single(s)
        else:
            self.free_single(nodes)

    def free_single(self, node: ClusterNode) -> None:
        raise NotImplementedError()

    def __eq__(self, other):
        return other is not None and self.__dict__ == other.__dict__

    def __hash__(self):
        return hash(tuple(sorted(self.__dict__.items())))

    def num_available_nodes(self) -> int:
        return self.available().size()

    def available(self) -> ClusterSpec:
        """
        Return a ClusterSpec object describing the currently available nodes.
        """
        raise NotImplementedError

    def used(self) -> ClusterSpec:
        """
        Return a ClusterSpec object describing the currently in use nodes.
        """
        raise NotImplementedError

    def max_used(self) -> int:
        return self.max_used_nodes

    def all(self):
        """
        Return a ClusterSpec object describing all nodes.
        """
        return self.available().clone().add(self.used())
