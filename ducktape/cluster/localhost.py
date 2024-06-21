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

from ducktape.cluster.cluster_spec import ClusterSpec
from ducktape.cluster.node_container import NodeContainer
from .cluster import Cluster, ClusterNode
from .linux_remoteaccount import LinuxRemoteAccount
from .remoteaccount import RemoteAccountSSHConfig


class LocalhostCluster(Cluster):
    """
    A "cluster" that runs entirely on localhost using default credentials. This doesn't require any user
    configuration and is equivalent to the old defaults in cluster_config.json. There are no constraints
    on the resources available.
    """

    def __init__(self, *args, **kwargs):
        super(LocalhostCluster, self).__init__()
        num_nodes = kwargs.get("num_nodes", 1000)
        self._available_nodes = NodeContainer()
        for i in range(num_nodes):
            ssh_config = RemoteAccountSSHConfig("localhost%d" % i, hostname="localhost", port=22)
            self._available_nodes.add_node(ClusterNode(
                LinuxRemoteAccount(ssh_config,
                                   ssh_exception_checks=kwargs.get("ssh_exception_checks"))))
        self._in_use_nodes = NodeContainer()

    def do_alloc(self, cluster_spec):
        # there shouldn't be any bad nodes in localhost cluster
        # since ClusterNode object does not implement `available()` method
        good_nodes, bad_nodes = self._available_nodes.remove_spec(cluster_spec)
        self._in_use_nodes.add_nodes(good_nodes)
        return good_nodes

    def free_single(self, node):
        self._in_use_nodes.remove_node(node)
        self._available_nodes.add_node(node)
        node.account.close()

    def available(self):
        return ClusterSpec.from_nodes(self._available_nodes)

    def used(self):
        return ClusterSpec.from_nodes(self._in_use_nodes)
