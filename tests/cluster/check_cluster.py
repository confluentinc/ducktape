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

import collections

from ducktape.cluster.cluster import ClusterNode
from ducktape.cluster.cluster_spec import NodeSpec, ClusterSpec, LINUX, WINDOWS
from ducktape.cluster.remoteaccount import MachineType
from tests.ducktape_mock import FakeCluster

FakeRemoteAccount = collections.namedtuple('FakeRemoteAccount', ['operating_system', 'machine_type'])


class CheckCluster(object):

    def setup_method(self, _):
        self.cluster = FakeCluster(0)
        self.cluster._available_nodes.add_node(ClusterNode(FakeRemoteAccount(operating_system=LINUX, machine_type=MachineType())))
        self.cluster._available_nodes.add_node(ClusterNode(FakeRemoteAccount(operating_system=LINUX, machine_type=MachineType())))
        self.cluster._available_nodes.add_node(ClusterNode(FakeRemoteAccount(operating_system=WINDOWS, machine_type=MachineType())))
        self.cluster._available_nodes.add_node(ClusterNode(FakeRemoteAccount(operating_system=WINDOWS, machine_type=MachineType())))
        self.cluster._available_nodes.add_node(ClusterNode(FakeRemoteAccount(operating_system=WINDOWS, machine_type=MachineType())))

    def spec(self, linux_nodes, windows_nodes):
        nodes = []
        for i in range(linux_nodes):
            nodes.append(NodeSpec(LINUX))
        for i in range(windows_nodes):
            nodes.append(NodeSpec(WINDOWS))
        return ClusterSpec(nodes)

    def check_enough_capacity(self):
        assert self.cluster.available().nodes.can_remove_spec(self.spec(2, 2))
        assert self.cluster.available().nodes.can_remove_spec(self.spec(2, 3))

    def check_not_enough_capacity(self):
        assert not self.cluster.available().nodes.can_remove_spec(self.spec(5, 2))
        assert not self.cluster.available().nodes.can_remove_spec(self.spec(5, 5))
        assert not self.cluster.available().nodes.can_remove_spec(self.spec(3, 3))
