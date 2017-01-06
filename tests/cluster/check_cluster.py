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

from ducktape.cluster.cluster import Cluster
from ducktape.cluster.remoteaccount import RemoteAccount

FakeTestContext = collections.namedtuple('FakeTestContext', ['expected_node_spec'])
FakeRemoteAccount = collections.namedtuple('FakeRemoteAccount', ['operating_system'])

class CheckCluster(object):

    def setup_method(self, _):
        self.cluster = Cluster()
        self.cluster._in_use_nodes = []
        self.cluster._available_nodes = [
            FakeRemoteAccount(operating_system=RemoteAccount.LINUX),
            FakeRemoteAccount(operating_system=RemoteAccount.LINUX),
            FakeRemoteAccount(operating_system=RemoteAccount.LINUX),
            FakeRemoteAccount(operating_system=RemoteAccount.WINDOWS),
            FakeRemoteAccount(operating_system=RemoteAccount.WINDOWS),
            FakeRemoteAccount(operating_system=RemoteAccount.WINDOWS)
        ]

        self.test_list = [
            FakeTestContext(expected_node_spec={RemoteAccount.LINUX: 2, RemoteAccount.WINDOWS: 2}),
            FakeTestContext(expected_node_spec={RemoteAccount.LINUX: 5, RemoteAccount.WINDOWS: 2}),
            FakeTestContext(expected_node_spec={RemoteAccount.LINUX: 5, RemoteAccount.WINDOWS: 5}),
            FakeTestContext(expected_node_spec={RemoteAccount.LINUX: 3, RemoteAccount.WINDOWS: 3}),
        ]

    def check_enough_capacity(self):
        assert self.cluster.test_capacity_comparison(self.test_list[0]) > 0

    def check_not_enough_capacity(self):
        assert self.cluster.test_capacity_comparison(self.test_list[1]) < 0
        assert self.cluster.test_capacity_comparison(self.test_list[2]) < 0

    def check_exact_capacity(self):
        assert self.cluster.test_capacity_comparison(self.test_list[3]) == 0
