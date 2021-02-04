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

from ducktape.cluster.cluster import Cluster
from ducktape.cluster.cluster_spec import ClusterSpec, LINUX
from ducktape.cluster.node_container import NodeContainer
from ducktape.tests.session import SessionContext
from ducktape.tests.test import TestContext
from ducktape.cluster.linux_remoteaccount import LinuxRemoteAccount
from ducktape.cluster.remoteaccount import RemoteAccountSSHConfig
from mock import MagicMock


import os
import tempfile


def mock_cluster():
    return MagicMock()


class FakeClusterNode(object):
    @property
    def operating_system(self):
        return LINUX


class FakeCluster(Cluster):
    """A cluster class with counters, but no actual node objects"""

    def __init__(self, num_nodes):
        super(FakeCluster, self).__init__()
        self._available_nodes = NodeContainer()
        for i in range(0, num_nodes):
            self._available_nodes.add_node(FakeClusterNode())
        self._in_use_nodes = NodeContainer()

    def do_alloc(self, cluster_spec):
        allocated = self._available_nodes.remove_spec(cluster_spec)
        self._in_use_nodes.add_nodes(allocated)
        return allocated

    def free_single(self, node):
        self._in_use_nodes.remove_node(node)
        self._available_nodes.add_node(node)

    def available(self):
        return ClusterSpec.from_nodes(self._available_nodes)

    def used(self):
        return ClusterSpec.from_nodes(self._in_use_nodes)


def session_context(**kwargs):
    """Return a SessionContext object"""

    if "results_dir" not in kwargs.keys():
        tmp = tempfile.mkdtemp()
        session_dir = os.path.join(tmp, "test_dir")
        os.mkdir(session_dir)
        kwargs["results_dir"] = session_dir

    return SessionContext(session_id="test_session", **kwargs)


def test_context(session_context=session_context(), cluster=mock_cluster()):
    """Return a TestContext object"""
    return TestContext(session_context=session_context, file="a/b/c", cluster=cluster)


class MockNode(object):
    """Mock cluster node"""

    def __init__(self):
        self.account = MockAccount()


class MockAccount(LinuxRemoteAccount):
    """Mock node.account object. It's Linux because tests are run in Linux."""

    def __init__(self):
        ssh_config = RemoteAccountSSHConfig(
            host="localhost",
            user=None,
            hostname="localhost",
            port=22)

        super(MockAccount, self).__init__(ssh_config, externally_routable_ip="localhost", logger=None)
