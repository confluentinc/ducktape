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

from ducktape.cluster.cluster import ClusterNode
from ducktape.cluster.cluster_spec import ClusterSpec, NodeSpec, LINUX, WINDOWS
from ducktape.cluster.node_container import NodeContainer, NodeNotPresentError, InsufficientResourcesError, \
    InsufficientHealthyNodesError
import pytest

from ducktape.cluster.remoteaccount import RemoteAccountSSHConfig
from tests.ducktape_mock import MockAccount
from tests.runner.fake_remote_account import FakeRemoteAccount, FakeWindowsRemoteAccount


def fake_account(host, is_available=True):
    return FakeRemoteAccount(ssh_config=RemoteAccountSSHConfig(host=host), is_available=is_available)


def fake_win_account(host, is_available=True):
    return FakeWindowsRemoteAccount(ssh_config=RemoteAccountSSHConfig(host=host), is_available=is_available)


class CheckNodeContainer(object):
    def check_sizes(self):
        empty = NodeContainer()
        assert 0 == empty.size()
        assert 0 == len(empty)
        nodes = [ClusterNode(MockAccount())]
        container = NodeContainer(nodes)
        assert 1 == container.size()
        assert 1 == len(container)

    def check_add_and_remove(self):
        nodes = [ClusterNode(MockAccount()),
                 ClusterNode(MockAccount()),
                 ClusterNode(MockAccount()),
                 ClusterNode(MockAccount()),
                 ClusterNode(MockAccount())]
        container = NodeContainer([])
        assert 0 == len(container)
        container.add_node(nodes[0])
        container.add_node(nodes[1])
        container.add_node(nodes[2])
        container2 = container.clone()
        i = 0
        for node in container:
            assert nodes[i] == node
            i += 1
        assert 3 == len(container)
        container.remove_node(nodes[0])
        with pytest.raises(NodeNotPresentError):
            container.remove_node(nodes[0])
        assert 2 == len(container)
        assert 3 == len(container2)

    def check_remove_single_node_spec(self):
        """Check remove_spec() method - verify a simple happy path of removing a single node"""
        accounts = [fake_account('host1'), fake_account('host2'),
                    fake_win_account('w1'), fake_win_account('w2')]
        container = NodeContainer(accounts)
        one_linux_node_spec = ClusterSpec(nodes=[NodeSpec(LINUX)])
        one_windows_node_spec = ClusterSpec(nodes=[NodeSpec(WINDOWS)])

        def _remove_single_node(one_node_spec, os):
            assert container.can_remove_spec(one_node_spec)
            good_nodes, bad_nodes = container.remove_spec(one_node_spec)
            assert good_nodes and len(good_nodes) == 1
            assert good_nodes[0].os == os
            assert not bad_nodes

        _remove_single_node(one_windows_node_spec, WINDOWS)
        assert len(container.os_to_nodes.get(LINUX)) == 2
        assert len(container.os_to_nodes.get(WINDOWS)) == 1

        _remove_single_node(one_windows_node_spec, WINDOWS)
        assert len(container.os_to_nodes.get(LINUX)) == 2
        assert not container.os_to_nodes.get(WINDOWS)
        assert not container.can_remove_spec(one_windows_node_spec)
        with pytest.raises(InsufficientResourcesError):
            container.remove_spec(one_windows_node_spec)

        _remove_single_node(one_linux_node_spec, LINUX)
        assert len(container.os_to_nodes.get(LINUX)) == 1
        assert not container.os_to_nodes.get(WINDOWS)

        _remove_single_node(one_linux_node_spec, LINUX)
        assert not container.os_to_nodes.get(LINUX)
        assert not container.os_to_nodes.get(WINDOWS)
        assert not container.can_remove_spec(one_linux_node_spec)
        with pytest.raises(InsufficientResourcesError):
            container.remove_spec(one_linux_node_spec)

    @pytest.mark.parametrize("cluster_spec", [
        pytest.param(ClusterSpec(nodes=[NodeSpec(LINUX), NodeSpec(WINDOWS), NodeSpec(WINDOWS)]),
                     id='not enough windows nodes'),
        pytest.param(ClusterSpec(nodes=[NodeSpec(LINUX), NodeSpec(LINUX), NodeSpec(WINDOWS)]),
                     id='not enough linux nodes'),
        pytest.param(ClusterSpec(nodes=[NodeSpec(LINUX), NodeSpec(LINUX), NodeSpec(WINDOWS), NodeSpec(WINDOWS)]),
                     id='not enough nodes'),
    ])
    def check_not_enough_nodes_to_remove(self, cluster_spec):
        """
        Check what happens if there aren't enough resources in this container to match a given spec.
        Various parametrizations check the behavior for when there are enough nodes for one OS but not another,
        or for both.
        """
        accounts = [fake_account('host1'), fake_win_account('w1')]
        container = NodeContainer(accounts)
        original_container = container.clone()

        assert not container.can_remove_spec(cluster_spec)
        assert len(container.attempt_remove_spec(cluster_spec)) > 0

        with pytest.raises(InsufficientResourcesError):
            container.remove_spec(cluster_spec)

        # check that container was not modified
        assert container.os_to_nodes == original_container.os_to_nodes

    @pytest.mark.parametrize("accounts", [
        pytest.param([
            fake_account('host1'), fake_account('host2'),
            fake_win_account('w1'), fake_win_account('w2', is_available=False)
        ], id="windows not available"),
        pytest.param([
            fake_account('host1'), fake_account('host2', is_available=False),
            fake_win_account('w1'), fake_win_account('w2')
        ], id="linux not available"),
        pytest.param([
            fake_account('host1'), fake_account('host2', is_available=False),
            fake_win_account('w1'), fake_win_account('w2', is_available=False)
        ], id="neither is available")
    ])
    def check_not_enough_healthy_nodes(self, accounts):
        """
        When there's not enough healthy nodes in any of the OS-s, we obviously don't want to remove anything.
        Even when there's enough healthy nodes for one of the OS-s, but not enough in another one,
        we don't want to remove any nodes at all.
        Various sets of params check if there aren't enough healthy nodes for one OS but not the other, or both.
        """
        container = NodeContainer(accounts)
        original_container = container.clone()
        expected_bad_nodes = [acc for acc in accounts if not acc.is_available]
        spec = ClusterSpec(nodes=[NodeSpec(LINUX), NodeSpec(LINUX), NodeSpec(WINDOWS), NodeSpec(WINDOWS)])
        assert container.can_remove_spec(spec)
        with pytest.raises(InsufficientHealthyNodesError) as exc_info:
            container.remove_spec(spec)
        assert exc_info.value.bad_nodes == expected_bad_nodes
        # check that no nodes were actually allocated, but unhealthy ones were removed from the cluster
        original_container.remove_nodes(expected_bad_nodes)
        assert container.os_to_nodes == original_container.os_to_nodes

    @pytest.mark.parametrize("accounts", [
        pytest.param([
            fake_account('host1'), fake_account('host2'), fake_account('host3'),
            fake_win_account('w1', is_available=False), fake_win_account('w2'), fake_win_account('w2')
        ], id="windows not available"),
        pytest.param([
            fake_account('host1', is_available=False), fake_account('host2'), fake_account('host3'),
            fake_win_account('w1'), fake_win_account('w2'), fake_win_account('w2')
        ], id="linux not available"),
        pytest.param([
            fake_account('host1', is_available=False), fake_account('host2'), fake_account('host3'),
            fake_win_account('w1', is_available=False), fake_win_account('w2'), fake_win_account('w2')
        ], id="neither is available"),
        pytest.param([
            fake_account('host1'), fake_account('host2'), fake_account('host3'),
            fake_win_account('w1'), fake_win_account('w2'), fake_win_account('w2')
        ], id="all are available")
    ])
    def check_enough_healthy_but_some_bad_nodes_too(self, accounts):
        """
        Check that we can successfully allocate all necessary nodes - even if some nodes don't pass health checks,
        we still have enough nodes to match the provided cluster spec.

        This test assumes that if we do encounter unhealthy node,
        we encounter it before we can finish allocating healthy ones, otherwise it would just mean testing the happy
        path (which we do in one of the params).
        """
        container = NodeContainer(accounts)
        original_container = container.clone()
        expected_bad_nodes = [acc for acc in accounts if not acc.is_available]
        spec = ClusterSpec(nodes=[NodeSpec(LINUX), NodeSpec(LINUX), NodeSpec(WINDOWS), NodeSpec(WINDOWS)])

        assert container.can_remove_spec(spec)
        good_nodes, bad_nodes = container.remove_spec(spec)

        # alloc should succeed
        # check that we did catch a bad node if any
        assert bad_nodes == expected_bad_nodes
        # check that container has exactly the right number of nodes left -
        # we removed len(spec) healthy nodes, plus len(expected_bad_nodes) of unhealthy nodes.
        assert len(container) == len(original_container) - len(spec) - len(expected_bad_nodes)

        # check that we got 2 windows nodes and two linux nodes in response,
        # don't care which ones in particular
        assert len(good_nodes) == 4
        actual_linux = [node for node in good_nodes if node.os == LINUX]
        assert len(actual_linux) == 2
        actual_win = [node for node in good_nodes if node.os == WINDOWS]
        assert len(actual_win) == 2

    def check_empty_cluster_spec(self):
        accounts = [fake_account('host1'), fake_account('host2'), fake_account('host3')]
        container = NodeContainer(accounts)
        spec = ClusterSpec.empty()
        assert not container.attempt_remove_spec(spec)
        assert container.can_remove_spec(spec)
        good, bad = container.remove_spec(spec)
        assert not good
        assert not bad

    def check_none_cluster_spec(self):
        accounts = [fake_account('host1'), fake_account('host2'), fake_account('host3')]
        container = NodeContainer(accounts)
        spec = None
        assert container.attempt_remove_spec(spec)
        assert not container.can_remove_spec(spec)
        with pytest.raises(InsufficientResourcesError):
            container.remove_spec(spec)
