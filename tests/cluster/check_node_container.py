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
from ducktape.cluster.node_container import NodeContainer, NodeNotPresentError
import pytest

from tests.ducktape_mock import MockAccount


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
