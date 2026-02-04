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

from ducktape.cluster.cluster_spec import ClusterSpec
from ducktape.cluster.node_spec import NodeSpec


class CheckClusterSpec(object):
    def check_cluster_spec_sizes(self):
        simple_linux_2 = ClusterSpec.simple_linux(2)
        assert 2 == len(simple_linux_2)
        assert 0 == len(ClusterSpec.empty())

    def check_to_string(self):
        empty = ClusterSpec.empty()
        assert "[]" == str(empty)
        simple_linux_5 = ClusterSpec.simple_linux(5)
        assert '[{"num_nodes": 5, "os": "linux"}]' == str(simple_linux_5)

    def check_simple_linux_with_node_type(self):
        """Test simple_linux with node_type parameter."""
        spec = ClusterSpec.simple_linux(3, node_type="large")
        assert len(spec) == 3
        for node_spec in spec:
            assert node_spec.operating_system == "linux"
            assert node_spec.node_type == "large"

    def check_simple_linux_without_node_type(self):
        """Test simple_linux without node_type (backward compatibility)."""
        spec = ClusterSpec.simple_linux(2)
        assert len(spec) == 2
        for node_spec in spec:
            assert node_spec.operating_system == "linux"
            assert node_spec.node_type is None

    def check_grouped_by_os_and_type_empty(self):
        """Test grouped_by_os_and_type on empty ClusterSpec via NodeContainer."""
        spec = ClusterSpec.empty()
        grouped = spec.nodes.grouped_by_os_and_type()
        assert grouped == {}

    def check_grouped_by_os_and_type_single_type(self):
        """Test grouped_by_os_and_type with single node type via NodeContainer."""
        spec = ClusterSpec.simple_linux(3, node_type="small")
        grouped = spec.nodes.grouped_by_os_and_type()
        assert grouped == {("linux", "small"): 3}

    def check_grouped_by_os_and_type_mixed(self):
        """Test grouped_by_os_and_type with mixed node types via NodeContainer."""
        spec = ClusterSpec(
            nodes=[
                NodeSpec("linux", node_type="small"),
                NodeSpec("linux", node_type="small"),
                NodeSpec("linux", node_type="large"),
                NodeSpec("linux", node_type=None),
                NodeSpec("windows", node_type="medium"),
            ]
        )
        grouped = spec.nodes.grouped_by_os_and_type()
        assert grouped == {
            ("linux", "small"): 2,
            ("linux", "large"): 1,
            ("linux", None): 1,
            ("windows", "medium"): 1,
        }
