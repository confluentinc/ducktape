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
from ducktape.cluster.consts import LINUX, WINDOWS


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
        """Check that simple_linux() accepts and applies node_type."""
        spec = ClusterSpec.simple_linux(3, node_type="large")
        assert len(spec) == 3

        # All nodes should have the specified node_type
        for node_spec in spec.nodes.elements():
            assert node_spec.operating_system == LINUX
            assert node_spec.node_type == "large"

    def check_simple_linux_without_node_type(self):
        """Check that simple_linux() works without node_type (backward compat)."""
        spec = ClusterSpec.simple_linux(2)
        assert len(spec) == 2

        # All nodes should have node_type=None
        for node_spec in spec.nodes.elements():
            assert node_spec.operating_system == LINUX
            assert node_spec.node_type is None


class CheckNodeSpec(object):
    def check_node_spec_defaults(self):
        """Check NodeSpec default values."""
        spec = NodeSpec()
        assert spec.operating_system == LINUX
        assert spec.node_type is None

    def check_node_spec_with_node_type(self):
        """Check NodeSpec with explicit node_type."""
        spec = NodeSpec(LINUX, node_type="large")
        assert spec.operating_system == LINUX
        assert spec.node_type == "large"

    def check_node_spec_str_with_node_type(self):
        """Check NodeSpec string representation includes node_type."""
        spec = NodeSpec(LINUX, node_type="small")
        spec_str = str(spec)
        assert '"node_type": "small"' in spec_str
        assert '"os": "linux"' in spec_str

    def check_node_spec_str_without_node_type(self):
        """Check NodeSpec string representation without node_type."""
        spec = NodeSpec(LINUX)
        spec_str = str(spec)
        assert "node_type" not in spec_str
        assert '"os": "linux"' in spec_str

    def check_node_spec_matches_exact(self):
        """Check NodeSpec.matches() with exact match."""
        requested = NodeSpec(LINUX, node_type="large")
        available = NodeSpec(LINUX, node_type="large")
        assert requested.matches(available)

    def check_node_spec_matches_different_type(self):
        """Check NodeSpec.matches() with different node_type."""
        requested = NodeSpec(LINUX, node_type="large")
        available = NodeSpec(LINUX, node_type="small")
        assert not requested.matches(available)

    def check_node_spec_matches_different_os(self):
        """Check NodeSpec.matches() with different OS."""
        requested = NodeSpec(LINUX, node_type="large")
        available = NodeSpec(WINDOWS, node_type="large")
        assert not requested.matches(available)

    def check_node_spec_matches_none_requested(self):
        """Check NodeSpec.matches() when requested has no node_type (matches any)."""
        requested = NodeSpec(LINUX, node_type=None)
        available_small = NodeSpec(LINUX, node_type="small")
        available_large = NodeSpec(LINUX, node_type="large")
        available_none = NodeSpec(LINUX, node_type=None)

        # None requested should match any available type
        assert requested.matches(available_small)
        assert requested.matches(available_large)
        assert requested.matches(available_none)

    def check_node_spec_matches_none_available(self):
        """Check NodeSpec.matches() when available has no node_type."""
        requested_large = NodeSpec(LINUX, node_type="large")
        available = NodeSpec(LINUX, node_type=None)

        # Requesting specific type should NOT match available None
        assert not requested_large.matches(available)

    def check_node_spec_equality(self):
        """Check NodeSpec equality."""
        spec1 = NodeSpec(LINUX, node_type="large")
        spec2 = NodeSpec(LINUX, node_type="large")
        spec3 = NodeSpec(LINUX, node_type="small")
        spec4 = NodeSpec(WINDOWS, node_type="large")

        assert spec1 == spec2
        assert spec1 != spec3
        assert spec1 != spec4

    def check_node_spec_hash(self):
        """Check NodeSpec is hashable and can be used in sets/dicts."""
        spec1 = NodeSpec(LINUX, node_type="large")
        spec2 = NodeSpec(LINUX, node_type="large")
        spec3 = NodeSpec(LINUX, node_type="small")

        # Same specs should have same hash
        assert hash(spec1) == hash(spec2)

        # Can be used in a set
        spec_set = {spec1, spec2, spec3}
        assert len(spec_set) == 2  # spec1 and spec2 are duplicates
