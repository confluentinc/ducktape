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

import copy
from typing import Callable, List

from ducktape.mark._mark import Mark
from ducktape.tests.test_context import TestContext


class ClusterUseMetadata(Mark):
    """Provide a hint about how a given test will use the cluster."""

    def __init__(self, **kwargs) -> None:
        # shallow copy
        self.metadata = copy.copy(kwargs)

    @property
    def name(self) -> str:
        return "RESOURCE_HINT_CLUSTER_USE"

    def apply(self, seed_context: TestContext, context_list: List[TestContext]) -> List[TestContext]:
        assert len(context_list) > 0, "cluster use annotation is not being applied to any test cases"

        for ctx in context_list:
            if not ctx.cluster_use_metadata:
                # only update if non-None and non-empty
                ctx.cluster_use_metadata = self.metadata
        return context_list


def cluster(**kwargs) -> Callable:
    """Test method decorator used to provide hints about how the test will use the given cluster.

    If this decorator is not provided, the test will either claim all cluster resources or fail immediately,
    depending on the flags passed to ducktape.


    :Keywords used by ducktape:

        - ``num_nodes`` provide hint about how many nodes the test will consume
        - ``node_type`` provide hint about what type of nodes the test needs (e.g., "large", "small")
        - ``cluster_spec`` provide hint about how many nodes of each type the test will consume


    Example::

        # basic usage with num_nodes
        @cluster(num_nodes=10)
        def the_test(...):
            ...

        # usage with num_nodes and node_type (Approach 1: single type for entire test)
        @cluster(num_nodes=5, node_type="large")
        def the_test(...):
            ...

        # basic usage with cluster_spec
        @cluster(cluster_spec=ClusterSpec.simple_linux(10))
        def the_test(...):
            ...

        # parametrized test:
        # both test cases will be marked with cluster_size of 200
        @cluster(num_nodes=200)
        @parametrize(x=1)
        @parametrize(x=2)
        def the_test(x):
            ...

        # test case {'x': 1} has cluster size 100, test case {'x': 2} has cluster size 200
        @cluster(num_nodes=100)
        @parametrize(x=1)
        @cluster(num_nodes=200)
        @parametrize(x=2)
        def the_test(x):
            ...

    """

    def cluster_use_metadata_adder(f):
        Mark.mark(f, ClusterUseMetadata(**kwargs))
        return f

    return cluster_use_metadata_adder
