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

from ducktape.mark._mark import Mark


def _is_int(n):
    try:
        int(n) == n
        return True
    except ValueError:
        return False


class ClusterSize(Mark):
    """Provide a hint about cluster use."""
    def __init__(self, num_nodes):
        assert _is_int(num_nodes), "num_nodes is not an int. Can't create a cluster use marker without meaningful num_nodes."
        self.cluster_size = num_nodes

    @property
    def name(self):
        return "RESOURCE_HINT_CLUSTER_USE"

    def apply(self, seed_context, context_list):
        assert len(context_list) > 0, "cluster size annotation is not being applied to any test cases"

        for ctx in context_list:
            if not hasattr(ctx, "_cluster_size") or ctx._cluster_size is None:
                setattr(ctx, "_cluster_size", self.cluster_size)
        return context_list


def cluster_size(num_nodes):
    """Test method decorator used to indicate how many cluster nodes will be used by the given test.

    Example::
        # basic usage
        @cluster_size(10)
        def the_test(...):
            ...

        # parametrized test:
        # both test cases will be marked with cluster_size of 200
        @cluster_size(200)
        @parametrize(x=1)
        @parametrize(x=2)
        def the_test(x):
            ...

        # test case {'x': 1} has cluster size 100, test case {'x': 2} has cluster size 200
        @cluster_size(100)
        @parametrize(x=1)
        @cluster_size(200)
        @parametrize(x=2)
        def the_test(x):
            ...

    """
    def cluster_sizer(f):
        Mark.mark(f, ClusterSize(num_nodes))
        return f

    return cluster_sizer
