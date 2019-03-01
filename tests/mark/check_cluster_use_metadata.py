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


from ducktape.mark import parametrize, matrix, ignore
from ducktape.mark.mark_expander import MarkedFunctionExpander
from ducktape.mark.resource import cluster
from ducktape.cluster.cluster_spec import ClusterSpec

import pytest


class CheckClusterUseAnnotation(object):

    def check_basic_usage_arbitrary_metadata(self):
        cluster_use_metadata = {
            'a': 2,
            'b': 'hi',
            'num_nodes': 300
        }

        @cluster(**cluster_use_metadata)
        def function():
            return "hi"
        assert hasattr(function, "marks")

        test_context_list = MarkedFunctionExpander(function=function).expand()
        assert len(test_context_list) == 1
        assert test_context_list[0].cluster_use_metadata == cluster_use_metadata

    def check_basic_usage_cluster_spec(self):
        num_nodes = 200

        @cluster(cluster_spec=ClusterSpec.simple_linux(num_nodes))
        def function():
            return "hi"
        assert hasattr(function, "marks")

        test_context_list = MarkedFunctionExpander(function=function).expand()
        assert len(test_context_list) == 1
        assert len(test_context_list[0].expected_cluster_spec.nodes.os_to_nodes) == 1
        assert len(test_context_list[0].expected_cluster_spec.nodes.os_to_nodes.get('linux')) == num_nodes

    def check_basic_usage_num_nodes(self):
        num_nodes = 200

        @cluster(num_nodes=num_nodes)
        def function():
            return "hi"
        assert hasattr(function, "marks")

        test_context_list = MarkedFunctionExpander(function=function).expand()
        assert len(test_context_list) == 1
        assert test_context_list[0].expected_num_nodes == num_nodes

    def check_with_parametrize(self):
        num_nodes = 200

        @cluster(num_nodes=num_nodes)
        @parametrize(x=1)
        def f(x, y=2, z=3):
            return x, y, z

        test_context_list = MarkedFunctionExpander(function=f).expand()
        assert len(test_context_list) == 1
        assert test_context_list[0].expected_num_nodes == num_nodes

    def check_beneath_parametrize(self):
        """ Annotations such as cluster, which add metadata, but do not create new tests, add the metadata to
        all test cases physically below the annotation.

        In the case of a parametrized test, when @cluster has no parametrize annotations below it,
        there are not any test cases to which it will apply, so none of the resulting tests should have
        associated cluster size metadata.
        """
        num_nodes = 200

        @parametrize(x=1)
        @cluster(num_nodes=num_nodes)
        def f(x, y=2, z=3):
            return x, y, z

        with pytest.raises(AssertionError):
            MarkedFunctionExpander(function=f).expand()

    def check_no_override(self):
        """ cluster use metadata should apply to all test cases physically below it, except for those which already
        have cluster use metadata.
        """

        num_nodes_1 = 200
        num_nodes_2 = 42

        # num_nodes_2 should *not* override num_nodes_1
        @cluster(num_nodes=num_nodes_2)
        @cluster(num_nodes=num_nodes_1)
        def f(x, y=2, z=3):
            return x, y, z

        test_context_list = MarkedFunctionExpander(function=f).expand()
        assert len(test_context_list) == 1
        assert test_context_list[0].expected_num_nodes == num_nodes_1

    def check_parametrized_with_multiple_cluster_annotations(self):
        num_nodes_1 = 10
        num_nodes_2 = 20

        # num_nodes_1 should *not* override num_nodes_2
        @cluster(num_nodes=num_nodes_1)
        @parametrize(x=1)
        @parametrize(x=1.5)
        @cluster(num_nodes=num_nodes_2)
        @parametrize(x=2)
        def f(x, y=2, z=3):
            return x, y, z

        test_context_list = MarkedFunctionExpander(function=f).expand()
        assert len(test_context_list) == 3
        assert test_context_list[0].expected_num_nodes == num_nodes_1
        assert test_context_list[1].expected_num_nodes == num_nodes_1
        assert test_context_list[2].expected_num_nodes == num_nodes_2

    def check_matrix_with_multiple_cluster_annotations(self):
        num_nodes_1 = 10
        num_nodes_2 = 20

        # num_nodes_1 should *not* override num_nodes_2
        @cluster(num_nodes=num_nodes_1)
        @matrix(x=[1])
        @matrix(x=[1.5, 1.6], y=[-15, -16])
        @cluster(num_nodes=num_nodes_2)
        @matrix(x=[2])
        def f(x, y=2, z=3):
            return x, y, z

        test_context_list = MarkedFunctionExpander(function=f).expand()
        assert len(test_context_list) == 6
        assert test_context_list[0].expected_num_nodes == num_nodes_1
        assert test_context_list[1].expected_num_nodes == num_nodes_1
        assert test_context_list[2].expected_num_nodes == num_nodes_1
        assert test_context_list[3].expected_num_nodes == num_nodes_1
        assert test_context_list[4].expected_num_nodes == num_nodes_1
        assert test_context_list[5].expected_num_nodes == num_nodes_2

    def check_with_ignore(self):
        num_nodes = 200

        @cluster(num_nodes=num_nodes)
        @ignore
        def function():
            return "hi"
        assert hasattr(function, "marks")

        test_context_list = MarkedFunctionExpander(function=function).expand()
        assert len(test_context_list) == 1
        assert test_context_list[0].expected_num_nodes == num_nodes

        # order shouldn't matter here
        @ignore
        @cluster(num_nodes=num_nodes)
        def function():
            return "hi"
        assert hasattr(function, "marks")

        test_context_list = MarkedFunctionExpander(function=function).expand()
        assert len(test_context_list) == 1
        assert test_context_list[0].expected_num_nodes == num_nodes
