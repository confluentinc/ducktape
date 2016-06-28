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
from ducktape.mark._mark import MarkedFunctionExpander
from ducktape.mark import resource

import pytest


class CheckClusterSizeAnnotation(object):

    def check_basic_usage(self):
        cluster_size = 200

        @resource.cluster_size(cluster_size)
        def function():
            return "hi"
        assert hasattr(function, "marks")

        test_context_list = MarkedFunctionExpander(function=function).expand()
        assert len(test_context_list) == 1
        assert test_context_list[0].cluster_size == cluster_size

    def check_with_parametrize(self):
        cluster_size = 200

        @resource.cluster_size(cluster_size)
        @parametrize(x=1)
        def f(x, y=2, z=3):
            return x, y, z

        test_context_list = MarkedFunctionExpander(function=f).expand()
        assert len(test_context_list) == 1
        assert test_context_list[0].cluster_size == cluster_size

    def check_beneath_parametrize(self):
        """ Annotations such as cluster_size, which add metadata, but do not create new tests, add the metadata to
        all test cases physically below the annotation.

        In the case of a parametrized test, when @cluster_size has no parametrize annotations below it,
        there are not any test cases to which it will apply, so none of the resulting tests should have
        associated cluster size metadata.
        """
        cluster_size = 200

        @parametrize(x=1)
        @resource.cluster_size(cluster_size)
        def f(x, y=2, z=3):
            return x, y, z

        with pytest.raises(AssertionError):
            MarkedFunctionExpander(function=f).expand()

    def check_no_override(self):
        """ cluster_size metadata should apply to all test cases physically below it, except for those which already
        have cluster_size metadata.
        """

        cluster_size_1 = 200
        cluster_size_2 = 42

        # cluster_size_2 should *not* override cluster_size_1
        @resource.cluster_size(cluster_size_2)
        @resource.cluster_size(cluster_size_1)
        def f(x, y=2, z=3):
            return x, y, z

        test_context_list = MarkedFunctionExpander(function=f).expand()
        assert len(test_context_list) == 1
        assert test_context_list[0].cluster_size == cluster_size_1

    def check_parametrized_with_multiple_cluster_size(self):
        cluster_size_1 = 10
        cluster_size_2 = 20

        # cluster_size_2 should *not* override cluster_size_1
        @resource.cluster_size(cluster_size_1)
        @parametrize(x=1)
        @parametrize(x=1.5)
        @resource.cluster_size(cluster_size_2)
        @parametrize(x=2)
        def f(x, y=2, z=3):
            return x, y, z

        test_context_list = MarkedFunctionExpander(function=f).expand()
        assert len(test_context_list) == 3
        assert test_context_list[0].cluster_size == cluster_size_1
        assert test_context_list[1].cluster_size == cluster_size_1
        assert test_context_list[2].cluster_size == cluster_size_2

    def check_matrix_with_multiple_cluster_size(self):
        cluster_size_1 = 10
        cluster_size_2 = 20

        # cluster_size_2 should *not* override cluster_size_1
        @resource.cluster_size(cluster_size_1)
        @matrix(x=[1])
        @matrix(x=[1.5, 1.6], y=[-15, -16])
        @resource.cluster_size(cluster_size_2)
        @matrix(x=[2])
        def f(x, y=2, z=3):
            return x, y, z

        test_context_list = MarkedFunctionExpander(function=f).expand()
        assert len(test_context_list) == 6
        assert test_context_list[0].cluster_size == cluster_size_1
        assert test_context_list[1].cluster_size == cluster_size_1
        assert test_context_list[2].cluster_size == cluster_size_1
        assert test_context_list[3].cluster_size == cluster_size_1
        assert test_context_list[4].cluster_size == cluster_size_1
        assert test_context_list[5].cluster_size == cluster_size_2

    def check_with_ignore(self):
        cluster_size = 200

        @resource.cluster_size(cluster_size)
        @ignore
        def function():
            return "hi"
        assert hasattr(function, "marks")

        test_context_list = MarkedFunctionExpander(function=function).expand()
        assert len(test_context_list) == 1
        assert test_context_list[0].cluster_size == cluster_size

        # order shoudn't matter here
        @ignore
        @resource.cluster_size(cluster_size)
        def function():
            return "hi"
        assert hasattr(function, "marks")

        test_context_list = MarkedFunctionExpander(function=function).expand()
        assert len(test_context_list) == 1
        assert test_context_list[0].cluster_size == cluster_size
