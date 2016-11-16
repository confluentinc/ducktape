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

from ducktape.cluster.finite_subcluster import FiniteSubcluster
import pickle
import pytest


class CheckFiniteSubcluster(object):
    single_node_cluster_json = {"nodes": [{"hostname": "localhost"}]}

    def check_cluster_size(self):
        cluster = FiniteSubcluster([])
        assert len(cluster) == 0

        n = 10
        cluster = FiniteSubcluster([object() for _ in range(n)])
        assert len(cluster) == n

    def check_pickleable(self):
        cluster = FiniteSubcluster([object() for _ in range(10)])
        pickle.dumps(cluster)

    def check_allocate_free(self):
        n = 10
        cluster = FiniteSubcluster([object() for _ in range(n)])
        assert len(cluster) == n
        assert cluster.num_available_nodes() == n

        nodes = cluster.alloc(1)
        assert len(nodes) == 1
        assert len(cluster) == n
        assert cluster.num_available_nodes() == n - 1

        nodes2 = cluster.alloc(2)
        assert len(nodes2) == 2
        assert len(cluster) == n
        assert cluster.num_available_nodes() == n - 3

        cluster.free(nodes)
        assert cluster.num_available_nodes() == n - 2

        cluster.free(nodes2)
        assert cluster.num_available_nodes() == n

    def check_alloc_too_many(self):
        n = 10
        cluster = FiniteSubcluster([object() for _ in range(n)])
        with pytest.raises(AssertionError):
            cluster.alloc(n + 1)

    def check_free_too_many(self):
        n = 10
        cluster = FiniteSubcluster([object() for _ in range(n)])
        nodes = cluster.alloc(n)
        with pytest.raises(AssertionError):
            nodes.append(object())
            cluster.free(nodes)
