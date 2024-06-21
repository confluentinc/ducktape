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


import collections

from ducktape.cluster.cluster_spec import ClusterSpec
from tests.ducktape_mock import FakeCluster
from ducktape.tests.scheduler import TestScheduler
from ducktape.services.service import Service

FakeContext = collections.namedtuple('FakeContext', ['test_id', 'expected_num_nodes', 'expected_cluster_spec'])


class CheckScheduler(object):
    def setup_method(self, _):
        self.cluster = FakeCluster(100)
        self.tc0 = FakeContext(0, expected_num_nodes=10, expected_cluster_spec=ClusterSpec.simple_linux(10))
        self.tc1 = FakeContext(1, expected_num_nodes=50, expected_cluster_spec=ClusterSpec.simple_linux(50))
        self.tc2 = FakeContext(2, expected_num_nodes=100, expected_cluster_spec=ClusterSpec.simple_linux(100))
        self.tc_list = [
            self.tc0,
            self.tc1,
            self.tc2,
        ]

    def check_empty(self):
        """Check expected behavior of empty scheduler."""
        scheduler = TestScheduler([], self.cluster)

        assert len(scheduler) == 0
        assert scheduler.peek() is None

    def check_non_empty_cluster_too_small(self):
        """Ensure that scheduler does not return tests if the cluster does not have enough available nodes. """

        scheduler = TestScheduler(self.tc_list, self.cluster)
        assert len(scheduler) == len(self.tc_list)
        assert scheduler.peek() is not None

        # alloc all cluster nodes so none are available
        self.cluster.alloc(Service.setup_cluster_spec(num_nodes=len(self.cluster)))
        assert self.cluster.num_available_nodes() == 0

        # peeking should not yield an object
        assert scheduler.peek() is None

    def check_simple_usage(self):
        """Check usage with fully available cluster."""

        scheduler = TestScheduler(self.tc_list, self.cluster)

        c = 2
        while len(scheduler) > 0:
            t = scheduler.peek()
            assert t.test_id == c
            assert len(scheduler) == c + 1

            scheduler.remove(t)
            assert len(scheduler) == c

            c -= 1

    def check_with_changing_cluster_availability(self):
        """Modify cluster usage in between calls to next() """

        scheduler = TestScheduler(self.tc_list, self.cluster)

        # start with 100-node cluster (configured in setup_method())
        # allocate 60 nodes; only test_id 0 (which needs 10 nodes) should be available
        nodes = self.cluster.alloc(Service.setup_cluster_spec(num_nodes=60))
        assert self.cluster.num_available_nodes() == 40
        t = scheduler.peek()
        assert t == self.tc0
        scheduler.remove(t)
        assert scheduler.peek() is None

        # return 10 nodes, so 50 are available in the cluster
        # next test from the scheduler should be test id 1 (which needs 50 nodes)
        return_nodes = nodes[: 10]
        keep_nodes = nodes[10:]
        self.cluster.free(return_nodes)
        assert self.cluster.num_available_nodes() == 50
        t = scheduler.peek()
        assert t == self.tc1
        scheduler.remove(t)
        assert scheduler.peek() is None

        # return remaining nodes, so cluster is fully available
        # next test from scheduler should be test id 2 (which needs 100 nodes)
        return_nodes = keep_nodes
        self.cluster.free(return_nodes)
        assert self.cluster.num_available_nodes() == len(self.cluster)
        t = scheduler.peek()
        assert t == self.tc2
        scheduler.remove(t)
        # scheduler should become empty now
        assert len(scheduler) == 0
        assert scheduler.peek() is None

    def check_filter_unschedulable_tests(self):
        self.cluster = FakeCluster(49)  # only test id 0 can be scheduled on this cluster
        scheduler = TestScheduler(self.tc_list, self.cluster)

        unschedulable = scheduler.filter_unschedulable_tests()
        assert set(unschedulable) == {self.tc1, self.tc2}
        # subsequent calls should return empty list and not modify scheduler
        assert not scheduler.filter_unschedulable_tests()
        assert scheduler.peek() == self.tc0
