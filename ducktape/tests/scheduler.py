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


class TestScheduler(object):
    """This class tracks tests which are scheduled to run, and provides an ordering based on the current cluster state.

    The ordering is "on-demand"; calling next returns the largest cluster user which fits in the currently
    available cluster nodes.
    """

    def __init__(self, test_contexts, cluster):
        self.cluster = cluster

        # Track tests which would never be offered up by the scheduling algorithm due to insufficient
        # cluster resources
        self._test_context_list = test_contexts.copy()

        self._sort_test_context_list()

    def __len__(self):
        """Number of tests currently in the scheduler"""
        return len(self._test_context_list)

    def __iter__(self):
        return self

    def filter_unschedulable_tests(self):
        """
        Filter out tests that cannot be scheduled with the current cluster, remove them from
        this scheduler and return them.
        """
        all = self.cluster.all()
        unschedulable = []
        for test_context in self._test_context_list:
            if not all.nodes.can_remove_spec(test_context.expected_cluster_spec):
                unschedulable.append(test_context)
        for u in unschedulable:
            self._test_context_list.remove(u)
        return unschedulable

    def _sort_test_context_list(self):
        """Replace self.test_context_list with a sorted shallow copy

        Sort from largest cluster users to smallest
        """
        # sort from the largest cluster users to smallest
        self._test_context_list = sorted(
            self._test_context_list,
            key=lambda tc: tc.expected_num_nodes,
            reverse=True
        )

    def peek(self):
        """Locate and return the next object to be scheduled, without removing it internally.

        :return test_context for the next test to be scheduled.
            If scheduler is empty, or no test can currently be scheduled, return None.
        """
        for tc in self._test_context_list:
            if self.cluster.available().nodes.can_remove_spec(tc.expected_cluster_spec):
                return tc

        return None

    def remove(self, tc):
        """Remove test context object from this scheduler.
        Intended usage is to peek() first, then perform whatever validity checks,
        and if they pass, remove() it from the scheduler.
        """
        if tc:
            self._test_context_list.remove(tc)
