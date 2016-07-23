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

TestExpectedNodes = collections.namedtuple('TestExpectedNodes', ['test_context', 'expected_nodes'])


class TestScheduler(object):
    """This class tracks tests which are scheduled to run, and provides an ordering based on the current cluster state.

    The ordering is "on-demand"; calling next returns the largest cluster user which fits in the currently
    available cluster nodes.
    """
    def __init__(self, tc_expected_nodes, cluster):

        self.cluster = cluster
        self._test_context_list = [t.test_context for t in tc_expected_nodes]
        self._expected_nodes = {
            t.test_context.test_id: t.expected_nodes
            for t in tc_expected_nodes
        }
        self._sort_test_context_list()

    def __len__(self):
        return len(self._test_context_list)

    def __iter__(self):
        """This class is iterable"""
        return self

    def _sort_test_context_list(self):
        """Replace self.test_context_list with a sorted shallow copy

        Sort from largest cluster users to smallest
        """
        # sort from largest cluster users to smallest
        self._test_context_list = sorted(self._test_context_list,
                                         key=lambda tc: self._expected_nodes[tc.test_id],
                                         reverse=True)

    def peek(self):
        """Locate and return the next object to be scheduled, without removing it internally.

        :return test_context for the next test to be scheduled
        :raise RuntimeError if the scheduler is empty
        """
        if len(self) == 0:
            raise RuntimeError("No more tests available")

        for tc in self._test_context_list:
            if self._expected_nodes[tc.test_id] <= self.cluster.num_available_nodes():
                return tc

        return None

    def next(self):
        """Get the next test.

        This action removes the test_context object from the scheduler.
        """
        tc = self.peek()
        self._test_context_list.remove(tc)
        return tc

    def put(self, test_context):
        """Enqueue another test"""
        self._test_context_list.append(test_context)
        self._sort_test_context_list()
