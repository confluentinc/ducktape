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

from ducktape.tests.test import TestContext
from ducktape.tests.runner import TestRunner
from ducktape.mark.mark_expander import MarkedFunctionExpander
from ducktape.cluster.localhost import LocalhostCluster

import tests.ducktape_mock
from .resources.test_thingy import TestThingy

from mock import MagicMock, Mock
import os

TEST_THINGY_FILE = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "resources/test_thingy.py"))


class FakeCluster(object):
    def __init__(self, num_nodes):
        self._num_nodes = num_nodes
        self._available_nodes = self._num_nodes

    def __len__(self):
        return self._num_nodes

    def request(self, nslots):
        """Request the specified number of slots, which will be reserved until they are freed by the caller."""
        self._available_nodes -= nslots
        return [object() for _ in range(nslots)]

    def request_subcluster(self, nslots):
        self.request(nslots)
        return FakeCluster(nslots)

    def free_subcluster(self, subcluster):
        self.free(subcluster.request(len(subcluster)))

    def num_available_nodes(self):
        return self._available_nodes

    def free(self, slots):
        self._available_nodes += len(slots)

    def free_single(self, slot):
        self._available_nodes += 1


class CheckRunner(object):
    port = 5556

    def check_insufficient_cluster_resources(self):
        """The test runner should behave sensibly when the cluster is too small to run a given test."""
        mock_cluster = FakeCluster(1)
        session_context = tests.ducktape_mock.session_context()

        test_context = TestContext(session_context=session_context, module=None, cls=TestThingy, function=TestThingy.test_pi,
                                   file=TEST_THINGY_FILE, cluster=mock_cluster)
        runner = TestRunner(mock_cluster, session_context, Mock(), [test_context], port=CheckRunner.port)

        # Even though the cluster is too small, the test runner should this handle gracefully without raising an error
        results = runner.run_all_tests()
        assert len(results) == 1
        assert results.num_failed == 1
        assert results.num_passed == 0
        assert results.num_ignored == 0

    def check_simple_run(self):
        """Check expected behavior when running a single test."""
        mock_cluster = LocalhostCluster()
        session_context = tests.ducktape_mock.session_context()

        test_methods = [TestThingy.test_pi, TestThingy.test_ignore1, TestThingy.test_ignore2]
        ctx_list = []
        for f in test_methods:
            ctx_list.extend(
                MarkedFunctionExpander(
                    session_context=session_context,
                    cls=TestThingy, function=f, file=TEST_THINGY_FILE, cluster=mock_cluster).expand())

        runner = TestRunner(mock_cluster, session_context, Mock(), ctx_list, port=CheckRunner.port)

        results = runner.run_all_tests()
        assert len(results) == 3
        assert results.num_failed == 0
        assert results.num_passed == 1
        assert results.num_ignored == 2

        result_with_data = filter(lambda r: r.data is not None, results)[0]
        assert result_with_data.data == {"data": 3.14159}

    def teardown_method(self, _):
        CheckRunner.port += 1
