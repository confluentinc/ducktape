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

from ducktape.tests.test import TestContext, Test
from ducktape.tests.runner import SerialTestRunner
from ducktape.mark import ignore, parametrize, MarkedFunctionExpander
from ducktape.cluster.localhost import LocalhostCluster

import tests.ducktape_mock

from mock import MagicMock, Mock


class CheckSerialRunner(object):

    def check_insufficient_cluster_resources(self):
        """The test runner should behave sensibly when the cluster is too small to run a given test."""
        mock_cluster = MagicMock()
        mock_cluster.__len__.return_value = 1
        mock_cluster.num_available_nodes = lambda: 1
        session_context = tests.ducktape_mock.session_context(mock_cluster)

        test_context = TestContext(session_context=session_context, module=None, cls=TestThingy, function=TestThingy.test_pi)
        runner = SerialTestRunner(session_context, [test_context])
        runner.log = Mock()

        # Even though the cluster is too small, the test runner should this handle gracefully without raising an error
        results = runner.run_all_tests()
        assert len(results) == 1
        assert results.num_failed == 1
        assert results.num_passed == 0
        assert results.num_ignored == 0

    def check_simple_run(self):
        """Check expected behavior when running a single test."""
        mock_cluster = LocalhostCluster()
        session_context = tests.ducktape_mock.session_context(mock_cluster)

        test_methods = [TestThingy.test_pi, TestThingy.test_ignore1, TestThingy.test_ignore2]
        ctx_list = []
        for f in test_methods:
            ctx_list.extend(MarkedFunctionExpander(session_context=session_context, cls=TestThingy, function=f).expand())

        runner = SerialTestRunner(session_context, ctx_list)
        runner.log = Mock()

        results = runner.run_all_tests()
        assert len(results) == 3
        assert results.num_failed == 0
        assert results.num_passed == 1
        assert results.num_ignored == 2
        assert results[0].data == {"data": 3.14159}


class TestThingy(Test):
    """Fake ducktape test class"""

    def min_cluster_size(self):
        """ This test uses many nodes, wow!"""
        return 1000

    def test_pi(self):
        return {"data": 3.14159}

    @ignore
    def test_ignore1(self):
        pass

    @ignore(x=5)
    @parametrize(x=5)
    def test_ignore2(self, x=2):
        pass
