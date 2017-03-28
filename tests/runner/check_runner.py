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
from tests.ducktape_mock import FakeCluster

import tests.ducktape_mock
from .resources.test_thingy import TestThingy
from .resources.test_failing_tests import FailingTest

from mock import Mock
import os

TEST_THINGY_FILE = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "resources/test_thingy.py"))
FAILING_TEST_FILE = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "resources/test_failing_tests.py"))


class CheckRunner(object):
    def check_insufficient_cluster_resources(self):
        """The test runner should behave sensibly when the cluster is too small to run a given test."""
        mock_cluster = FakeCluster(1)
        session_context = tests.ducktape_mock.session_context()

        test_context = TestContext(session_context=session_context, module=None, cls=TestThingy,
                                   function=TestThingy.test_pi, file=TEST_THINGY_FILE, cluster=mock_cluster)
        runner = TestRunner(mock_cluster, session_context, Mock(), [test_context])

        # Even though the cluster is too small, the test runner should this handle gracefully without raising an error
        results = runner.run_all_tests()
        assert len(results) == 1
        assert results.num_failed == 1
        assert results.num_passed == 0
        assert results.num_ignored == 0

    def check_simple_run(self):
        """Check expected behavior when running a single test."""
        mock_cluster = LocalhostCluster(num_nodes=1000)
        session_context = tests.ducktape_mock.session_context()

        test_methods = [TestThingy.test_pi, TestThingy.test_ignore1, TestThingy.test_ignore2]
        ctx_list = []
        for f in test_methods:
            ctx_list.extend(
                MarkedFunctionExpander(
                    session_context=session_context,
                    cls=TestThingy, function=f, file=TEST_THINGY_FILE, cluster=mock_cluster).expand())

        runner = TestRunner(mock_cluster, session_context, Mock(), ctx_list)

        results = runner.run_all_tests()
        assert len(results) == 3
        assert results.num_failed == 0
        assert results.num_passed == 1
        assert results.num_ignored == 2

        result_with_data = filter(lambda r: r.data is not None, results)[0]
        assert result_with_data.data == {"data": 3.14159}

    def check_exit_first(self):
        """Confirm that exit_first in session context has desired effect of preventing any tests from running
        after the first test failure.
        """
        mock_cluster = LocalhostCluster(num_nodes=1000)
        session_context = tests.ducktape_mock.session_context(**{"exit_first": True})

        test_methods = [FailingTest.test_fail]
        ctx_list = []
        for f in test_methods:
            ctx_list.extend(
                MarkedFunctionExpander(
                    session_context=session_context,
                    cls=FailingTest, function=f, file=FAILING_TEST_FILE, cluster=mock_cluster).expand())

        runner = TestRunner(mock_cluster, session_context, Mock(), ctx_list)
        results = runner.run_all_tests()
        assert len(ctx_list) > 1
        assert len(results) == 1
