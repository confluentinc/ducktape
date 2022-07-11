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

from unittest.mock import patch

import pytest

from ducktape.cluster.node_container import NodeContainer, InsufficientResourcesError
from ducktape.tests.runner_client import RunnerClient
from ducktape.tests.status import PASS, FAIL
from ducktape.tests.test import TestContext
from ducktape.tests.runner import TestRunner
from ducktape.mark.mark_expander import MarkedFunctionExpander
from ducktape.cluster.localhost import LocalhostCluster
from tests.ducktape_mock import FakeCluster

import tests.ducktape_mock
from tests.runner.resources.test_fails_to_init import FailsToInitTest
from tests.runner.resources.test_fails_to_init_in_setup import FailsToInitInSetupTest
from .resources.test_bad_actor import BadActorTest
from .resources.test_thingy import ClusterTestThingy, TestThingy
from .resources.test_failing_tests import FailingTest
from ducktape.tests.reporter import JUnitReporter
from ducktape.errors import TimeoutError

from mock import Mock
import os
import xml.etree.ElementTree as ET

from .resources.test_various_num_nodes import VariousNumNodesTest

TEST_THINGY_FILE = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "resources/test_thingy.py"))
FAILING_TEST_FILE = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "resources/test_failing_tests.py"))
FAILS_TO_INIT_TEST_FILE = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "resources/test_fails_to_init.py"))
FAILS_TO_INIT_IN_SETUP_TEST_FILE = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "resources/test_fails_to_init_in_setup.py"))
VARIOUS_NUM_NODES_TEST_FILE = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "resources/test_various_num_nodes.py"))
BAD_ACTOR_TEST_FILE = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "resources/test_bad_actor.py"))


class CheckRunner(object):
    def check_insufficient_cluster_resources(self):
        """The test runner should behave sensibly when the cluster is too small to run a given test."""
        mock_cluster = FakeCluster(1)
        session_context = tests.ducktape_mock.session_context()

        test_context = TestContext(session_context=session_context, module=None, cls=TestThingy,
                                   function=TestThingy.test_pi, file=TEST_THINGY_FILE, cluster=mock_cluster,
                                   cluster_use_metadata={'num_nodes': 1000})
        runner = TestRunner(mock_cluster, session_context, Mock(), [test_context], 1)

        # Even though the cluster is too small, the test runner should this handle gracefully without raising an error
        results = runner.run_all_tests()
        assert len(results) == 1
        assert results.num_failed == 1
        assert results.num_passed == 0
        assert results.num_ignored == 0

    def _do_expand(self, test_file, test_class, test_methods, cluster=None, session_context=None):
        ctx_list = []
        for f in test_methods:
            ctx_list.extend(
                MarkedFunctionExpander(
                    session_context=session_context,
                    cls=test_class, function=f, file=test_file, cluster=cluster).expand())
        return ctx_list

    def check_simple_run(self):
        """Check expected behavior when running a single test."""
        mock_cluster = LocalhostCluster(num_nodes=1000)
        session_context = tests.ducktape_mock.session_context()

        test_methods = [TestThingy.test_pi, TestThingy.test_ignore1, TestThingy.test_ignore2, TestThingy.test_failure]
        ctx_list = self._do_expand(test_file=TEST_THINGY_FILE, test_class=TestThingy, test_methods=test_methods,
                                   cluster=mock_cluster, session_context=session_context)
        runner = TestRunner(mock_cluster, session_context, Mock(), ctx_list, 1)

        results = runner.run_all_tests()
        assert len(results) == 4
        assert results.num_flaky == 0
        assert results.num_failed == 1
        assert results.num_passed == 1
        assert results.num_ignored == 2

        result_with_data = [r for r in results if r.data is not None][0]
        assert result_with_data.data == {"data": 3.14159}

    def check_deflake_run(self):
        """Check expected behavior when running a single test."""
        mock_cluster = LocalhostCluster(num_nodes=1000)
        session_context = tests.ducktape_mock.session_context()

        test_methods = [TestThingy.test_flaky, TestThingy.test_failure]
        ctx_list = self._do_expand(test_file=TEST_THINGY_FILE, test_class=TestThingy, test_methods=test_methods,
                                   cluster=mock_cluster, session_context=session_context)
        runner = TestRunner(mock_cluster, session_context, Mock(), ctx_list, 2)

        results = runner.run_all_tests()
        assert len(results) == 2
        assert results.num_flaky == 1
        assert results.num_failed == 1
        assert results.num_passed == 0
        assert results.num_ignored == 0

    def check_runner_report_junit(self):
        """Check we can serialize results into a xunit xml format. Also ensures that the XML report
        adheres to the Junit spec using xpath queries"""
        mock_cluster = LocalhostCluster(num_nodes=1000)
        session_context = tests.ducktape_mock.session_context()
        test_methods = [TestThingy.test_pi, TestThingy.test_ignore1, TestThingy.test_ignore2, TestThingy.test_failure]
        ctx_list = self._do_expand(test_file=TEST_THINGY_FILE, test_class=TestThingy, test_methods=test_methods,
                                   cluster=mock_cluster, session_context=session_context)
        runner = TestRunner(mock_cluster, session_context, Mock(), ctx_list, 1)

        results = runner.run_all_tests()
        JUnitReporter(results).report()
        xml_report = os.path.join(session_context.results_dir, "report.xml")
        assert os.path.exists(xml_report)
        tree = ET.parse(xml_report)
        assert len(tree.findall('./testsuite/testcase/failure')) == 1
        assert len(tree.findall('./testsuite/testcase/skipped')) == 2
        assert len(tree.findall('./testsuite/testcase')) == 4

        passed = tree.findall("./testsuite/testcase/[@status='pass']")
        assert len(passed) == 1
        assert passed[0].get("classname") == "TestThingy"
        assert passed[0].get("name") == "test_pi"

        failures = tree.findall("./testsuite/testcase/[@status='fail']")
        assert len(failures) == 1
        assert failures[0].get("classname") == "TestThingy"
        assert failures[0].get("name") == "test_failure"

        ignores = tree.findall("./testsuite/testcase/[@status='ignore']")
        assert len(ignores) == 2
        assert ignores[0].get("classname") == "TestThingy"
        assert ignores[1].get("classname") == "TestThingy"

        assert ignores[0].get("name") == "test_ignore1"
        assert ignores[1].get("name") == "test_ignore2.x=5"

    def check_exit_first(self):
        """Confirm that exit_first in session context has desired effect of preventing any tests from running
        after the first test failure.
        """
        mock_cluster = LocalhostCluster(num_nodes=1000)
        session_context = tests.ducktape_mock.session_context(**{"exit_first": True})

        test_methods = [FailingTest.test_fail]
        ctx_list = self._do_expand(test_file=FAILING_TEST_FILE, test_class=FailingTest, test_methods=test_methods,
                                   cluster=mock_cluster, session_context=session_context)
        runner = TestRunner(mock_cluster, session_context, Mock(), ctx_list, 1)
        results = runner.run_all_tests()
        assert len(ctx_list) > 1
        assert len(results) == 1

    def check_exits_if_failed_to_initialize(self):
        """Validate that runner exits correctly when tests failed to initialize.
        """
        mock_cluster = LocalhostCluster(num_nodes=1000)
        session_context = tests.ducktape_mock.session_context()

        ctx_list = self._do_expand(test_file=FAILS_TO_INIT_TEST_FILE, test_class=FailsToInitTest,
                                   test_methods=[FailsToInitTest.test_nothing],
                                   cluster=mock_cluster, session_context=session_context)
        ctx_list.extend(self._do_expand(test_file=FAILS_TO_INIT_IN_SETUP_TEST_FILE, test_class=FailsToInitInSetupTest,
                                        test_methods=[FailsToInitInSetupTest.test_nothing],
                                        cluster=mock_cluster, session_context=session_context))

        runner = TestRunner(mock_cluster, session_context, Mock(), ctx_list, 1)
        results = runner.run_all_tests()
        # These tests fail to initialize, each class has two test methods, so should have 4 results, all failed
        assert len(results) == 4
        assert results.num_flaky == 0
        assert results.num_failed == 4
        assert results.num_passed == 0
        assert results.num_ignored == 0

    # mock an error reporting test failure - this should not prevent subsequent tests from execution and mark
    # failed test as failed correctly
    @patch.object(RunnerClient, '_exc_msg', side_effect=Exception)
    def check_sends_result_when_error_reporting_exception(self, exc_msg_mock):
        """Validates that an error when reporting an exception in the test doesn't prevent subsequent tests
        from executing"""
        mock_cluster = LocalhostCluster(num_nodes=1000)
        session_context = tests.ducktape_mock.session_context()
        test_methods = [TestThingy.test_failure, TestThingy.test_pi]
        ctx_list = self._do_expand(test_file=TEST_THINGY_FILE, test_class=TestThingy, test_methods=test_methods,
                                   cluster=mock_cluster, session_context=session_context)
        runner = TestRunner(mock_cluster, session_context, Mock(), ctx_list, 1)

        results = runner.run_all_tests()
        assert len(results) == 2
        assert results.num_flaky == 0
        assert results.num_failed == 1
        assert results.num_passed == 1
        assert results.num_ignored == 0

    def check_run_failure_with_bad_cluster_allocation(self):
        """Check test should be marked failed if it under-utilizes the cluster resources."""
        mock_cluster = LocalhostCluster(num_nodes=1000)
        session_context = tests.ducktape_mock.session_context(
            fail_bad_cluster_utilization="fail_bad_cluster_utilization")

        ctx_list = self._do_expand(test_file=TEST_THINGY_FILE, test_class=ClusterTestThingy,
                                   test_methods=[ClusterTestThingy.test_bad_num_nodes], cluster=mock_cluster,
                                   session_context=session_context)
        runner = TestRunner(mock_cluster, session_context, Mock(), ctx_list, 1)

        results = runner.run_all_tests()

        assert len(results) == 1
        assert results.num_flaky == 0
        assert results.num_failed == 1
        assert results.num_passed == 0
        assert results.num_ignored == 0

    def check_test_failure_with_too_many_nodes_requested(self):
        mock_cluster = LocalhostCluster(num_nodes=1000)
        session_context = tests.ducktape_mock.session_context(debug=True)

        ctx_list = self._do_expand(test_file=BAD_ACTOR_TEST_FILE, test_class=BadActorTest,
                                   test_methods=[BadActorTest.test_too_many_nodes],
                                   cluster=mock_cluster, session_context=session_context)
        ctx_list.extend(self._do_expand(test_file=VARIOUS_NUM_NODES_TEST_FILE, test_class=VariousNumNodesTest,
                                        test_methods=[VariousNumNodesTest.test_one_node_a],
                                        cluster=mock_cluster, session_context=session_context))
        runner = TestRunner(mock_cluster, session_context, Mock(), ctx_list, 1)
        results = runner.run_all_tests()
        assert results.num_flaky == 0
        assert results.num_failed == 1
        assert results.num_passed == 1
        assert results.num_ignored == 0
        passed = [r for r in results if r.test_status == PASS]
        failed = [r for r in results if r.test_status == FAIL]
        assert passed[0].test_id == 'tests.runner.resources.test_various_num_nodes.VariousNumNodesTest.test_one_node_a'
        assert failed[0].test_id == 'tests.runner.resources.test_bad_actor.BadActorTest.test_too_many_nodes'

    def check_runner_timeout(self):
        """Check process cleanup and error handling in a parallel runner client run."""
        mock_cluster = LocalhostCluster(num_nodes=1000)
        session_context = tests.ducktape_mock.session_context(max_parallel=1000, test_runner_timeout=1)

        test_methods = [TestThingy.test_delayed, TestThingy.test_failure]
        ctx_list = self._do_expand(test_file=TEST_THINGY_FILE, test_class=TestThingy, test_methods=test_methods,
                                   cluster=mock_cluster, session_context=session_context)
        runner = TestRunner(mock_cluster, session_context, Mock(), ctx_list, 1)

        with pytest.raises(TimeoutError):
            runner.run_all_tests()

        assert not runner._client_procs

    @pytest.mark.parametrize('fail_greedy_tests', [True, False])
    def check_fail_greedy_tests(self, fail_greedy_tests):
        mock_cluster = LocalhostCluster(num_nodes=1000)
        session_context = tests.ducktape_mock.session_context(fail_greedy_tests=fail_greedy_tests)

        test_methods = [
            VariousNumNodesTest.test_empty_cluster_annotation,
            VariousNumNodesTest.test_no_cluster_annotation,
            VariousNumNodesTest.test_zero_nodes
        ]
        ctx_list = self._do_expand(test_file=VARIOUS_NUM_NODES_TEST_FILE, test_class=VariousNumNodesTest,
                                   test_methods=test_methods,
                                   cluster=mock_cluster, session_context=session_context)
        runner = TestRunner(mock_cluster, session_context, Mock(), ctx_list, 1)
        results = runner.run_all_tests()
        assert results.num_flaky == 0
        assert results.num_failed == (2 if fail_greedy_tests else 0)
        # zero-node test should always pass, whether we fail on greedy or not
        assert results.num_passed == (1 if fail_greedy_tests else 3)
        assert results.num_ignored == 0

    def check_cluster_shrink(self):
        """
        Check what happens if cluster loses a node while the runner is already running.
        SchedulerTestThingy has two 5-node tests, and one of each for 4, 3, and 2 nodes.

        Thus both 5-node tests should pass, first one failing during pre-allocation phase,
        second one shouldn't even attempt to be allocated.
        And all the other tests should pass still.
        """

        mock_cluster = ShrinkingLocalhostCluster(num_nodes=5)
        session_context = tests.ducktape_mock.session_context(max_parallel=10)

        test_methods = [
            VariousNumNodesTest.test_five_nodes_a,
            VariousNumNodesTest.test_five_nodes_b,
            VariousNumNodesTest.test_four_nodes,
            VariousNumNodesTest.test_three_nodes_a,
            VariousNumNodesTest.test_two_nodes_a
        ]

        ctx_list = self._do_expand(test_file=VARIOUS_NUM_NODES_TEST_FILE, test_class=VariousNumNodesTest,
                                   test_methods=test_methods, cluster=mock_cluster,
                                   session_context=session_context)

        runner = TestRunner(mock_cluster, session_context, Mock(), ctx_list, 1)
        results = runner.run_all_tests()

        assert len(results) == 5
        assert results.num_flaky == 0
        assert results.num_failed == 2  # both of the 5-node tests should fail
        assert results.num_passed == 3  # 4-node, 3-node and 2-node should all pass
        assert results.num_ignored == 0

    def check_cluster_shrink_reschedule(self):
        """
        Test that the test that failed to schedule initially due to a node going offline is not lost and is still
        scheduled when more nodes become available.

        We start with a 6-node cluster.
        First we run a long-ish 3-node test, leaving 3 nodes available.
        Then when trying to run a second 3-node test, we shrink the cluster, emulating one of the nodes
        going down - this leaves only 2 nodes available, so we cannot run this test.

        However, after the first 3-node test finishes running, it will return its 3 nodes back to the cluster,
        so the second 3-node test becomes schedulable again - this is what we test for.

        Also two two-node tests should pass too - they should be scheduled before the second 3-node test,
        while two nodes are waiting for the first 3-node test to finish.

        It's generally not a good practice to rely on sleep, but I think it's acceptable in this case,
        since we do need to rely on parallelism.
        """

        mock_cluster = ShrinkingLocalhostCluster(num_nodes=6, shrink_on=2)
        session_context = tests.ducktape_mock.session_context(max_parallel=10)

        test_methods = [
            VariousNumNodesTest.test_three_nodes_asleep,
            VariousNumNodesTest.test_three_nodes_b,
            VariousNumNodesTest.test_two_nodes_a,
            VariousNumNodesTest.test_two_nodes_b
        ]

        ctx_list = self._do_expand(test_file=VARIOUS_NUM_NODES_TEST_FILE, test_class=VariousNumNodesTest,
                                   test_methods=test_methods, cluster=mock_cluster,
                                   session_context=session_context)

        runner = TestRunner(mock_cluster, session_context, Mock(), ctx_list, 1)
        results = runner.run_all_tests()

        assert len(results) == 4
        assert results.num_flaky == 0
        assert results.num_failed == 0
        assert results.num_passed == 4
        assert results.num_ignored == 0

        # normal order on a 6-node cluster would be:
        #  - test_three_nodes_asleep, test_three_nodes_b, test_two_nodes_a, test_two_nodes_b
        # however the cluster would shrink to 5 nodes after scheduling the first 3-node test,
        # leaving no space for the second 3-node test to be scheduled, bumping it down the line,
        # while two 2-node tests will be scheduled alongside the
        expected_scheduling_order = [
            "VariousNumNodesTest.test_three_nodes_asleep",
            "VariousNumNodesTest.test_two_nodes_a",
            "VariousNumNodesTest.test_two_nodes_b",
            "VariousNumNodesTest.test_three_nodes_b"
        ]
        # We check the actual order the tests were scheduled in, since completion order might be different,
        # with so many fast tests running in parallel.
        actual_scheduling_order = [x.test_id for x in runner.test_schedule_log]
        assert actual_scheduling_order == expected_scheduling_order

    def check_cluster_shrink_to_zero(self):
        """
        Validates that if the cluster is shrunk to zero nodes size, no tests can run,
        but we still exit gracefully.
        """

        mock_cluster = ShrinkingLocalhostCluster(num_nodes=1, shrink_on=1)
        session_context = tests.ducktape_mock.session_context(max_parallel=10)

        test_methods = [
            VariousNumNodesTest.test_one_node_a,
            VariousNumNodesTest.test_one_node_b,
        ]

        ctx_list = self._do_expand(test_file=VARIOUS_NUM_NODES_TEST_FILE, test_class=VariousNumNodesTest,
                                   test_methods=test_methods, cluster=mock_cluster,
                                   session_context=session_context)

        runner = TestRunner(mock_cluster, session_context, Mock(), ctx_list, 1)
        results = runner.run_all_tests()

        assert len(results) == 2
        assert results.num_flaky == 0
        assert results.num_failed == 2
        assert results.num_passed == 0
        assert results.num_ignored == 0


class ShrinkingLocalhostCluster(LocalhostCluster):

    def __init__(self, *args, shrink_on=1, **kwargs):
        super().__init__(*args, **kwargs)
        self.bad_nodes = NodeContainer()
        # which call to shrink on
        self.shrink_on = shrink_on
        self.num_alloc_calls = 0

    def do_alloc(self, cluster_spec):
        allocated = super().do_alloc(cluster_spec)
        self.num_alloc_calls += 1
        if self.shrink_on == self.num_alloc_calls:
            bad_node = allocated.pop()
            self._in_use_nodes.remove_node(bad_node)
            self.bad_nodes.add_node(bad_node)

            # simplified logic, we know all nodes are of the same OS/type
            # check if we don't have enough nodes any more
            # (which really should be true every time, since the largest test would be scheduled)
            if len(allocated) < len(cluster_spec):
                # return all good nodes back to be available
                for node in allocated:
                    self._in_use_nodes.remove_node(node)
                    self._available_nodes.add_node(node)

                raise InsufficientResourcesError("yeah")

        return allocated
