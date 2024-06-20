"""
This module contains tests that are useful for developer debugging
and can contain sleep statements or test that intentionally fail or break things.
They're separate from test_remote_account.py for that reason.
"""
import time

from ducktape.mark import matrix, parametrize, ignore
from ducktape.mark.resource import cluster
from ducktape.tests.test import Test
from systests.cluster.test_remote_account import GenericService


class FailingTest(Test):
    """
    The purpose of this test is to validate reporters. Some of them are intended to fail.
    """
    def setup(self):
        self.service = GenericService(self.test_context, 1)

    @cluster(num_nodes=1)
    @matrix(string_param=['success-first', 'fail-second', 'fail-third'], int_param=[10, 20, -30])
    def matrix_test(self, string_param, int_param):
        assert not string_param.startswith('fail') and int_param > 0

    @cluster(num_nodes=1)
    @parametrize(string_param='success-first', int_param=10)
    @parametrize(string_param='fail-second', int_param=-10)
    def parametrized_test(self, string_param, int_param):
        assert not string_param.startswith('fail') and int_param > 0

    @cluster(num_nodes=1)
    def failing_test(self):
        assert False

    @cluster(num_nodes=1)
    def successful_test(self):
        assert True


class DebugThisTest(Test):

    @cluster(num_nodes=1)
    def one_node_test_sleep_90s(self):
        self.service = GenericService(self.test_context, 1)
        self.logger.warning('one_node_test - Sleeping for 90s')
        time.sleep(90)
        assert True

    @cluster(num_nodes=1)
    def one_node_test_sleep_30s(self):
        self.service = GenericService(self.test_context, 1)
        self.logger.warning('another_one_node_test - Sleeping for 30s')
        time.sleep(30)
        assert True

    @cluster(num_nodes=1)
    def another_one_node_test_sleep_30s(self):
        self.service = GenericService(self.test_context, 1)
        self.logger.warning('yet_another_one_node_test - Sleeping for 30s')
        time.sleep(30)
        assert True

    @cluster(num_nodes=2)
    def two_node_test(self):
        self.service = GenericService(self.test_context, 2)
        assert True

    @cluster(num_nodes=2)
    def another_two_node_test(self):
        self.service = GenericService(self.test_context, 2)
        assert True

    @ignore
    @cluster(num_nodes=2)
    def a_two_node_ignored_test(self):
        assert False

    @cluster(num_nodes=2)
    def yet_another_two_node_test(self):
        self.service = GenericService(self.test_context, 2)
        assert True

    @cluster(num_nodes=3)
    def three_node_test(self):
        self.service = GenericService(self.test_context, 3)
        assert True

    @cluster(num_nodes=3)
    def three_node_test_sleeping_30s(self):
        self.service = GenericService(self.test_context, 3)
        self.logger.warning('Sleeping for 30s')
        time.sleep(30)
        assert True

    @cluster(num_nodes=3)
    def another_three_node_test(self):
        self.service = GenericService(self.test_context, 3)
        assert True

    @cluster(num_nodes=2)
    def bad_alloc_test(self):
        # @cluster annotation specifies 2 nodes, but we ask for 3, this will fail
        self.service = GenericService(self.test_context, 3)
        time.sleep(10)
        assert True
