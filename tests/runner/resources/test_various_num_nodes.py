import time

from ducktape.mark.resource import cluster
from ducktape.tests.test import Test


class VariousNumNodesTest(Test):
    """
    Allocates various number of nodes.
    """

    @cluster(num_nodes=5)
    def test_five_nodes_a(self):
        assert True

    @cluster(num_nodes=5)
    def test_five_nodes_b(self):
        assert True

    @cluster(num_nodes=4)
    def test_four_nodes(self):
        assert True

    @cluster(num_nodes=3)
    def test_three_nodes_asleep(self):
        time.sleep(3)
        assert True

    @cluster(num_nodes=3)
    def test_three_nodes_a(self):
        assert True

    @cluster(num_nodes=3)
    def test_three_nodes_b(self):
        assert True

    @cluster(num_nodes=2)
    def test_two_nodes_a(self):
        assert True

    @cluster(num_nodes=2)
    def test_two_nodes_b(self):
        assert True

    @cluster(num_nodes=1)
    def test_one_node_a(self):
        assert True

    @cluster(num_nodes=1)
    def test_one_node_b(self):
        assert True

    def test_no_cluster_annotation(self):
        assert True

    @cluster()
    def test_empty_cluster_annotation(self):
        assert True

    # this one is valid regardless of
    # whether the greedy tests are allowed or not
    # because it's not greedy, quite the opposite
    @cluster(num_nodes=0)
    def test_zero_nodes(self):
        assert True
