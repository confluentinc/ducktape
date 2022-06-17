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
