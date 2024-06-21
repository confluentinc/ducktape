from ducktape.mark.resource import cluster
from ducktape.tests.test import Test


class NoClusterTest(Test):
    """This test helps validate the behavior for no-cluster tests (ie 0 nodes)"""

    @cluster(num_nodes=0)
    def test_zero_nodes(self):
        self.logger.warn('Testing')
        assert True
