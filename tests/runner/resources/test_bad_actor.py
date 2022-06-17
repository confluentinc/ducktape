from ducktape.mark.resource import cluster
from ducktape.services.service import Service
from ducktape.tests.test import Test


class FakeService(Service):
    pass


class BadActorTest(Test):

    @cluster(num_nodes=2)
    def test_too_many_nodes(self):
        """
        This test should fail to allocate and
        should be dealt with gracefully by the framework, marking it as failed
        and moving on.
        """
        FakeService(self.test_context, num_nodes=3)
