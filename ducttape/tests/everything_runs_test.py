from .test import Test
from ducttape.services.core import ZookeeperService
from ducttape.services.core import KafkaService
from ducttape.services.core import KafkaRestService
from ducttape.services.core import SchemaRegistryService


class EverythingRunsTest(Test):
    """ Simply check that the various core services all run.
    """
    def __init__(self, cluster):
        self.cluster = cluster

    def run(self):
        self.zk = ZookeeperService(self.cluster, 1)
        self.zk.start()

        self.kafka = KafkaService(self.cluster, 3, self.zk)
        self.kafka.start()

        self.rest_proxy = KafkaRestService(self.cluster, 1, self.zk, self.kafka)
        self.rest_proxy.start()

        self.schema_registry = SchemaRegistryService(self.cluster, 1, self.zk, self.kafka)
        self.schema_registry.start()

        self.schema_registry.stop()
        self.rest_proxy.stop()
        self.zk.stop()
        self.kafka.stop()

        self.logger.info("All proceeded smoothly.")

if __name__ == "__main__":
    EverythingRunsTest.run_standalone()