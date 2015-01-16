from .test import Test
from ducttape.services.core import ZookeeperService
from ducttape.services.core import KafkaService
from ducttape.services.core import SchemaRegistryService


class MiniTest(Test):
    def __init__(self, cluster):
        self.cluster = cluster

    def run(self):
        self.zk = ZookeeperService(self.cluster, 1)
        self.zk.start()

        self.kafka = KafkaService(self.cluster, 1, self.zk)
        self.kafka.start()


        self.schema_registry = SchemaRegistryService(self.cluster, 1, self.zk, self.kafka)
        self.schema_registry.start()

        self.schema_registry.stop()
        self.zk.stop()
        self.kafka.stop()



if __name__ == "__main__":
    MiniTest.run_standalone()