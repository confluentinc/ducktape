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

from .test import Test
from ducttape.services.core import ZookeeperService
from ducttape.services.core import KafkaService
from ducttape.services.core import KafkaRestService
from ducttape.services.core import SchemaRegistryService
from ducttape.services.register_schemas_service import RegisterSchemasService


class EverythingRunsTest(Test):
    """ Sanity check to ensure that various core services all run.
    """
    def __init__(self, cluster):
        self.cluster = cluster
        self.num_zk = 1
        self.num_brokers = 1
        self.num_rest = 1
        self.num_schema_registry = 1
        self.num_register_driver = 1

    def min_cluster_size(self):
        return self.num_zk + self.num_brokers + self.num_rest + self.num_schema_registry + self.num_register_driver

    def run(self):
        self.zk = ZookeeperService(self.cluster, self.num_zk)
        self.zk.start()

        self.kafka = KafkaService(self.cluster, self.num_brokers, self.zk)
        self.kafka.start()

        self.rest_proxy = KafkaRestService(self.cluster, self.num_rest, self.zk, self.kafka)
        self.rest_proxy.start()

        self.schema_registry = SchemaRegistryService(self.cluster, self.num_schema_registry, self.zk, self.kafka)
        self.schema_registry.start()

        self.register_driver = RegisterSchemasService(self.cluster, self.num_register_driver, self.schema_registry,
                                                      retry_wait_sec=.02, num_tries=5,
                                                      max_time_seconds=10, max_schemas=50)
        self.register_driver.start()
        self.register_driver.wait()
        self.register_driver.stop()

        self.schema_registry.stop()
        self.rest_proxy.stop()
        self.zk.stop()
        self.kafka.stop()

        self.logger.info("All proceeded smoothly.")

if __name__ == "__main__":
    EverythingRunsTest.run_standalone()