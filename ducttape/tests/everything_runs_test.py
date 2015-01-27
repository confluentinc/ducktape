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
    """ Simply check that the various core services all run.
    """
    def __init__(self, cluster):
        self.cluster = cluster

    def run(self):
        self.zk = ZookeeperService(self.cluster, 1)
        self.zk.start()

        self.kafka = KafkaService(self.cluster, 1, self.zk)
        self.kafka.start()
        #
        # self.rest_proxy = KafkaRestService(self.cluster, 1, self.zk, self.kafka)
        # self.rest_proxy.start()

        self.schema_registry = SchemaRegistryService(self.cluster, 1, self.zk, self.kafka)
        self.schema_registry.start()

        # self.register_schemas = RegisterSchemasService(self.cluster, 1, self.schema_registry, 10000, .02, 10)

        # master_node = self.schema_registry.get_master_node()
        # print master_node
        # master_id = self.schema_registry.idx(master_node)
        # print "Master node is " + self.schema_registry.url(master_id) + " and has id " + str(master_id)

        # self.schema_registry.stop()
        # self.zk.stop()
        # self.kafka.stop()

        self.logger.info("All proceeded smoothly.")

if __name__ == "__main__":
    EverythingRunsTest.run_standalone()