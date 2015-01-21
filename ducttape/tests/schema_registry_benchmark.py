# Copyright 2014 Confluent Inc.
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

from .test import SchemaRegistryTest
from ducttape.services.performance import SchemaRegistryPerformanceService
import logging


logging.basicConfig(level=logging.DEBUG)

class SchemaRegistryBenchmark(SchemaRegistryTest):
    def __init__(self, cluster):
        super(SchemaRegistryBenchmark, self).__init__(cluster, num_zk=1, num_brokers=1, num_schema_reg=1)

    def run(self):
        self.setUp()

        num_schema_reg_nodes = 1
        subject = "testSubject"
        num_schemas = 10000
        schemas_per_sec = 1000

        schema_registry_perf = SchemaRegistryPerformanceService(
            self.cluster, num_schema_reg_nodes, self.schema_registry, subject, num_schemas, schemas_per_sec, settings={}
        )

        self.logger.info("Running SchemaRegistryBenchmark: registering %d schemas on %d schema registry node." %
                         (num_schemas, num_schema_reg_nodes))
        schema_registry_perf.run()
        self.tearDown()

        # self.logger.info("Schema Registry performance: %f per sec, %f ms",
        #                  schema_registry_perf.results[0]['records_per_sec'],
        #                  schema_registry_perf.results[0]['latency_99th_ms'])
        self.logger.info(schema_registry_perf.results)


if __name__ == "__main__":
    SchemaRegistryBenchmark.run_standalone()
