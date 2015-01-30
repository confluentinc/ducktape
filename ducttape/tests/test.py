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

from ducttape.cluster import VagrantCluster
from ducttape.services.core import ZookeeperService, KafkaService, KafkaRestService, SchemaRegistryService, \
    HadoopV1Service, HadoopV2Service
from ducttape.logger import Logger
import logging


class Test(Logger):
    'Base class for tests that provides some minimal helper utilities'

    def __init__(self, cluster):
        self.cluster = cluster

    def log_start(self):
        self.logger.info("Running test %s", self._short_class_name())

    @classmethod
    def run_standalone(cls):
        logging.basicConfig(level=logging.INFO)
        cluster = VagrantCluster()
        test = cls(cluster)

        test.log_start()
        test.run()


class KafkaTest(Test):
    '''Helper class that managest setting up a Kafka cluster. Use this if the
    default settings for Kafka are sufficient for your test; any customization
    needs to be done manually. Your run() method should call tearDown and
    setUp. The Zookeeper and Kafka services are available as the fields
    KafkaTest.zk and KafkaTest.kafka.
    '''

    def __init__(self, cluster, num_zk, num_brokers, topics=None):
        super(KafkaTest, self).__init__(cluster)
        self.num_zk_nodes = num_zk
        self.num_brokers = num_brokers
        self.topics = topics

    def setUp(self):
        self.zk = ZookeeperService(self.cluster, self.num_zk_nodes)
        self.kafka = KafkaService(self.cluster, self.num_brokers, self.zk, topics=self.topics)
        self.zk.start()
        self.kafka.start()

    def tearDown(self):
        self.kafka.stop()
        self.zk.stop()


class RestProxyTest(KafkaTest):
    '''Helper class that manages setting up Kafka and the REST proxy. The REST proxy
    service is available as the field RestProxyTest.rest.
    '''
    def __init__(self, cluster, num_zk, num_brokers, num_rest, topics=None):
        super(RestProxyTest, self).__init__(cluster, num_zk, num_brokers, topics=topics)
        self.num_rest = num_rest

    def setUp(self):
        super(RestProxyTest, self).setUp()
        self.rest = KafkaRestService(self.cluster, self.num_rest, self.zk, self.kafka)
        self.rest.start()

    def tearDown(self):
        self.rest.stop()
        super(RestProxyTest, self).tearDown()


class SchemaRegistryTest(KafkaTest):
    '''Helper class that manages setting up Kafka and the Schema Registry proxy. The Schema Registry
    service is available as the field SchemaRegistryTest.schema_registry.
    '''
    def __init__(self, cluster, num_zk=1, num_brokers=1, num_schema_reg=1):
        super(SchemaRegistryTest, self).__init__(cluster, num_zk, num_brokers, topics=None)
        self.num_schema_reg = num_schema_reg

    def setUp(self):
        super(SchemaRegistryTest, self).setUp()
        self.schema_registry = SchemaRegistryService(self.cluster, self.num_schema_reg, self.zk, self.kafka)
        self.schema_registry.start()

    def tearDown(self):
        self.schema_registry.stop()
        super(SchemaRegistryTest, self).tearDown()


class HadoopTest(Test):
    '''Helper class that managest setting up a Hadoop V1 cluster. Your run() method should
    call tearDown and setUp.
    '''
    def __init__(self, cluster, num_nodes, hadoop_version=2):
        super(HadoopTest, self).__init__(cluster)
        self.num_nodes = num_nodes
        self.hadoop = None
        self.hadoop_version = hadoop_version

    def setUp(self):
        if self.hadoop_version == 1:
            self.hadoop = HadoopV1Service(self.cluster, self.num_nodes)
        else:
            self.hadoop = HadoopV2Service(self.cluster, self.num_nodes)
        self.hadoop.start()

    def tearDown(self):
        self.hadoop.stop()


class CamusTest(Test):
    def __init__(self, cluster, num_zk, num_brokers, num_hadoop_nodes, num_registry_nodes, hadoop_version=2, topics=None):
        super(CamusTest, self).__init__(cluster)
        self.num_zk = num_zk
        self.num_brokers = num_brokers
        self.num_nodes = num_hadoop_nodes
        self.num_registry_nodes = num_registry_nodes
        self.topics = topics
        self.hadoop_version = hadoop_version

    def setUp(self):
        self.zk = ZookeeperService(self.cluster, self.num_zk)
        self.kafka = KafkaService(self.cluster, self.num_brokers, self.zk, topics=self.topics)
        if self.hadoop_version == 1:
            self.hadoop = HadoopV1Service(self.cluster, self.num_nodes)
        else:
            self.hadoop = HadoopV2Service(self.cluster, self.num_nodes)
        self.schema_registry = SchemaRegistryService(self.cluster, self.num_registry_nodes, self.zk, self.kafka)
        self.zk.start()
        self.kafka.start()
        self.hadoop.start()
        self.schema_registry.start()

    def tearDown(self):
        self.zk.stop()
        self.kafka.stop()
        self.hadoop.stop()
        self.schema_registry.stop()

